/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;

import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.commons.rest.SecureRestClientBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.forecast.AbstractForecastSyntheticDataTest;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.TaskState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Running and starting a forecaster check the caller's read access to the forecaster's source indices.
 */
public class SecureForecastRunOnceIndexAccessIT extends AbstractForecastSyntheticDataTest {
    private static final String OWNER = "run_once_owner";
    private static final String CALLER = "run_once_caller";
    private static final String OWNER_ROLE = "run_once_owner_role";
    private static final String CALLER_ROLE = "run_once_caller_role";
    private static final String ENTITY_NAME = "cityName";
    private static final String INDEX_ACCESS_DENIAL = "User doesn't have read permissions for one or more configured index";

    private RestClient ownerClient;
    private RestClient callerClient;
    private String forecaster;

    @Override
    @Before
    public void setUp() throws Exception {
        if (!isHttps()) {
            throw new IllegalArgumentException("Secure Tests are running but HTTPS is not set");
        }
        // With resource sharing, the caller cannot see the owner's config and is denied before the index access check.
        assumeFalse("covers the deployment without resource sharing", isResourceSharingFeatureEnabled());
        super.setUp();
        Instant trainTime = loadRuleData(200);
        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        forecaster = String
            .format(
                Locale.ROOT,
                "{\"name\":\"run-once-index-access-%d\",\"time_field\":\"timestamp\",\"indices\":[\"%s\"],"
                    + "\"feature_attributes\":[{\"feature_id\":\"max1\",\"feature_name\":\"max1\",\"feature_enabled\":true,"
                    + "\"importance\":1,\"aggregation_query\":{\"max1\":{\"max\":{\"field\":\"visitCount\"}}}}],"
                    + "\"window_delay\":{\"period\":{\"interval\":%d,\"unit\":\"MINUTES\"}},\"schema_version\":2,\"horizon\":24,"
                    + "\"forecast_interval\":{\"period\":{\"interval\":10,\"unit\":\"MINUTES\"}},\"category_field\":[\"%s\"]}",
                System.nanoTime(),
                RULE_DATASET_NAME,
                windowDelayMinutes,
                ENTITY_NAME
            );
        updateClusterSettings(ForecastEnabledSetting.FORECAST_ENABLED, true);

        // The owner may read every index. The caller holds the same forecast permissions but may read only the
        // forecast system and result indices, not the forecaster's source index.
        createRole(OWNER_ROLE, "*");
        createRole(CALLER_ROLE, "opensearch-forecast-*");

        String ownerPassword = generatePassword(OWNER);
        createUser(OWNER, ownerPassword, new ArrayList<>());
        createRoleMapping(OWNER_ROLE, new ArrayList<>(Arrays.asList(OWNER)));
        ownerClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), OWNER, ownerPassword)
            .setSocketTimeout(60000)
            .build();

        String callerPassword = generatePassword(CALLER);
        createUser(CALLER, callerPassword, new ArrayList<>());
        createRoleMapping(CALLER_ROLE, new ArrayList<>(Arrays.asList(CALLER)));
        callerClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), CALLER, callerPassword)
            .setSocketTimeout(60000)
            .build();
    }

    @After
    public void tearDownUsers() throws IOException {
        // setUp may fail before the clients are built, for example when HTTPS is not set
        if (ownerClient != null) {
            ownerClient.close();
        }
        if (callerClient != null) {
            callerClient.close();
        }
        if (!isHttps() || isResourceSharingFeatureEnabled()) {
            return;
        }
        deleteUser(OWNER);
        deleteUser(CALLER);
        deleteRole(OWNER_ROLE);
        deleteRole(CALLER_ROLE);
    }

    public void testRunOnceRequiresCallerReadAccessToSourceIndex() throws Exception {
        // The caller is denied the source index directly.
        ResponseException directRead = expectThrows(
            ResponseException.class,
            () -> TestHelpers.makeRequest(callerClient, "POST", RULE_DATASET_NAME + "/_search", ImmutableMap.of(), (HttpEntity) null, null)
        );
        assertEquals(RestStatus.FORBIDDEN.getStatus(), directRead.getResponse().getStatusLine().getStatusCode());

        Response response = TestHelpers
            .makeRequest(ownerClient, "POST", CREATE_FORECASTER, ImmutableMap.of(), TestHelpers.toHttpEntity(forecaster), null);
        String forecasterId = (String) entityAsMap(response).get("_id");
        assertNotNull(forecasterId);

        // Running the owner's forecaster requires read access to its source index.
        ResponseException runOnce = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    callerClient,
                    "POST",
                    String.format(Locale.ROOT, RUN_ONCE_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                )
        );
        assertEquals(RestStatus.FORBIDDEN.getStatus(), runOnce.getResponse().getStatusLine().getStatusCode());
        // denied by the caller index access check, not by another check such as resource sharing
        assertTrue(runOnce.getMessage(), runOnce.getMessage().contains(INDEX_ACCESS_DENIAL));

        // The owner, who can read the source index, still can.
        response = TestHelpers
            .makeRequest(
                ownerClient,
                "POST",
                String.format(Locale.ROOT, RUN_ONCE_FORECASTER, forecasterId),
                ImmutableMap.of(),
                (HttpEntity) null,
                null
            );
        assertNotNull(entityAsMap(response).get("task_id"));
        waitUntilTaskReachState(forecasterId, ImmutableSet.of(TaskState.TEST_COMPLETE.name()), client());
    }

    public void testStartRequiresCallerReadAccessToSourceIndex() throws Exception {
        Response response = TestHelpers
            .makeRequest(ownerClient, "POST", CREATE_FORECASTER, ImmutableMap.of(), TestHelpers.toHttpEntity(forecaster), null);
        String forecasterId = (String) entityAsMap(response).get("_id");
        try {
            ResponseException start = expectThrows(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        callerClient,
                        "POST",
                        String.format(Locale.ROOT, START_FORECASTER, forecasterId),
                        ImmutableMap.of(),
                        (HttpEntity) null,
                        null
                    )
            );
            assertEquals(RestStatus.FORBIDDEN.getStatus(), start.getResponse().getStatusLine().getStatusCode());
            // denied by the caller index access check, not by another check such as resource sharing
            assertTrue(start.getMessage(), start.getMessage().contains(INDEX_ACCESS_DENIAL));
        } finally {
            try {
                TestHelpers
                    .makeRequest(
                        client(),
                        "POST",
                        String.format(Locale.ROOT, STOP_FORECASTER, forecasterId),
                        ImmutableMap.of(),
                        (HttpEntity) null,
                        null
                    );
            } catch (ResponseException e) {
                // nothing was started
            }
        }
    }

    private void createRole(String role, String indexPattern) throws IOException {
        TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/_opendistro/_security/api/roles/" + role,
                null,
                TestHelpers
                    .toHttpEntity(
                        String
                            .format(
                                Locale.ROOT,
                                "{\"cluster_permissions\":[\"cluster:admin/plugin/forecast/*\"],"
                                    + "\"index_permissions\":[{\"index_patterns\":[\"%s\"],\"allowed_actions\":["
                                    + "\"indices:data/read*\",\"indices:admin/mappings/fields/get*\",\"indices:admin/mappings/get\","
                                    + "\"indices:admin/aliases/get\",\"indices:admin/resolve/index\",\"indices_monitor\"]}]}",
                                indexPattern
                            )
                    ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    private void deleteRole(String role) throws IOException {
        for (String path : new String[] { "/_opendistro/_security/api/rolesmapping/" + role, "/_opendistro/_security/api/roles/" + role }) {
            Response response = TestHelpers
                .makeRequest(client(), "DELETE", path, null, "", ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana")));
            EntityUtils.consume(response.getEntity());
        }
    }
}
