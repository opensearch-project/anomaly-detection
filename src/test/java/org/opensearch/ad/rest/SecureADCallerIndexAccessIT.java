/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import org.opensearch.ad.AnomalyDetectorRestTestCase;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.commons.rest.SecureRestClientBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Starting and previewing a detector check the caller's read access to the detector's source indices.
 */
public class SecureADCallerIndexAccessIT extends AnomalyDetectorRestTestCase {
    private static final String OWNER = "ad_index_access_owner";
    private static final String CALLER = "ad_index_access_caller";
    private static final String OWNER_ROLE = "ad_index_access_owner_role";
    private static final String CALLER_ROLE = "ad_index_access_caller_role";
    private static final String SOURCE_PREFIX = "ad-index-access-source-";
    private static final String READABLE_PREFIX = "ad-index-access-readable-";
    private static final String INDEX_ACCESS_DENIAL = "User doesn't have read permissions for one or more configured index";

    private RestClient ownerClient;
    private RestClient callerClient;
    private String sourceIndex;
    private Instant periodStart;
    private Instant periodEnd;

    @Override
    @Before
    public void setUp() throws Exception {
        if (!isHttps()) {
            throw new IllegalArgumentException("Secure Tests are running but HTTPS is not set");
        }
        // With resource sharing, the caller cannot see the owner's config and is denied before the index access check.
        assumeFalse("covers the deployment without resource sharing", isResourceSharingFeatureEnabled());
        super.setUp();
        sourceIndex = SOURCE_PREFIX + System.nanoTime();
        periodEnd = Instant.now().truncatedTo(ChronoUnit.MINUTES);
        periodStart = periodEnd.minus(400, ChronoUnit.MINUTES);
        createSourceIndex(sourceIndex);

        // Both hold the anomaly detection permissions. The owner may read every index; the caller may read only the
        // anomaly detection system and result indices and the readable indices, not the source index.
        createRole(OWNER_ROLE, "*");
        createRole(CALLER_ROLE, ".opendistro-anomaly-*", READABLE_PREFIX + "*");

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

    private void createSourceIndex(String index) throws IOException {
        TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/" + index,
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity("{\"mappings\":{\"properties\":{\"timestamp\":{\"type\":\"date\"},\"value\":{\"type\":\"double\"}}}}"),
                null
            );
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 420; i++) {
            bulk
                .append("{\"index\":{}}\n")
                .append(
                    String
                        .format(
                            Locale.ROOT,
                            "{\"timestamp\":%d,\"value\":%d}\n",
                            periodEnd.minus(i, ChronoUnit.MINUTES).toEpochMilli(),
                            (i % 9) + 1
                        )
                );
        }
        TestHelpers
            .makeRequest(
                client(),
                "POST",
                "/" + index + "/_bulk?refresh=true",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(bulk.toString()),
                null
            );
    }

    public void testCallerIsDeniedSourceIndex() throws Exception {
        assertForbidden(
            "direct search",
            () -> TestHelpers.makeRequest(callerClient, "POST", sourceIndex + "/_search", ImmutableMap.of(), (HttpEntity) null, null)
        );
    }

    public void testPreviewByIdRequiresCallerReadAccess() throws Exception {
        String detectorId = createDetector(ownerClient);
        String body = previewBody("");
        assertDenied("preview by id", () -> preview(callerClient, detectorId, body));
        assertEquals(RestStatus.OK.getStatus(), preview(ownerClient, detectorId, body).getStatusLine().getStatusCode());
    }

    public void testInlinePreviewRequiresCallerReadAccess() throws Exception {
        String body = previewBody(",\"detector\":" + detector(""));
        assertDenied("inline preview", () -> preview(callerClient, null, body));
    }

    public void testInlinePreviewIgnoresUserInRequestBody() throws Exception {
        String injected = ",\"user\":{\"name\":\"admin\",\"backend_roles\":[],\"roles\":[\"all_access\"],\"custom_attribute_names\":[]}";
        String body = previewBody(",\"detector\":" + detector(injected));
        assertDenied("inline preview with a user in the body", () -> preview(callerClient, null, body));
    }

    public void testStartRealTimeRequiresCallerReadAccess() throws Exception {
        String detectorId = createDetector(ownerClient);
        try {
            assertIndexAccessDenied(
                "start real time",
                () -> TestHelpers.makeRequest(callerClient, "POST", startPath(detectorId), ImmutableMap.of(), "", null)
            );
        } finally {
            stop(detectorId, "");
        }
    }

    public void testStartHistoricalRequiresCallerReadAccess() throws Exception {
        String detectorId = createDetector(ownerClient);
        String historical = String
            .format(Locale.ROOT, "{\"start_time\":%d,\"end_time\":%d}", periodStart.toEpochMilli(), periodEnd.toEpochMilli());
        try {
            assertIndexAccessDenied(
                "start historical",
                () -> TestHelpers
                    .makeRequest(callerClient, "POST", startPath(detectorId), ImmutableMap.of(), TestHelpers.toHttpEntity(historical), null)
            );
        } finally {
            stop(detectorId, "?historical=true");
        }
    }

    public void testStartOverPatternRequiresCallerReadAccessToEveryMatchedIndex() throws Exception {
        // The caller may read one of the indices the pattern matches but not the source index.
        createSourceIndex(READABLE_PREFIX + System.nanoTime());
        String detectorId = createDetector(ownerClient, "ad-index-access-*");
        try {
            assertIndexAccessDenied(
                "start real time over a pattern",
                () -> TestHelpers.makeRequest(callerClient, "POST", startPath(detectorId), ImmutableMap.of(), "", null)
            );
        } finally {
            stop(detectorId, "");
        }
    }

    public void testOwnerCanStartRealTime() throws Exception {
        String detectorId = createDetector(ownerClient);
        try {
            Response response = TestHelpers.makeRequest(ownerClient, "POST", startPath(detectorId), ImmutableMap.of(), "", null);
            assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        } finally {
            stop(detectorId, "");
        }
    }

    private String startPath(String detectorId) {
        return String.format(Locale.ROOT, "%s/%s/_start", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, detectorId);
    }

    private void stop(String detectorId, String params) throws IOException {
        String stop = String.format(Locale.ROOT, "%s/%s/_stop%s", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, detectorId, params);
        try {
            TestHelpers.makeRequest(client(), "POST", stop, ImmutableMap.of(), "", null);
        } catch (ResponseException e) {
            // nothing was started
        }
    }

    private String createDetector(RestClient client) throws IOException {
        return createDetector(client, sourceIndex);
    }

    private String createDetector(RestClient client, String indices) throws IOException {
        Response response = TestHelpers
            .makeRequest(
                client,
                "POST",
                TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector(indices, "")),
                null
            );
        String id = (String) entityAsMap(response).get("_id");
        assertNotNull(id);
        return id;
    }

    private String detector(String extraFields) {
        return detector(sourceIndex, extraFields);
    }

    private String detector(String indices, String extraFields) {
        return String
            .format(
                Locale.ROOT,
                "{\"name\":\"index-access-%d\",\"time_field\":\"timestamp\",\"indices\":[\"%s\"],"
                    + "\"feature_attributes\":[{\"feature_name\":\"max_value\",\"feature_enabled\":true,"
                    + "\"aggregation_query\":{\"max_value\":{\"max\":{\"field\":\"value\"}}}}],"
                    + "\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},"
                    + "\"window_delay\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}}%s}",
                System.nanoTime(),
                indices,
                extraFields
            );
    }

    private String previewBody(String detectorField) {
        return String
            .format(
                Locale.ROOT,
                "{\"period_start\":%d,\"period_end\":%d%s}",
                periodStart.toEpochMilli(),
                periodEnd.toEpochMilli(),
                detectorField
            );
    }

    private Response preview(RestClient client, String detectorId, String body) throws IOException {
        String path = detectorId == null
            ? TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI + "/_preview"
            : String.format(Locale.ROOT, "%s/%s/_preview", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, detectorId);
        return TestHelpers.makeRequest(client, "POST", path, ImmutableMap.of(), TestHelpers.toHttpEntity(body), null);
    }

    /**
     * Asserts the call is answered 403 by the caller index access check, not by another check such as resource sharing.
     */
    private void assertIndexAccessDenied(String what, ThrowingRunnable request) throws IOException {
        assertForbidden(what, request, INDEX_ACCESS_DENIAL);
    }

    private void assertForbidden(String what, ThrowingRunnable request) throws IOException {
        assertForbidden(what, request, null);
    }

    private void assertForbidden(String what, ThrowingRunnable request, String expectedMessage) throws IOException {
        try {
            request.run();
        } catch (ResponseException e) {
            assertEquals(what + ": " + e.getMessage(), RestStatus.FORBIDDEN.getStatus(), e.getResponse().getStatusLine().getStatusCode());
            if (expectedMessage != null) {
                assertTrue(what + ": " + e.getMessage(), e.getMessage().contains(expectedMessage));
            }
            return;
        } catch (Throwable t) {
            throw new AssertionError(what, t);
        }
        fail(what + " succeeded for a caller denied " + sourceIndex);
    }

    /**
     * Preview searches run as the caller, so the security plugin denies them; the denial reaches the client wrapped in a
     * run failure rather than as a 403.
     */
    private void assertDenied(String what, ThrowingRunnable request) throws IOException {
        try {
            request.run();
        } catch (ResponseException e) {
            assertTrue(what + ": " + e.getMessage(), e.getMessage().contains("no permissions for [indices:data/read/search]"));
            return;
        } catch (Throwable t) {
            throw new AssertionError(what, t);
        }
        fail(what + " succeeded for a caller denied " + sourceIndex);
    }

    private void createRole(String role, String... indexPatterns) throws IOException {
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
                                "{\"cluster_permissions\":[\"cluster:admin/opendistro/ad/*\"],"
                                    + "\"index_permissions\":[{\"index_patterns\":[\"%s\"],\"allowed_actions\":["
                                    + "\"indices:data/read*\",\"indices:admin/mappings/fields/get*\",\"indices:admin/mappings/get\","
                                    + "\"indices:admin/aliases/get\",\"indices:admin/resolve/index\",\"indices_monitor\"]}]}",
                                String.join("\",\"", indexPatterns)
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
