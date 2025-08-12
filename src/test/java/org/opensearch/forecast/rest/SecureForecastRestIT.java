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
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.commons.rest.SecureRestClientBuilder;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.forecast.AbstractForecastSyntheticDataTest;
import org.opensearch.forecast.model.ForecastTaskProfile;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.search.SearchHit;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.TaskState;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class SecureForecastRestIT extends AbstractForecastSyntheticDataTest {
    private static final Logger LOG = (Logger) LogManager.getLogger(SecureForecastRestIT.class);

    private static final String forecastReadRole = "forecast_read_access";
    private static final String forecastFullRole = "forecast_full_access";
    private static final String sourceIndexReadRole = "data_source_read";
    private static final String phoenixReadRole = "phoenix_read_role";
    private static final String noResultRole = "forecast_no_result_access";
    private static final String devOpsResultRole = "forecast_devOps_result_access";
    private static final String sdeResultRole = "forecast_sde_result_access";

    private static final String fullUser = "forecaster_full_user";
    private static final String readUser = "forecaster_read_user";
    private static final String noUser = "no_forecast_user";
    private static final String devOpsUser = "devOpsUser";
    private static final String sdeUser = "sdeUser";
    private static final String phoenixReadUser = "phoenixReadUser";
    private static final String noResultUser = "noResultUser";
    private static final String devOpsLimitedUser = "devOpsLimitedUser";
    private static final String sdeLimitedUser = "sdeLimitedUser";

    private static final String NAME = "Second-Test-Forecaster-4";
    private static final String ENTITY_NAME = "cityName";
    private static final String ENTITY_VALUE = "S*";
    private static final String PHOENIX_VALUE = "Phoenix";

    private static final String searchForecasterRequest = "{\n"
        + "    \"query\": {\n"
        + "        \"bool\": {\n"
        + "            \"filter\": {\n"
        + "                \"wildcard\": {\n"
        + "                    \"indices\": {\n"
        + "                        \"value\": \"r*\"\n"
        + "                    }\n"
        + "                }\n"
        + "            }\n"
        + "        }\n"
        + "    }\n"
        + "}";

    private RestClient fullClient;
    private RestClient readClient;
    private RestClient noClient;
    private RestClient devOpsClient;
    private RestClient sdeClient;
    private RestClient phoenixReadClient;
    private RestClient noResultClient;
    private RestClient devOpsLimitedClient;
    private RestClient sdeLimitedClient;

    private String formattedForecaster;
    private Instant trainTime;
    private String forecasterDef;
    private long windowDelayMinutes;

    @Override
    @Before
    public void setUp() throws Exception {
        if (!isHttps()) {
            throw new IllegalArgumentException("Secure Tests are running but HTTPS is not set");
        }
        super.setUp();
        try {
            getRole(forecastReadRole);
        } catch (ResponseException e) {
            // Role not exists
            Response createResonse = createForecastReadRole();
            assertEquals(RestStatus.CREATED.getStatus(), createResonse.getStatusLine().getStatusCode());
            LOG.info("Created read role");
        }

        try {
            getRole(forecastFullRole);
        } catch (ResponseException e) {
            // Role not exists
            Response createResonse = createForecastFullRole();
            assertEquals(RestStatus.CREATED.getStatus(), createResonse.getStatusLine().getStatusCode());
            LOG.info("Created full role");
        }

        try {
            getRole(sourceIndexReadRole);
        } catch (ResponseException e) {
            // Role not exists
            Response createResonse = createDataSourceReadRole();
            assertEquals(RestStatus.CREATED.getStatus(), createResonse.getStatusLine().getStatusCode());
            LOG.info("Created source index role");
        }

        try {
            getRole(phoenixReadRole);
        } catch (ResponseException e) {
            // Role not exists
            Response createResonse = createPhoenixReadRole();
            assertEquals(RestStatus.CREATED.getStatus(), createResonse.getStatusLine().getStatusCode());
            LOG.info("Created phoenix read role");
        }

        try {
            getRole(noResultRole);
        } catch (ResponseException e) {
            // Role not exists
            Response createResonse = createForecastNoResultAccessRole();
            assertEquals(RestStatus.CREATED.getStatus(), createResonse.getStatusLine().getStatusCode());
            LOG.info("Created no result access role");
        }

        try {
            getRole(devOpsResultRole);
        } catch (ResponseException e) {
            // Role not exists
            Response createResonse = createDevOpsResultAccessRole();
            assertEquals(RestStatus.CREATED.getStatus(), createResonse.getStatusLine().getStatusCode());
            LOG.info("Created devOps result access role");
        }

        try {
            getRole(sdeResultRole);
        } catch (ResponseException e) {
            // Role not exists
            Response createResonse = createSDEResultAccessRole();
            assertEquals(RestStatus.CREATED.getStatus(), createResonse.getStatusLine().getStatusCode());
            LOG.info("Created SDE result access role");
        }

        String fullPassword = generatePassword(fullUser);
        logger.info(fullPassword);
        createUser(fullUser, fullPassword, new ArrayList<>());
        fullClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), fullUser, fullPassword)
            .setSocketTimeout(60000)
            .build();

        String readPassword = generatePassword(readUser);
        createUser(readUser, readPassword, new ArrayList<>());
        readClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), readUser, readPassword)
            .setSocketTimeout(60000)
            .build();

        String noPassword = generatePassword(noUser);
        createUser(noUser, noPassword, new ArrayList<>());
        noClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), noUser, noPassword)
            .setSocketTimeout(60000)
            .build();

        String devOpsPassword = generatePassword(devOpsUser);
        createUser(devOpsUser, devOpsPassword, new ArrayList<>(Arrays.asList("devOps")));
        devOpsClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), devOpsUser, devOpsPassword)
            .setSocketTimeout(60000)
            .build();

        String sdePassword = generatePassword(sdeUser);
        createUser(sdeUser, sdePassword, new ArrayList<>(Arrays.asList("SDE")));
        sdeClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), sdeUser, sdePassword)
            .setSocketTimeout(60000)
            .build();

        String phoenixReadPassword = generatePassword(phoenixReadUser);
        createUser(phoenixReadUser, phoenixReadPassword, new ArrayList<>());
        phoenixReadClient = new SecureRestClientBuilder(
            getClusterHosts().toArray(new HttpHost[0]),
            isHttps(),
            phoenixReadUser,
            phoenixReadPassword
        ).setSocketTimeout(60000).build();

        String noResultPassword = generatePassword(noResultUser);
        createUser(noResultUser, noResultPassword, new ArrayList<>());
        noResultClient = new SecureRestClientBuilder(getClusterHosts().toArray(new HttpHost[0]), isHttps(), noResultUser, noResultPassword)
            .setSocketTimeout(60000)
            .build();

        String devOpsLimitedPassword = generatePassword(devOpsLimitedUser);
        createUser(devOpsLimitedUser, devOpsLimitedPassword, new ArrayList<>());
        devOpsLimitedClient = new SecureRestClientBuilder(
            getClusterHosts().toArray(new HttpHost[0]),
            isHttps(),
            devOpsLimitedUser,
            devOpsLimitedPassword
        ).setSocketTimeout(60000).build();

        String sdeLimitedPassword = generatePassword(sdeLimitedUser);
        createUser(sdeLimitedUser, sdeLimitedPassword, new ArrayList<>());
        sdeLimitedClient = new SecureRestClientBuilder(
            getClusterHosts().toArray(new HttpHost[0]),
            isHttps(),
            sdeLimitedUser,
            sdeLimitedPassword
        ).setSocketTimeout(60000).build();

        createRoleMapping(forecastFullRole, new ArrayList<>(Arrays.asList(fullUser, devOpsUser, sdeUser, phoenixReadUser)));
        createRoleMapping(forecastReadRole, new ArrayList<>(Arrays.asList(readUser)));
        // no need to give source index read role to fullUser who has already equipped with the permission
        createRoleMapping(
            sourceIndexReadRole,
            new ArrayList<>(Arrays.asList(readUser, noUser, noResultUser, devOpsLimitedUser, sdeLimitedUser))
        );
        createRoleMapping(phoenixReadRole, new ArrayList<>(Arrays.asList(phoenixReadUser)));
        createRoleMapping(noResultRole, new ArrayList<>(Arrays.asList(noResultUser)));
        createRoleMapping(devOpsResultRole, new ArrayList<>(Arrays.asList(devOpsLimitedUser)));
        createRoleMapping(sdeResultRole, new ArrayList<>(Arrays.asList(sdeLimitedUser)));

        trainTime = loadRuleData(200);
        forecasterDef = "{\n"
            + "    \"name\": \"%s\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"max1\",\n"
            + "            \"feature_name\": \"max1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                        \"max1\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"visitCount\"\n"
            + "                            }\n"
            + "                        }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": %d,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24,\n"
            + "    \"forecast_interval\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 10,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"category_field\": [\"%s\"]"
            + "}";

        // +1 to make sure it is big enough
        windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        formattedForecaster = String.format(Locale.ROOT, forecasterDef, NAME, RULE_DATASET_NAME, windowDelayMinutes, ENTITY_NAME);

        updateClusterSettings(ForecastEnabledSetting.FORECAST_ENABLED, true);
    }

    private Response createForecastReadRole() throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/_opendistro/_security/api/roles/" + forecastReadRole,
                null,
                TestHelpers
                    .toHttpEntity(
                        "{\n"
                            + "  \"cluster_permissions\": [\n"
                            + "    \"cluster:admin/plugin/forecast/forecasters/info\",\n"
                            + "    \"cluster:admin/plugin/forecast/forecasters/search\",\n"
                            + "    \"cluster:admin/plugin/forecast/forecasters/get\",\n"
                            + "    \"cluster:admin/plugin/forecast/result/topForecasts\",\n"
                            + "    \"cluster:admin/plugin/forecast/forecaster/stats\",\n"
                            + "    \"cluster:admin/plugin/forecast/tasks/search\",\n"
                            + "    \"cluster:admin/plugin/forecast/forecaster/validate\",\n"
                            + "    \"cluster:admin/plugin/forecast/forecaster/suggest\",\n"
                            + "    \"cluster:admin/plugin/forecast/forecaster/info\"\n"
                            + "  ],\n"
                            + "  \"index_permissions\": [\n"
                            + "    {\n"
                            + "      \"index_patterns\": [\n"
                            + "        \"opensearch-forecast-result*\"\n"
                            + "      ],\n"
                            + "      \"dls\": \"\",\n"
                            + "      \"fls\": [],\n"
                            + "      \"masked_fields\": [],\n"
                            + "      \"allowed_actions\": [\n"
                            + "        \"indices:data/read*\",\n"
                            + "        \"indices:admin/mappings/fields/get*\",\n"
                            + "        \"indices:admin/resolve/index\"\n"
                            + "      ]\n"
                            + "    }\n"
                            + "  ]\n"
                            + "}\n"
                    ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    private Response createForecastFullRole() throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/_opendistro/_security/api/roles/" + forecastFullRole,
                null,
                TestHelpers
                    .toHttpEntity(
                        "{\n"
                            + "  \"cluster_permissions\": [\n"
                            + "    \"cluster:admin/plugin/forecast/*\",\n"
                            + "    \"cluster:admin/settings/update\"\n"
                            + "  ],\n"
                            + "  \"index_permissions\": [{\n"
                            + "    \"index_patterns\": [\n"
                            + "      \"*\"\n"
                            + "    ],\n"
                            + "    \"dls\": \"\",\n"
                            + "    \"fls\": [],\n"
                            + "    \"masked_fields\": [],\n"
                            + "    \"allowed_actions\": [\n"
                            + "      \"indices:data/read*\", \n"
                            + "      \"indices:admin/aliases/get\",\n"
                            + "      \"indices:admin/mappings/fields/get*\",\n"
                            + "      \"indices:admin/resolve/index\",\n"
                            + "      \"indices:data/write*\",\n"
                            + "      \"indices:data/read/field_caps*\",\n"
                            + "      \"indices:data/read/search\",\n"
                            + "      \"indices:admin/mapping/put\",\n"
                            + "      \"indices:admin/mapping/get\",\n"
                            + "      \"indices_monitor\"\n"
                            + "    ]\n"
                            + "  }]\n"
                            + "}\n"
                    ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    private Response createDataSourceReadRole() throws IOException {
        String requestBody = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "  \"index_permissions\": [{\n"
                    + "    \"index_patterns\": [\n"
                    + "      \"%s\"\n"
                    + "    ],\n"
                    + "    \"dls\": \"\",\n"
                    + "    \"fls\": [],\n"
                    + "    \"masked_fields\": [],\n"
                    + "    \"allowed_actions\": [\n"
                    + "      \"read\"\n"
                    + "    ]\n"
                    + "  }]\n"
                    + "}",
                RULE_DATASET_NAME
            );
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/_opendistro/_security/api/roles/" + sourceIndexReadRole,
                null,
                TestHelpers.toHttpEntity(requestBody),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    private Response createPhoenixReadRole() throws IOException {
        String requestBody = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "  \"index_permissions\": [{\n"
                    + "    \"index_patterns\": [\n"
                    + "      \"%s\"\n"
                    + "    ],\n"
                    + "    \"dls\": \"{\\\"term\\\": { \\\"%s\\\": \\\"%s\\\"}}\",\n"
                    + "    \"fls\": [],\n"
                    + "    \"masked_fields\": [],\n"
                    + "    \"allowed_actions\": [\n"
                    + "      \"read\"\n"
                    + "    ]\n"
                    + "  }]\n"
                    + "}",
                RULE_DATASET_NAME,
                ENTITY_NAME,
                PHOENIX_VALUE
            );
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/_opendistro/_security/api/roles/" + phoenixReadRole,
                null,
                TestHelpers.toHttpEntity(requestBody),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    private Response createForecastNoResultAccessRole() throws IOException {

        // Create the request body as a formatted string
        String requestBody = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "  \"cluster_permissions\": [\n"
                    + "    \"cluster:admin/plugin/forecast/forecasters/info\",\n"
                    + "    \"cluster:admin/plugin/forecast/forecasters/search\",\n"
                    + "    \"cluster:admin/plugin/forecast/forecasters/get\",\n"
                    + "    \"cluster:admin/plugin/forecast/result/topForecasts\",\n"
                    + "    \"cluster:admin/plugin/forecast/forecaster/stats\",\n"
                    + "    \"cluster:admin/plugin/forecast/tasks/search\",\n"
                    + "    \"cluster:admin/plugin/forecast/forecaster/validate\",\n"
                    + "    \"cluster:admin/plugin/forecast/forecaster/suggest\",\n"
                    + "    \"cluster:admin/plugin/forecast/forecaster/info\"\n"
                    + "  ]\n"
                    + "}"
            );

        // Make the PUT request to create the role
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/_plugins/_security/api/roles/" + noResultRole,
                null,
                TestHelpers.toHttpEntity(requestBody),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    private Response createRoleWithResultAccess(String roleName, String userRole) throws IOException {
        // Define the DLS query with the specified user role
        String dlsQuery = String
            .format(
                Locale.ROOT,
                "{\"bool\": {\"filter\": [{\"nested\": {\"path\": \"user\", \"query\": {\"bool\": {\"filter\": [{\"term\": {\"user.backend_roles.keyword\": \"%s\"}}]}}, \"score_mode\": \"none\"}}], \"adjust_pure_negative\": true, \"boost\": 1.0}}",
                userRole
            );

        // Escape double quotes
        String escapedDlsQuery = dlsQuery.replace("\"", "\\\"");

        // Create the request body with the specified role name and DLS query
        String requestBody = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "  \"cluster_permissions\": [\n"
                    + "    \"cluster:admin/plugin/forecast/*\",\n"
                    + "    \"cluster:admin/settings/update\"\n"
                    + "  ],\n"
                    + "  \"index_permissions\": [\n"
                    + "    {\n"
                    + "      \"index_patterns\": [\n"
                    + "        \"opensearch-forecast-result*\"\n"
                    + "      ],\n"
                    + "      \"dls\": \"%s\",\n"
                    + "      \"fls\": [],\n"
                    + "      \"masked_fields\": [],\n"
                    + "      \"allowed_actions\": [\n"
                    + "        \"read\",\n"
                    + "        \"write\"\n"
                    + "      ]\n"
                    + "    }\n"
                    + "  ],\n"
                    + "  \"tenant_permissions\": []\n"
                    + "}",
                escapedDlsQuery
            );

        // Make the PUT request to create the role
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "/_plugins/_security/api/roles/" + roleName,
                null,
                TestHelpers.toHttpEntity(requestBody),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    private Response createSDEResultAccessRole() throws IOException {
        return createRoleWithResultAccess(sdeResultRole, "SDE");
    }

    private Response createDevOpsResultAccessRole() throws IOException {
        return createRoleWithResultAccess(devOpsResultRole, "devOps");
    }

    @After
    public void deleteUserSetup() throws IOException {
        fullClient.close();
        readClient.close();
        noClient.close();
        devOpsClient.close();
        sdeClient.close();
        phoenixReadClient.close();
        noResultClient.close();
        devOpsLimitedClient.close();
        sdeLimitedClient.close();
        deleteUser(fullUser);
        deleteUser(readUser);
        deleteUser(noUser);
        deleteUser(devOpsUser);
        deleteUser(sdeUser);
        deleteUser(phoenixReadUser);
        deleteUser(noResultUser);
        deleteUser(devOpsLimitedUser);
        deleteUser(sdeLimitedUser);
    }

    public Response searchResult(RestClient client) throws IOException {
        String request = "{\n"
            + "    \"size\": 10000,\n"
            + "    \"sort\": [\n"
            + "        {\n"
            + "            \"execution_start_time\": {\n"
            + "                \"order\": \"desc\"\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"track_total_hits\": true\n"
            + "}";

        Response response = TestHelpers
            .makeRequest(client(), "GET", SEARCH_RESULTS, ImmutableMap.of(), TestHelpers.toHttpEntity(request), null);
        return response;
    }

    protected List<SearchHit> waitUntilResultAvailable(RestClient client) throws InterruptedException, IOException {
        long total = 0;
        List<SearchHit> hits = null;
        for (int i = 0; total == 0 && i < MAX_RETRY_TIMES; i++) {
            try {
                Thread.sleep(1000);
                Response response = searchResult(client);
                hits = toHits(response);
                total = hits.size();
            } catch (Exception e) {
                LOG.info(e);
            }
        }

        assertTrue(total > 0);
        return hits;
    }

    private Response enableFilterBy() throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "_cluster/settings",
                null,
                TestHelpers
                    .toHttpEntity(
                        "{\n"
                            + "  \"persistent\": {\n"
                            + "       \"plugins.forecast.filter_by_backend_roles\" : \"true\"\n"
                            + "   }\n"
                            + "}"
                    ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response disableFilterBy() throws IOException {
        return TestHelpers
            .makeRequest(
                client(),
                "PUT",
                "_cluster/settings",
                null,
                TestHelpers
                    .toHttpEntity(
                        "{\n"
                            + "  \"persistent\": {\n"
                            + "       \"plugins.forecast.filter_by_backend_roles\" : \"false\"\n"
                            + "   }\n"
                            + "}"
                    ),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
    }

    public Response searchResultMatchAll(RestClient client, String index) throws IOException {
        String requestBody = "{\n" + "    \"query\": {\n" + "        \"match_all\": {}\n" + "    },\n" + "    \"size\": 10\n" + "}";

        Response response = TestHelpers
            .makeRequest(
                client,
                "GET",
                String.format(Locale.ROOT, SEARCH_INDEX_MATCH_ALL, index),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(requestBody),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
        return response;
    }

    public void testAuthApi() throws IOException, InterruptedException {
        // case 0: read access user cannot create forecaster
        Exception exception = expectThrows(
            IOException.class,
            () -> TestHelpers
                .makeRequest(
                    readClient,
                    "POST",
                    String.format(Locale.ROOT, CREATE_FORECASTER),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(formattedForecaster),
                    null
                )
        );
        Assert
            .assertTrue(
                "actual: " + exception.getMessage(),
                exception.getMessage().contains("no permissions for [cluster:admin/plugin/forecast/forecaster/write]")
            );

        // case 1: full access user can create forecaster
        Response response = TestHelpers
            .makeRequest(
                fullClient,
                "POST",
                String.format(Locale.ROOT, CREATE_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        Map<String, Object> responseMap = entityAsMap(response);
        String forecasterId = (String) responseMap.get("_id");
        assertNotNull(forecasterId);

        // case 2: given a forecaster Id, read access user cannot start the forecaster
        exception = expectThrows(
            IOException.class,
            () -> TestHelpers
                .makeRequest(
                    readClient,
                    "POST",
                    String.format(Locale.ROOT, START_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                )
        );
        String message = "no permissions for [cluster:admin/plugin/forecast/forecaster/jobmanagement]";
        Assert.assertTrue("actual: " + exception.getMessage(), exception.getMessage().contains(message));

        // case 3: given a forecaster Id, full access user can start the forecaster
        response = TestHelpers
            .makeRequest(
                fullClient,
                "POST",
                String.format(Locale.ROOT, START_FORECASTER, forecasterId),
                ImmutableMap.of(),
                (HttpEntity) null,
                null
            );
        responseMap = entityAsMap(response);
        String startId = (String) responseMap.get("_id");
        assertEquals(String.format(Locale.ROOT, "Expected: %s, got %s", forecasterId, startId), forecasterId, startId);

        // case 4: given a forecaster Id, read access user can read results
        List<SearchHit> hits = waitUntilResultAvailable(readClient);

        // case 5: given a forecaster Id, read access user can
        if (isResourceSharingFeatureEnabled()) {
            // not look up forecaster configuration for full-client's forecaster
            exception = expectThrows(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        readClient,
                        "GET",
                        String.format(Locale.ROOT, GET_FORECASTER, forecasterId),
                        ImmutableMap.of(),
                        (HttpEntity) null,
                        null
                    )
            );
            message = "no permissions for [cluster:admin/plugin/forecast/forecasters/get]";
            Assert.assertTrue("actual: " + exception.getMessage(), exception.getMessage().contains(message));
        } else {
            // look up forecaster configuration if resource sharing feature is disabled
            response = TestHelpers
                .makeRequest(
                    readClient,
                    "GET",
                    String.format(Locale.ROOT, GET_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                );

            responseMap = entityAsMap(response);
            String parsedName = (String) ((Map<String, Object>) responseMap.get("forecaster")).get("name");
            assertEquals(String.format(Locale.ROOT, "Expected: %s, got %s", NAME, parsedName), NAME, parsedName);
        }

        // case 6: read access user can run top forecast on an HC forecaster
        long forecastFrom = (long) (hits.get(0).getSourceAsMap().get("data_end_time"));

        // Create the JSON string using String.format
        String topForcastRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"size\": 3,\n"
                    + "    \"forecast_from\": %d,\n"
                    + "    \"filter_by\": \"CUSTOM_QUERY\",\n"
                    + "    \"filter_query\": {\n"
                    + "        \"nested\": {\n"
                    + "            \"path\": \"entity\",\n"
                    + "            \"query\": {\n"
                    + "                \"bool\": {\n"
                    + "                    \"must\": [\n"
                    + "                        {\n"
                    + "                            \"term\": {\n"
                    + "                                \"entity.name\": \"%s\"\n"
                    + "                            }\n"
                    + "                        },\n"
                    + "                        {\n"
                    + "                            \"wildcard\": {\n"
                    + "                                \"entity.value\": \"%s\"\n"
                    + "                            }\n"
                    + "                        }\n"
                    + "                    ]\n"
                    + "                }\n"
                    + "            }\n"
                    + "        }\n"
                    + "    },\n"
                    + "    \"subaggregations\": [\n"
                    + "        {\n"
                    + "            \"aggregation_query\": {\n"
                    + "                \"forecast_value_max\": {\n"
                    + "                    \"min\": {\n"
                    + "                        \"field\": \"forecast_value\"\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            },\n"
                    + "            \"order\": \"ASC\"\n"
                    + "        }\n"
                    + "    ],\n"
                    + "    \"run_once\": false\n"
                    + "}",
                forecastFrom,
                ENTITY_NAME,
                ENTITY_VALUE
            );

        if (isResourceSharingFeatureEnabled()) {
            // not look up forecaster configuration for full-client's forecaster
            exception = expectThrows(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        readClient,
                        "POST",
                        String.format(Locale.ROOT, TOP_FORECASTER, forecasterId),
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(topForcastRequest),
                        null
                    )
            );
            message = "no permissions for [cluster:admin/plugin/forecast/forecasters/get]";
            Assert.assertTrue("actual: " + exception.getMessage(), exception.getMessage().contains(message));
        } else {
            response = TestHelpers
                .makeRequest(
                    readClient,
                    "POST",
                    String.format(Locale.ROOT, TOP_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(topForcastRequest),
                    null
                );
            responseMap = entityAsMap(response);
            List<Object> parsedBuckets = (List<Object>) responseMap.get("buckets");
            assertTrue(parsedBuckets.size() > 0);
        }

        // case 7: read access user is able to run profile API
        if (isResourceSharingFeatureEnabled()) {
            // not look up forecaster configuration for full-client's forecaster
            exception = expectThrows(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        readClient,
                        "GET",
                        String.format(Locale.ROOT, PROFILE_ALL_FORECASTER, forecasterId),
                        ImmutableMap.of(),
                        (HttpEntity) null,
                        null
                    )
            );
            message = "no permissions for [cluster:admin/plugin/forecast/forecasters/get]";
            Assert.assertTrue("actual: " + exception.getMessage(), exception.getMessage().contains(message));
        } else {
            response = TestHelpers
                .makeRequest(
                    readClient,
                    "GET",
                    String.format(Locale.ROOT, PROFILE_ALL_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                );
            responseMap = entityAsMap(response);
            String parsedState = (String) responseMap.get("state");
            assertEquals(String.format(Locale.ROOT, "Expected: %s, got %s", "RUNNING", parsedState), "RUNNING", parsedState);
        }

        // case 28: read access user is able to run stats API
        response = TestHelpers.makeRequest(readClient, "GET", STATS_FORECASTER, ImmutableMap.of(), (HttpEntity) null, null);
        responseMap = entityAsMap(response);
        int parsedHcCount = (Integer) responseMap.get("hc_forecaster_count");
        assertEquals(String.format(Locale.ROOT, "Expected: %d, got %d", 1, parsedHcCount), 1, parsedHcCount);

        // case 29: for a config that a user has no access, profile API returns 403
        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    noClient,
                    "GET",
                    String.format(Locale.ROOT, PROFILE_ALL_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                )
        );
        int statusCode = responseException.getResponse().getStatusLine().getStatusCode();
        Assert.assertEquals("actual :" + statusCode, 403, statusCode);

        // case 8: read access user cannot stop a job
        exception = expectThrows(
            IOException.class,
            () -> TestHelpers
                .makeRequest(
                    readClient,
                    "POST",
                    String.format(Locale.ROOT, START_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                )
        );
        Assert
            .assertTrue(
                "actual: " + exception.getMessage(),
                exception.getMessage().contains("no permissions for [cluster:admin/plugin/forecast/forecaster/jobmanagement]")
            );

        // case 9: full access user can stop a job
        response = TestHelpers
            .makeRequest(
                fullClient,
                "POST",
                String.format(Locale.ROOT, STOP_FORECASTER, forecasterId),
                ImmutableMap.of(),
                (HttpEntity) null,
                null
            );
        responseMap = entityAsMap(response);
        String parsedId = (String) responseMap.get("_id");
        assertEquals(String.format(Locale.ROOT, "Expected: %s, got %s", forecasterId, parsedId), forecasterId, parsedId);

        // case 10: read access user cannot run once
        exception = expectThrows(
            IOException.class,
            () -> TestHelpers
                .makeRequest(
                    readClient,
                    "POST",
                    String.format(Locale.ROOT, RUN_ONCE_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                )
        );
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/plugin/forecast/forecaster/runOnce]"));

        // case 11: full access user can run once
        response = TestHelpers
            .makeRequest(
                fullClient,
                "POST",
                String.format(Locale.ROOT, RUN_ONCE_FORECASTER, forecasterId),
                ImmutableMap.of(),
                (HttpEntity) null,
                null
            );
        responseMap = entityAsMap(response);
        String parsedTaskId = (String) responseMap.get("task_id");
        assertNotNull(parsedTaskId);

        // case 12: read access user can run search task
        String taskSearchRequest = "{\n"
            + "    \"from\": 0,\n"
            + "    \"size\": 1000,\n"
            + "    \"query\": {\n"
            + "        \"bool\": {\n"
            + "            \"filter\": [\n"
            + "                {\n"
            + "                    \"term\": {\n"
            + "                        \"is_latest\": {\n"
            + "                            \"value\": true,\n"
            + "                            \"boost\": 1.0\n"
            + "                        }\n"
            + "                    }\n"
            + "                },\n"
            + "                {\n"
            + "                    \"terms\": {\n"
            + "                        \"task_type\": [\n"
            + "                            \"RUN_ONCE_FORECAST_HC_FORECASTER\"\n"
            + "                        ],\n"
            + "                        \"boost\": 1.0\n"
            + "                    }\n"
            + "                }\n"
            + "            ],\n"
            + "            \"adjust_pure_negative\": true,\n"
            + "            \"boost\": 1.0\n"
            + "        }\n"
            + "    },\n"
            + "    \"sort\": [\n"
            + "        {\n"
            + "            \"execution_start_time\": {\n"
            + "                \"order\": \"desc\"\n"
            + "            }\n"
            + "        }\n"
            + "    ]\n"
            + "}";
        response = TestHelpers
            .makeRequest(
                readClient,
                "POST",
                String.format(Locale.ROOT, SEARCH_TASK_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(taskSearchRequest),
                null
            );
        SearchResponse searchResponse = SearchResponse
            .fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
        long total = searchResponse.getHits().getTotalHits().value();
        assertTrue("got: " + total, total > 0);

        // case 13: read access user can validate forecaster
        String invalidForecasterDef = "{\n"
            + "    \"name\": \"%s\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"max1\",\n"
            + "            \"feature_name\": \"max1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                        \"max1\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"visitCount\"\n"
            + "                            }\n"
            + "                        }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": %d,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24,\n"
            + "    \"forecast_interval\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 10,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"category_field\": [\"%s\"]"
            + "}";

        // +1 to make sure it is big enough
        windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        String invalidForecaster = String.format(Locale.ROOT, invalidForecasterDef, NAME, windowDelayMinutes, ENTITY_NAME);
        response = TestHelpers
            .makeRequest(
                readClient,
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(invalidForecaster),
                null
            );
        responseMap = entityAsMap(response);
        String parsedMsg = (String) ((Map<String, Object>) ((Map<String, Object>) responseMap.get("forecaster")).get("indices"))
            .get("message");
        assertEquals("got: " + parsedMsg, "Indices should be set", parsedMsg);

        // case 14: User get 400 error when running suggest API with invalid forecaster configuration
        responseException = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    readClient,
                    "POST",
                    String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(invalidForecaster),
                    null
                )
        );
        statusCode = responseException.getResponse().getStatusLine().getStatusCode();
        Assert.assertEquals("actual :" + statusCode, 400, statusCode);

        // case 15: User without cluster:admin/plugin/forecast/forecaster/suggest cluster permission cannot run suggest API
        exception = expectThrows(
            IOException.class,
            () -> TestHelpers
                .makeRequest(
                    noClient,
                    "POST",
                    String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(formattedForecaster),
                    null
                )
        );
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/plugin/forecast/forecaster/suggest]"));

        // case 16: read access user created in the prerequisite section can run suggest API
        response = TestHelpers
            .makeRequest(
                readClient,
                "POST",
                String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        responseMap = entityAsMap(response);
        int parsedInterval = (Integer) ((Map<String, Object>) ((Map<String, Object>) responseMap.get("interval")).get("period"))
            .get("interval");
        assertEquals("got: " + parsedInterval, 10, parsedInterval);

        // case 17: read access user can search forecaster
        response = TestHelpers
            .makeRequest(
                readClient,
                "GET",
                String.format(Locale.ROOT, SEARCH_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(searchForecasterRequest),
                null
            );
        searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
        total = searchResponse.getHits().getTotalHits().value();
        assertTrue("got: " + total, total > 0);

        // case 18: read access user cannot update forecaster
        ForecastTaskProfile forecastTaskProfile = (ForecastTaskProfile) waitUntilTaskReachState(
            forecasterId,
            ImmutableSet.of(TaskState.TEST_COMPLETE.name()),
            fullClient
        ).get(0);
        assertTrue(forecastTaskProfile != null);
        assertTrue(forecastTaskProfile.getTask().isLatest());

        exception = expectThrows(
            IOException.class,
            () -> TestHelpers
                .makeRequest(
                    readClient,
                    "PUT",
                    String.format(Locale.ROOT, UPDATE_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(formattedForecaster),
                    null
                )
        );
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/plugin/forecast/forecaster/write]"));

        // case 19: full access user can update forecaster
        response = TestHelpers
            .makeRequest(
                fullClient,
                "PUT",
                String.format(Locale.ROOT, UPDATE_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        responseMap = entityAsMap(response);
        String parsedForecasterId = (String) responseMap.get("_id");
        assertEquals("got: " + parsedForecasterId, forecasterId, parsedForecasterId);

        // case 20: read access user cannot delete forecaster
        exception = expectThrows(
            IOException.class,
            () -> TestHelpers
                .makeRequest(
                    readClient,
                    "DELETE",
                    String.format(Locale.ROOT, DELETE_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                )
        );
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/plugin/forecast/forecaster/delete]"));

        // case 21: read access user can count number of forecaster
        response = TestHelpers
            .makeRequest(
                readClient,
                "GET",
                String.format(Locale.ROOT, COUNT_FORECASTER, forecasterId),
                ImmutableMap.of(),
                (HttpEntity) null,
                null
            );
        responseMap = entityAsMap(response);
        int parsedCount = (Integer) responseMap.get("count");
        assertEquals("got: " + parsedCount, 1, parsedCount);

        // case 22: read access user can look up forecaster
        response = TestHelpers
            .makeRequest(
                readClient,
                "GET",
                String.format(Locale.ROOT, MATCH_FORECASTER, forecasterId),
                ImmutableMap.of("name", NAME),
                (HttpEntity) null,
                null
            );
        responseMap = entityAsMap(response);
        boolean parsedMatch = (Boolean) responseMap.get("match");
        assertTrue(parsedMatch);

        // case 23: A user cannot create a forecaster without source data present
        String noDataForecasterDef = "{\n"
            + "    \"name\": \"%s\",\n"
            + "    \"indices\": [\n"
            + "        \"blah\"\n"
            + "    ],\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"max1\",\n"
            + "            \"feature_name\": \"max1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                        \"max1\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"visitCount\"\n"
            + "                            }\n"
            + "                        }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": %d,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24,\n"
            + "    \"forecast_interval\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 10,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"category_field\": [\"%s\"]"
            + "}";

        // +1 to make sure it is big enough
        windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        String noDataForecaster = String.format(Locale.ROOT, noDataForecasterDef, NAME, windowDelayMinutes, ENTITY_NAME);
        responseException = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    fullClient,
                    "POST",
                    String.format(Locale.ROOT, CREATE_FORECASTER),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(noDataForecaster),
                    null
                )
        );
        int parsedCode = responseException.getResponse().getStatusLine().getStatusCode();
        assertEquals("got: " + parsedCode, 404, parsedCode);

        // case 24: for an non-existent config, profile API returns 404
        responseException = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    readClient,
                    "GET",
                    String.format(Locale.ROOT, PROFILE_ALL_FORECASTER, "blah"),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                )
        );
        Assert.assertEquals(404, responseException.getResponse().getStatusLine().getStatusCode());

        // case 25: Full access user cannot find forecasters that do not share their backend role
        try {
            enableFilterBy();
            response = TestHelpers
                .makeRequest(
                    fullClient,
                    "GET",
                    String.format(Locale.ROOT, SEARCH_FORECASTER),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(searchForecasterRequest),
                    null
                );
            searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            total = searchResponse.getHits().getTotalHits().value();
            assertTrue("got: " + total, total == 0);
        } finally {
            disableFilterBy();
        }

        // case 26: read access user cannot delete forecaster
        exception = expectThrows(
            IOException.class,
            () -> TestHelpers
                .makeRequest(
                    readClient,
                    "DELETE",
                    String.format(Locale.ROOT, DELETE_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                )
        );
        Assert.assertTrue(exception.getMessage().contains("no permissions for [cluster:admin/plugin/forecast/forecaster/delete]"));

        // case 27: full access user can delete forecaster
        response = TestHelpers
            .makeRequest(
                fullClient,
                "DELETE",
                String.format(Locale.ROOT, DELETE_FORECASTER, forecasterId),
                ImmutableMap.of(),
                (HttpEntity) null,
                null
            );
        responseMap = entityAsMap(response);
        parsedForecasterId = (String) responseMap.get("_id");
        assertEquals("got: " + parsedForecasterId, forecasterId, parsedForecasterId);
    }

    public void testFilterBy() throws IOException {
        try {
            enableFilterBy();

            // case 1: Both SDE and DevOps user create a forecaster. They can only see their own forecaster.
            Response response = TestHelpers
                .makeRequest(
                    devOpsClient,
                    "POST",
                    String.format(Locale.ROOT, CREATE_FORECASTER),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(formattedForecaster),
                    null
                );
            Map<String, Object> responseMap = entityAsMap(response);
            String devOpsForecasterId = (String) responseMap.get("_id");
            assertNotNull(devOpsForecasterId);

            // Define the malicious payload
            String sdeForecasterDef = "{\n"
                + "    \"name\": \"%s\",\n"
                + "    \"description\": \"OK rate\",\n"
                + "    \"time_field\": \"timestamp\",\n"
                + "    \"indices\": [\n"
                + "        \"%s\"\n"
                + "    ],\n"
                + "    \"feature_attributes\": [\n"
                + "        {\n"
                + "            \"feature_id\": \"max1\",\n"
                + "            \"feature_name\": \"max1\",\n"
                + "            \"feature_enabled\": true,\n"
                + "            \"importance\": 1,\n"
                + "            \"aggregation_query\": {\n"
                + "                        \"max1\": {\n"
                + "                            \"max\": {\n"
                + "                                \"field\": \"visitCount\"\n"
                + "                            }\n"
                + "                        }\n"
                + "            }\n"
                + "        }\n"
                + "    ],\n"
                + "    \"window_delay\": {\n"
                + "        \"period\": {\n"
                + "            \"interval\": %d,\n"
                + "            \"unit\": \"MINUTES\"\n"
                + "        }\n"
                + "    },\n"
                + "    \"ui_metadata\": {\n"
                + "        \"aabb\": {\n"
                + "            \"ab\": \"bb\"\n"
                + "        }\n"
                + "    },\n"
                + "    \"schema_version\": 2,\n"
                + "    \"horizon\": 24,\n"
                + "    \"forecast_interval\": {\n"
                + "        \"period\": {\n"
                + "            \"interval\": 10,\n"
                + "            \"unit\": \"MINUTES\"\n"
                + "        }\n"
                + "    },\n"
                + "    \"category_field\": [\"%s\"]"
                + "}";

            // +1 to make sure it is big enough
            windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
            String sdeForecaster = String
                .format(Locale.ROOT, sdeForecasterDef, "sde-forcaster", RULE_DATASET_NAME, windowDelayMinutes, ENTITY_NAME);

            response = TestHelpers
                .makeRequest(
                    sdeClient,
                    "POST",
                    String.format(Locale.ROOT, CREATE_FORECASTER),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(sdeForecaster),
                    null
                );
            responseMap = entityAsMap(response);
            String sdeForecasterId = (String) responseMap.get("_id");
            assertTrue(!sdeForecasterId.equals(devOpsForecasterId));

            // Extract the forecaster details
            Map<String, Object> forecaster = (Map<String, Object>) responseMap.get("forecaster");

            // Get the description from the response
            String responseDescription = (String) forecaster.get("description");

            // Expected encoded description
            String expectedEncodedDescription = "OK rate";

            // Assert that the description is correctly encoded
            assertEquals("Description encoding mismatch", expectedEncodedDescription, responseDescription);

            response = TestHelpers
                .makeRequest(
                    sdeClient,
                    "GET",
                    String.format(Locale.ROOT, SEARCH_FORECASTER),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(searchForecasterRequest),
                    null
                );
            SearchResponse searchResponse = SearchResponse
                .fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            long total = searchResponse.getHits().getTotalHits().value();
            assertTrue("got: " + total, total == 1);

            response = TestHelpers
                .makeRequest(
                    devOpsClient,
                    "GET",
                    String.format(Locale.ROOT, SEARCH_FORECASTER),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(searchForecasterRequest),
                    null
                );
            searchResponse = SearchResponse.fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            total = searchResponse.getHits().getTotalHits().value();
            assertTrue("got: " + total, total == 1);

            // case 2: Full access user cannot start/stop/delete forecaster created by other user
            ResponseException responseException = expectThrows(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        fullClient,
                        "POST",
                        String.format(Locale.ROOT, START_FORECASTER, devOpsForecasterId),
                        ImmutableMap.of(),
                        (HttpEntity) null,
                        null
                    )
            );
            Assert.assertEquals(403, responseException.getResponse().getStatusLine().getStatusCode());

            response = TestHelpers
                .makeRequest(
                    devOpsClient,
                    "POST",
                    String.format(Locale.ROOT, START_FORECASTER, devOpsForecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                );
            responseMap = entityAsMap(response);
            String startId = (String) responseMap.get("_id");
            assertEquals(String.format(Locale.ROOT, "Expected: %s, got %s", devOpsForecasterId, startId), devOpsForecasterId, startId);

            responseException = expectThrows(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        fullClient,
                        "POST",
                        String.format(Locale.ROOT, STOP_FORECASTER, devOpsForecasterId),
                        ImmutableMap.of(),
                        (HttpEntity) null,
                        null
                    )
            );
            Assert.assertEquals(403, responseException.getResponse().getStatusLine().getStatusCode());

            response = TestHelpers
                .makeRequest(
                    devOpsClient,
                    "POST",
                    String.format(Locale.ROOT, STOP_FORECASTER, devOpsForecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                );
            responseMap = entityAsMap(response);
            String stopId = (String) responseMap.get("_id");
            assertEquals(String.format(Locale.ROOT, "Expected: %s, got %s", devOpsForecasterId, stopId), devOpsForecasterId, stopId);

            responseException = expectThrows(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        fullClient,
                        "DELETE",
                        String.format(Locale.ROOT, DELETE_FORECASTER, devOpsForecasterId),
                        ImmutableMap.of(),
                        (HttpEntity) null,
                        null
                    )
            );
            Assert.assertEquals(403, responseException.getResponse().getStatusLine().getStatusCode());

            response = TestHelpers
                .makeRequest(
                    devOpsClient,
                    "DELETE",
                    String.format(Locale.ROOT, DELETE_FORECASTER, devOpsForecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                );
            responseMap = entityAsMap(response);
            String deleteId = (String) responseMap.get("_id");
            assertEquals(String.format(Locale.ROOT, "Expected: %s, got %s", devOpsForecasterId, deleteId), devOpsForecasterId, deleteId);

        } finally {
            disableFilterBy();
        }
    }

    private List<SearchHit> createStartWaitResult(RestClient client) throws IOException, InterruptedException {
        Response response = TestHelpers
            .makeRequest(
                client,
                "POST",
                String.format(Locale.ROOT, CREATE_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        Map<String, Object> responseMap = entityAsMap(response);
        String forecasterId = (String) responseMap.get("_id");
        assertNotNull(forecasterId);

        response = TestHelpers
            .makeRequest(
                client,
                "POST",
                String.format(Locale.ROOT, START_FORECASTER, forecasterId),
                ImmutableMap.of(),
                (HttpEntity) null,
                null
            );
        responseMap = entityAsMap(response);
        String startId = (String) responseMap.get("_id");
        assertEquals(String.format(Locale.ROOT, "Expected: %s, got %s", forecasterId, startId), forecasterId, startId);

        return waitUntilResultAvailable(client);
    }

    public void testDFS() throws IOException, InterruptedException {
        if (isResourceSharingFeatureEnabled()) {
            Exception exception = expectThrows(ResponseException.class, () -> createStartWaitResult(phoenixReadClient));
            Assert.assertTrue("actual: " + exception.getMessage(), exception.getMessage().contains("no permissions for"));
            return;
        }

        // case 1: Forecast job follows user permission (e.g., since the user can only access phoenix, the job does so too)
        List<SearchHit> hits = createStartWaitResult(phoenixReadClient);

        for (int i = 0; i < hits.size(); i++) {
            String value = ((Map<String, String>) ((List<Object>) (hits.get(i).getSourceAsMap().get("entity"))).get(0)).get("value");
            assertEquals("actual: " + value, PHOENIX_VALUE, value);
        }
    }

    public void testSystemIndex() throws IOException, InterruptedException {
        if (isResourceSharingFeatureEnabled()) {
            Exception exception = expectThrows(ResponseException.class, () -> createStartWaitResult(fullClient));
            Assert.assertTrue("actual: " + exception.getMessage(), exception.getMessage().contains("no permissions for"));
            return;
        }
        List<SearchHit> hits = createStartWaitResult(fullClient);
        assertTrue(hits.size() > 0);

        // case 1: Access state index using forecaster_full_user
        checkSystemIndex(".opensearch-forecast-state");

        // case 2: Access config index using forecaster_full_user
        Response response = searchResultMatchAll(fullClient, ".opensearch-forecasters");
        Assert.assertEquals(0, toHits(response).size());

        // case 3: Access checkpoint index using forecaster_full_user
        checkSystemIndex(".opensearch-forecast-checkpoints");

        // case 4: Access job index using forecaster_full_user
        response = searchResultMatchAll(fullClient, ".opendistro-anomaly-detector-jobs");
        Assert.assertEquals(0, toHits(response).size());
    }

    private void checkSystemIndex(String index) throws IOException {
        try {
            Response response = searchResultMatchAll(fullClient, index);
            Assert.assertEquals(0, toHits(response));
        } catch (AssertionError error) {
            // due to https://github.com/opensearch-project/security/issues/4755, we can search as well. Change to check write rejection.
            // regular user cannot delete system index
            ResponseException responseException = expectThrows(
                ResponseException.class,
                () -> TestHelpers
                    .makeRequest(
                        fullClient,
                        "DELETE",
                        index,
                        ImmutableMap.of(),
                        (HttpEntity) null,
                        ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
                    )
            );

            Assert.assertEquals(RestStatus.FORBIDDEN.getStatus(), responseException.getResponse().getStatusLine().getStatusCode());
        }
    }

    public void testResultAccess() throws IOException, InterruptedException {
        if (isResourceSharingFeatureEnabled()) {
            Exception exception = expectThrows(ResponseException.class, () -> createStartWaitResult(devOpsLimitedClient));
            Assert.assertTrue("actual: " + exception.getMessage(), exception.getMessage().contains("no permissions for"));
            return;
        }

        List<SearchHit> hits = createStartWaitResult(devOpsLimitedClient);
        assertTrue(hits.size() > 0);

        // case 1: an unexpected user cannot access results
        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> searchResultMatchAll(noResultClient, "opensearch-forecast-result*")
        );
        Assert.assertEquals(403, responseException.getResponse().getStatusLine().getStatusCode());
        Assert
            .assertTrue(
                "actual: " + responseException.getMessage(),
                responseException.getMessage().contains("no permissions for [indices:data/read/search]")
            );

        // case 2: Backend role can help limit result access
        try {
            enableFilterBy();

            Response response = searchResultMatchAll(sdeLimitedClient, "opensearch-forecast-result*");
            SearchResponse searchResponse = SearchResponse
                .fromXContent(createParser(JsonXContent.jsonXContent, response.getEntity().getContent()));
            long total = searchResponse.getHits().getTotalHits().value();
            assertEquals("actual: " + total, 0, total);
        } finally {
            disableFilterBy();
        }
    }

    public void testCreate() throws IOException {
        // case 1: Output encoding of forecaster description
        String maliciousDescription = "ok rate '<script>console.log('hi')</script>'";
        String maliciousForecasterDef = "{\n"
            + "    \"name\": \"%s\",\n"
            + "    \"description\": \"%s\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"max1\",\n"
            + "            \"feature_name\": \"max1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                        \"max1\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"visitCount\"\n"
            + "                            }\n"
            + "                        }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": %d,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24,\n"
            + "    \"forecast_interval\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 10,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"category_field\": [\"%s\"]"
            + "}";

        windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        String maliciousName = "malicious-forcaster";
        String maliciousForecaster = String
            .format(
                Locale.ROOT,
                maliciousForecasterDef,
                maliciousName,
                maliciousDescription,
                RULE_DATASET_NAME,
                windowDelayMinutes,
                ENTITY_NAME
            );

        Response response = TestHelpers
            .makeRequest(
                fullClient,
                "POST",
                String.format(Locale.ROOT, CREATE_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(maliciousForecaster),
                null
            );
        Map<String, Object> responseMap = entityAsMap(response);

        // Extract the forecaster details
        Map<String, Object> forecaster = (Map<String, Object>) responseMap.get("forecaster");

        // Get the description from the response
        String responseDescription = (String) forecaster.get("description");

        // Expected encoded description
        String expectedEncodedDescription = "ok rate &#39;&lt;script&gt;console.log(&#39;hi&#39;)&lt;/script&gt;&#39;";

        // Assert that the description is correctly encoded
        assertEquals("Description encoding mismatch", expectedEncodedDescription, responseDescription);

        // case 2:Description parameter has length validation
        // Generate a string longer than 1000 characters
        StringBuilder longDescriptionBuilder = new StringBuilder();
        for (int i = 0; i < 1001; i++) {
            longDescriptionBuilder.append("a");
        }
        String longDescription = longDescriptionBuilder.toString();

        String lengthyForecasterDef = "{\n"
            + "    \"name\": \"%s\",\n"
            + "    \"description\": \"%s\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"max1\",\n"
            + "            \"feature_name\": \"max1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                        \"max1\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"visitCount\"\n"
            + "                            }\n"
            + "                        }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": %d,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24,\n"
            + "    \"forecast_interval\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 10,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"category_field\": [\"%s\"]"
            + "}";

        windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        String lengthyForecaster = String
            .format(
                Locale.ROOT,
                lengthyForecasterDef,
                "lengthy-forcaster",
                longDescription,
                RULE_DATASET_NAME,
                windowDelayMinutes,
                ENTITY_NAME
            );

        ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    fullClient,
                    "POST",
                    String.format(Locale.ROOT, CREATE_FORECASTER),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(lengthyForecaster),
                    null
                )
        );
        Assert.assertEquals(400, responseException.getResponse().getStatusLine().getStatusCode());
        Assert
            .assertTrue(
                "actual: " + responseException.getMessage(),
                responseException.getMessage().contains("Description length is too long. Max length is 1000 characters.")
            );

        // case 3: A user cannot create a forecaster with a name that is already used by another forecaster
        responseException = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    fullClient,
                    "POST",
                    String.format(Locale.ROOT, CREATE_FORECASTER),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(maliciousForecaster),
                    null
                )
        );
        assertEquals(409, responseException.getResponse().getStatusLine().getStatusCode());
        Assert
            .assertTrue(
                "actual: " + responseException.getMessage(),
                responseException
                    .getMessage()
                    .contains(
                        String
                            .format(
                                Locale.ROOT,
                                "Cannot create forecasters with name [%s] as it's already used by another forecaster",
                                maliciousName
                            )
                    )
            );

        // case 4: A user cannot update a forecaster with a name that is already used by another forecaster
        response = TestHelpers
            .makeRequest(
                fullClient,
                "POST",
                String.format(Locale.ROOT, CREATE_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        responseMap = entityAsMap(response);
        String forecasterId = (String) responseMap.get("_id");
        assertNotNull(forecasterId);

        responseException = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    fullClient,
                    "PUT",
                    String.format(Locale.ROOT, UPDATE_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(maliciousForecaster),
                    null
                )
        );
        Assert
            .assertTrue(
                "actual: " + responseException.getMessage(),
                responseException
                    .getMessage()
                    .contains(
                        String
                            .format(
                                Locale.ROOT,
                                "Cannot create forecasters with name [%s] as it's already used by another forecaster",
                                maliciousName
                            )
                    )
            );

        // case 5: Error messages on user’s input has encoding
        // Define the malicious input
        String maliciousInput = "<script>alert (1)</script>";

        // Attempt to profile the forecaster with the malicious '_all' parameter
        responseException = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    fullClient,
                    "GET",
                    String.format(Locale.ROOT, PROFILE_FORECASTER, forecasterId),
                    ImmutableMap.of("_all", maliciousInput),
                    (HttpEntity) null,
                    null
                )
        );

        // Verify that the status code is 400 Bad Request
        int statusCode = responseException.getResponse().getStatusLine().getStatusCode();
        Assert.assertEquals("Expected status code 400", 400, statusCode);

        // Parse the error response
        responseMap = entityAsMap(responseException.getResponse());

        // Extract error details
        Map<String, Object> error = (Map<String, Object>) responseMap.get("error");
        String errorType = (String) error.get("type");
        String errorReason = (String) error.get("reason");

        // Assert the error type
        Assert.assertEquals("Expected error type 'illegal_argument_exception'", "illegal_argument_exception", errorType);

        // Assert the error reason contains the expected message
        String expectedErrorMessage =
            "Failed to parse value [&lt;script&gt;alert (1)&lt;/script&gt;] as only [true] or [false] are allowed.";
        Assert.assertEquals("Error reason mismatch", expectedErrorMessage, errorReason);

        // Verify that the malicious input is encoded in the error message
        Assert
            .assertTrue(
                "Expected malicious input to be encoded in the error message",
                errorReason.contains("&lt;script&gt;alert (1)&lt;/script&gt;")
            );

        // Attempt to get the forecaster with the malicious 'task' parameter
        responseException = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    fullClient,
                    "GET",
                    String.format(Locale.ROOT, GET_FORECASTER, forecasterId),
                    ImmutableMap.of("task", maliciousInput),
                    (HttpEntity) null,
                    null
                )
        );

        // Verify that the status code is 400 Bad Request
        statusCode = responseException.getResponse().getStatusLine().getStatusCode();
        Assert.assertEquals("Expected status code 400", 400, statusCode);

        // Parse the error response
        responseMap = entityAsMap(responseException.getResponse());

        // Extract error details
        error = (Map<String, Object>) responseMap.get("error");
        errorType = (String) error.get("type");
        errorReason = (String) error.get("reason");

        // Assert the error type
        Assert.assertEquals("Expected error type 'illegal_argument_exception'", "illegal_argument_exception", errorType);

        // Expected error message
        expectedErrorMessage = "Failed to parse value [&lt;script&gt;alert (1)&lt;/script&gt;] as only [true] or [false] are allowed.";

        // Assert the error reason contains the expected message
        Assert.assertEquals("Error reason mismatch", expectedErrorMessage, errorReason);

        // Verify that the malicious input is encoded in the error message
        Assert
            .assertTrue(
                "Expected malicious input to be encoded in the error message",
                errorReason.contains("&lt;script&gt;alert (1)&lt;/script&gt;")
            );
    }

}
