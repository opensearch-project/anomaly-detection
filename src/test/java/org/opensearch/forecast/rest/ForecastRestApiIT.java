/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest;

import static org.hamcrest.Matchers.containsString;
import static org.opensearch.timeseries.util.RestHandlerUtils.RUN_ONCE;
import static org.opensearch.timeseries.util.RestHandlerUtils.START_JOB;
import static org.opensearch.timeseries.util.RestHandlerUtils.STOP_JOB;
import static org.opensearch.timeseries.util.RestHandlerUtils.SUGGEST;
import static org.opensearch.timeseries.util.RestHandlerUtils.VALIDATE;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.AbstractForecastSyntheticDataTest;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.model.ForecastTaskProfile;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.EntityTaskProfile;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.util.RestHandlerUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;

/**
 * Test the following Restful API:
 *  - Suggest
 *  - Validate
 *  - Create
 *  - run once
 *  - start
 *  - stop
 *  - update
 */
public class ForecastRestApiIT extends AbstractForecastSyntheticDataTest {
    public static final int MAX_RETRY_TIMES = 200;
    private static final String SUGGEST_INTERVAL_URI;
    private static final String SUGGEST_INTERVAL_HORIZON_HISTORY_URI;
    private static final String VALIDATE_FORECASTER;
    private static final String VALIDATE_FORECASTER_MODEL;
    private static final String CREATE_FORECASTER;
    private static final String RUN_ONCE_FORECASTER;
    private static final String START_FORECASTER;
    private static final String STOP_FORECASTER;
    private static final String UPDATE_FORECASTER;

    static {
        SUGGEST_INTERVAL_URI = String
            .format(
                Locale.ROOT,
                "%s/%s/%s",
                TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI,
                SUGGEST,
                Forecaster.FORECAST_INTERVAL_FIELD
            );
        SUGGEST_INTERVAL_HORIZON_HISTORY_URI = String
            .format(
                Locale.ROOT,
                "%s/%s/%s,%s,%s",
                TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI,
                SUGGEST,
                Forecaster.FORECAST_INTERVAL_FIELD,
                Forecaster.HORIZON_FIELD,
                Config.HISTORY_INTERVAL_FIELD
            );
        VALIDATE_FORECASTER = String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, VALIDATE);
        VALIDATE_FORECASTER_MODEL = String
            .format(Locale.ROOT, "%s/%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, VALIDATE, "model");
        CREATE_FORECASTER = TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI;
        RUN_ONCE_FORECASTER = String.format(Locale.ROOT, "%s/%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s", RUN_ONCE);
        START_FORECASTER = String.format(Locale.ROOT, "%s/%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s", START_JOB);
        STOP_FORECASTER = String.format(Locale.ROOT, "%s/%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s", STOP_JOB);
        UPDATE_FORECASTER = String.format(Locale.ROOT, "%s/%s", TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI, "%s");
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings(ForecastEnabledSetting.FORECAST_ENABLED, true);
    }

    /**
     * manipulate the data to have three categorical fields. Originally, the data file has one categorical field with two possible values.
     * Simulate we have enough raw data but the categorical value caused sparsity.
     * @param trainTestSplit the number of rows to load
     * @return train time
     * @throws Exception when failing to ingest data
     */
    private static Instant loadSparseCategoryData(int trainTestSplit) throws Exception {
        RestClient client = client();

        String dataFileName = String.format(Locale.ROOT, "org/opensearch/ad/e2e/data/%s.data", RULE_DATASET_NAME);
        List<JsonObject> data = readJsonArrayWithLimit(dataFileName, trainTestSplit);

        // Deep copy the list using Gson

        String mapping = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\":"
            + "\"date\""
            + "},"
            + " \"transform._doc_count\": { \"type\": \"integer\" },"
            + "\"component1Name\": { \"type\": \"keyword\"},"
            + "\"component2Name\": { \"type\": \"keyword\"}"
            + "} } }";
        int phonenixIndex = 0;
        int scottsdaleIndex = 0;
        for (int i = 0; i < trainTestSplit; i++) {
            JsonObject row = data.get(i);

            // Get the value of the "componentName" field
            String componentName = row.get("componentName").getAsString();

            // Replace the field based on the value of "componentName"
            row.remove("componentName");  // Remove the original "componentName" field

            if ("Phoenix".equals(componentName)) {
                if (phonenixIndex % 2 == 0) {
                    row.addProperty("component1Name", "server1");
                    row.addProperty("component2Name", "app1");
                } else {
                    row.addProperty("component1Name", "server2");
                    row.addProperty("component2Name", "app1");
                }
                phonenixIndex++;
            } else if ("Scottsdale".equals(componentName)) {
                if (scottsdaleIndex % 2 == 0) {
                    row.addProperty("component1Name", "server3");
                    row.addProperty("component2Name", "app2");
                } else {
                    row.addProperty("component1Name", "server4");
                    row.addProperty("component2Name", "app2");
                }
                scottsdaleIndex++;
            }
        }

        bulkIndexTrainData(RULE_DATASET_NAME, data, trainTestSplit, client, mapping);
        String trainTimeStr = data.get(trainTestSplit - 1).get("timestamp").getAsString();
        return Instant.ofEpochMilli(Long.parseLong(trainTimeStr));
    }

    /**
     * Test suggest API. I wrote multiple cases together to save time in loading data. Cannot load data once for
     * all tests as client is set up in the instance method of OpenSearchRestTestCase and we need client to
     * ingest data. Also, the JUnit 5 feature @BeforeAll is not supported in OpenSearchRestTestCase and the code
     * inside @BeforeAll won't be executed even if I upgrade junit from 4 to 5 in build.gradle.
     * @throws Exception when loading data
     */
    public void testSuggestOneMinute() throws Exception {
        loadSyntheticData(200);
        // case 1: suggest 1 minute interval for a time series with 1 minute cadence
        String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24\n"
            + "}";

        String formattedForecaster = String.format(Locale.ROOT, forecasterDef, SYNTHETIC_DATASET_NAME);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Suggest forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> suggestions = (Map<String, Object>) ((Map<String, Object>) responseMap.get("interval")).get("period");
        assertEquals(1, (int) suggestions.get("interval"));
        assertEquals("Minutes", suggestions.get("unit"));

        // case 2: If you provide forecaster interval in the request body, we will ignore it.
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Detector-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
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
            + "            \"interval\": 4,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    }\n"
            + "}";
        formattedForecaster = String.format(Locale.ROOT, forecasterDef, SYNTHETIC_DATASET_NAME);
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        responseMap = entityAsMap(response);
        suggestions = (Map<String, Object>) ((Map<String, Object>) responseMap.get("interval")).get("period");
        assertEquals(1, (int) suggestions.get("interval"));
        assertEquals("Minutes", suggestions.get("unit"));

        // case 3: We also support horizon and history.
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Detector-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"shingle_size\": 5,\n"
            + "    \"forecast_interval\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 4,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    }\n"
            + "}";
        formattedForecaster = String.format(Locale.ROOT, forecasterDef, SYNTHETIC_DATASET_NAME);
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, SUGGEST_INTERVAL_HORIZON_HISTORY_URI),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        responseMap = entityAsMap(response);
        Map<String, Object> intervalSuggestions = (Map<String, Object>) ((Map<String, Object>) responseMap.get("interval")).get("period");
        assertEquals(1, (int) intervalSuggestions.get("interval"));
        assertEquals("Minutes", intervalSuggestions.get("unit"));

        int horizonSuggestions = ((Integer) responseMap.get("horizon"));
        assertEquals(15, horizonSuggestions);

        int historySuggestions = ((Integer) responseMap.get("history"));
        assertEquals(37, historySuggestions);

        // case 4: no feature is ok
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Detector-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
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
            + "            \"interval\": 4,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    }\n"
            + "}";
        formattedForecaster = String.format(Locale.ROOT, forecasterDef, SYNTHETIC_DATASET_NAME);
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        responseMap = entityAsMap(response);
        suggestions = (Map<String, Object>) ((Map<String, Object>) responseMap.get("interval")).get("period");
        assertEquals(1, (int) suggestions.get("interval"));
        assertEquals("Minutes", suggestions.get("unit"));
    }

    public void testSuggestTenMinute() throws Exception {
        loadRuleData(200);
        // case 1: The following request validates against the source data to see if model training might succeed. In this example, the data
        // is ingested at a rate of every ten minutes.
        final String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"transform._doc_count\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24\n"
            + "}";

        String formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Suggest forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> suggestions = (Map<String, Object>) ((Map<String, Object>) responseMap.get("interval")).get("period");
        assertEquals(10, (int) suggestions.get("interval"));
        assertEquals("Minutes", suggestions.get("unit"));

        // case 2: If you provide unsupported parameters, we will throw 400 error.
        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    String.format(Locale.ROOT, SUGGEST_INTERVAL_URI + "2"),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(forecasterDef),
                    null
                )
        );
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        assertTrue(
            "Expect contains" + CommonMessages.NOT_EXISTENT_SUGGEST_TYPE + " ; but got" + exception.getMessage(),
            exception.getMessage().contains(CommonMessages.NOT_EXISTENT_SUGGEST_TYPE)
        );

        // case 3: If the provided configuration lacks enough information (e.g., source index), we will throw 400 error.
        final String errorForecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"transform._doc_count\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24\n"
            + "}";

        ResponseException noSourceIndexException = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(errorForecasterDef),
                    null
                )
        );
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), noSourceIndexException.getResponse().getStatusLine().getStatusCode());
        assertTrue(
            "Expect contains" + CommonMessages.EMPTY_INDICES + " ; but got" + noSourceIndexException.getMessage(),
            noSourceIndexException.getMessage().contains(CommonMessages.EMPTY_INDICES)
        );
    }

    public void testSuggestSparseData() throws Exception {
        loadSyntheticData(10);
        // case 1: suggest 1 minute interval for a time series with 1 minute cadence
        String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24\n"
            + "}";

        String formattedForecaster = String.format(Locale.ROOT, forecasterDef, SYNTHETIC_DATASET_NAME);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Suggest forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        // no suggestion
        assertEquals(0, responseMap.size());
    }

    /**
     * Test data interval is larger than 1 hr and we fail to suggest
     */
    public void testFailToSuggest() throws Exception {
        int trainTestSplit = 100;
        String categoricalField = "componentName";
        GenData dataGenerated = genUniformSingleFeatureData(
            70,
            trainTestSplit,
            1,
            categoricalField,
            MISSING_MODE.NO_MISSING_DATA,
            -1,
            -1,
            50
        );
        ingestUniformSingleFeatureData(trainTestSplit, dataGenerated.data, UNIFORM_DATASET_NAME, categoricalField);

        // case 1: IntervalCalculation.findMinimumInterval cannot find any data point in the last 40 points and return 1 minute instead.
        // We keep searching and find nothing below 1 hr and then return.
        String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"data\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24\n"
            + "}";

        String formattedForecaster = String.format(Locale.ROOT, forecasterDef, UNIFORM_DATASET_NAME);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Suggest forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        assertEquals(0, responseMap.size());

        // case 2: IntervalCalculation.findMinimumInterval find an interval larger than 1 hr by going through the last 240 points.
        // findMinimumInterval returns null and we stop searching further.
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"data\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24,\n"
            + "    \"history\": 240\n"
            + "}";

        formattedForecaster = String.format(Locale.ROOT, forecasterDef, UNIFORM_DATASET_NAME);

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, SUGGEST_INTERVAL_URI),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Suggest forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        assertEquals(0, responseMap.size());
    }

    public void testValidate() throws Exception {
        loadSyntheticData(200);
        // case 1: forecaster interval is not set
        String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"ui_metadata\": {\n"
            + "        \"aabb\": {\n"
            + "            \"ab\": \"bb\"\n"
            + "        }\n"
            + "    },\n"
            + "    \"schema_version\": 2,\n"
            + "    \"horizon\": 24\n"
            + "}";

        String formattedForecaster = String.format(Locale.ROOT, forecasterDef, SYNTHETIC_DATASET_NAME);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> validations = (Map<String, Object>) ((Map<String, Object>) responseMap.get("forecaster"))
            .get("forecast_interval");
        assertEquals("Forecast interval should be set", validations.get("message"));

        // case 2: if there is no problem, nothing shows
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
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
            + "            \"interval\": 1,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    }\n"
            + "}";

        formattedForecaster = String.format(Locale.ROOT, forecasterDef, SYNTHETIC_DATASET_NAME);

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        // no issues
        assertEquals(0, responseMap.size());

        // case 3: the feature query aggregates over a field that doesn’t exist in the data source:
        // Note: this only works for aggregation without a default value. For sum/count, we won't
        // detect as OpenSearch still returns default value.
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
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
            + "                \"max1\": {\n"
            + "                    \"max\": {\n"
            + "                        \"field\": \"Feature10\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
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
            + "            \"interval\": 1,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    }\n"
            + "}";

        formattedForecaster = String.format(Locale.ROOT, forecasterDef, SYNTHETIC_DATASET_NAME);

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        validations = (Map<String, Object>) ((Map<String, Object>) responseMap.get("forecaster")).get("feature_attributes");
        assertEquals("Feature has an invalid query returning empty aggregated data: max1", validations.get("message"));
        assertEquals(
            "Feature has an invalid query returning empty aggregated data",
            ((Map<String, Object>) validations.get("sub_issues")).get("max1")
        );
    }

    public void testValidateSparseData() throws Exception {
        Instant trainTime = loadSyntheticData(10);
        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes();

        // case 1 : sparse data
        String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
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
            + "            \"interval\": 1,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    }\n"
            + "}";

        String formattedForecaster = String.format(Locale.ROOT, forecasterDef, SYNTHETIC_DATASET_NAME, windowDelayMinutes);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster model failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> validations = (Map<String, Object>) ((Map<String, Object>) responseMap.get("model")).get("indices");
        assertEquals(
            "Source index data is potentially too sparse for model training. Consider changing interval length or ingesting more data",
            validations.get("message")
        );
    }

    public void testValidateTenMinute() throws Exception {
        Instant trainTime = loadRuleData(200);

        // case 1: The following request validates against the source data to see if model training might succeed.
        // In this example, the data is ingested at a rate of every 10 minutes, and forecaster interval is set to 1 minute.
        String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"transform._doc_count\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
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
            + "            \"interval\": 1,\n"
            + "            \"unit\": \"MINUTES\"\n"
            + "        }\n"
            + "    }\n"
            + "}";

        String formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> validation = (Map<String, Object>) ((Map<String, Object>) responseMap.get("model")).get("forecast_interval");
        assertEquals(
            "The selected interval might collect sparse data. Consider changing interval length to: 10",
            validation.get("message")
        );
        Map<String, Object> suggested = (Map<String, Object>) ((Map<String, Object>) validation.get("suggested_value")).get("period");
        assertEquals("Minutes", suggested.get("unit"));
        assertEquals(10, (int) suggested.get("interval"));

        // case 2: Another scenario might indicate that you can change filter_query (data filter) because the currently filtered data is too
        // sparse for
        // the model to train correctly, which can happen because the index is also ingesting data that falls outside the chosen filter.
        // Using another
        // filter_query can make your data more dense.
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"transform._doc_count\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
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
            + "    \"filter_query\": {\n"
            + "        \"bool\": {\n"
            + "            \"filter\": [\n"
            + "                {\n"
            + "                    \"range\": {\n"
            + "                        \"@timestamp\": {\n"
            + "                            \"lt\": 1\n"
            + "                        }\n"
            + "                    }\n"
            + "                }\n"
            + "            ],\n"
            + "            \"adjust_pure_negative\": true,\n"
            + "            \"boost\": 1\n"
            + "        }\n"
            + "    }\n"
            + "}";

        formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME);

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        validation = (Map<String, Object>) ((Map<String, Object>) responseMap.get("model")).get("filter_query");
        assertEquals("Data is too sparse after data filter is applied. Consider changing the data filter", validation.get("message"));

        // case 3: window delay too small
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"transform._doc_count\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
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
            + "    }\n"
            + "}";

        formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME);

        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes();

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        validation = (Map<String, Object>) ((Map<String, Object>) responseMap.get("model")).get("window_delay");

        String regex = "Latest seen data point is at least \\d+ minutes ago. Consider changing window delay to at least \\d+ minutes.";

        // Compile the pattern
        Pattern pattern = Pattern.compile(regex);

        String validationMsg = (String) validation.get("message");
        Matcher matcher = pattern.matcher(validationMsg);
        assertTrue("Message does not match the expected pattern.", matcher.matches());

        Map<String, Object> suggestions = (Map<String, Object>) ((Map<String, Object>) validation.get("suggested_value")).get("period");
        assertTrue("should be at least " + windowDelayMinutes, (int) suggestions.get("interval") >= windowDelayMinutes);
        assertEquals("should be Minutes", "Minutes", suggestions.get("unit"));

        // case 4: feature query format invalid
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "    \"aggregation_query\": {\n"
            + "        \"filter\": {\n"
            + "            \"bool\": {\n"
            + "                \"must\": [\n"
            + "                    {\n"
            + "                        \"range\": {\n"
            + "                            \"@timestamp\": {\n"
            + "                                \"lt\": 1\n"
            + "                            }\n"
            + "                        }\n"
            + "                    }\n"
            + "                ]\n"
            + "            }\n"
            + "        },\n"
            + "        \"aggregations\": {\n"
            + "            \"sum1\": {\n"
            + "                \"sum\": {\n"
            + "                    \"field\": \"transform._doc_count\"\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
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
            + "    }\n"
            + "}";

        formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME);

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        validation = (Map<String, Object>) ((Map<String, Object>) responseMap.get("forecaster")).get("feature_attributes");
        assertEquals("Custom query error: Unknown aggregation type [bool]", validation.get("message"));

        // case 5: filter in feature query causes empty data
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
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
            + "                \"filtered_max_1\": {\n"
            + "                    \"filter\": {\n"
            + "                        \"bool\": {\n"
            + "                            \"must\": [\n"
            + "                                {\n"
            + "                                    \"range\": {\n"
            + "                                        \"timestamp\": {\n"
            + "                                            \"lt\": 1\n"
            + "                                        }\n"
            + "                                    }\n"
            + "                                }\n"
            + "                            ]\n"
            + "                        }\n"
            + "                    },\n"
            + "                    \"aggregations\": {\n"
            + "                        \"max1\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"transform._doc_count\"\n"
            + "                            }\n"
            + "                        }\n"
            + "                    }\n"
            + "                }\n"
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
            + "    }\n"
            + "}";

        // +1 to make sure it is big enough
        windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, windowDelayMinutes);
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        validation = (Map<String, Object>) ((Map<String, Object>) responseMap.get("forecaster")).get("feature_attributes");
        assertEquals("Feature has an invalid query returning empty aggregated data: max1", validation.get("message"));

        // case 6: wrong field name in feature query causes empty data
        // Note we cannot deal with aggregation with default value like sum.
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
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
            + "                \"max1\": {\n"
            + "                    \"max\": {\n"
            + "                        \"field\": \"transform._doc_count2\"\n"
            + "                    }\n"
            + "                }\n"
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
            + "    }\n"
            + "}";

        // +1 to make sure it is big enough
        windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, windowDelayMinutes);
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        validation = (Map<String, Object>) ((Map<String, Object>) responseMap.get("forecaster")).get("feature_attributes");
        assertEquals("Feature has an invalid query returning empty aggregated data: max1", validation.get("message"));

        // case 7: filter in feature query causes sparse data
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
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
            + "                \"filtered_max_1\": {\n"
            + "                    \"filter\": {\n"
            + "                        \"bool\": {\n"
            + "                            \"must\": [\n"
            + "                                {\n"
            + "                                    \"range\": {\n"
            + "                                        \"timestamp\": {\n"
            + "                                            \"lt\": %d\n"
            + "                                        }\n"
            + "                                    }\n"
            + "                                }\n"
            + "                            ]\n"
            + "                        }\n"
            + "                    },\n"
            + "                    \"aggregations\": {\n"
            + "                        \"max1\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"transform._doc_count\"\n"
            + "                            }\n"
            + "                        }\n"
            + "                    }\n"
            + "                }\n"
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
            + "    }\n"
            + "}";

        // +1 to make sure it is big enough
        windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        // we have 100 timestamps (2 entities per timestamp). Timestamps are 10 minutes apart. If we subtract 70 * 10 = 700 minutes, we have
        // sparse data.
        formattedForecaster = String
            .format(
                Locale.ROOT,
                forecasterDef,
                RULE_DATASET_NAME,
                trainTime.minus(700, ChronoUnit.MINUTES).toEpochMilli(),
                windowDelayMinutes
            );
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        validation = (Map<String, Object>) ((Map<String, Object>) responseMap.get("model")).get("feature_attributes");
        assertEquals(
            "Data is most likely too sparse when given feature queries are applied. Consider revising feature queries: max1",
            validation.get("message")
        );
        assertEquals(
            "Data is most likely too sparse when given feature queries are applied. Consider revising feature queries",
            ((Map<String, Object>) validation.get("sub_issues")).get("max1")
        );

        // case 8: two features will fail
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
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
            + "                \"filtered_max_1\": {\n"
            + "                    \"filter\": {\n"
            + "                        \"bool\": {\n"
            + "                            \"must\": [\n"
            + "                                {\n"
            + "                                    \"range\": {\n"
            + "                                        \"timestamp\": {\n"
            + "                                            \"lt\": %d\n"
            + "                                        }\n"
            + "                                    }\n"
            + "                                }\n"
            + "                            ]\n"
            + "                        }\n"
            + "                    },\n"
            + "                    \"aggregations\": {\n"
            + "                        \"max1\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"transform._doc_count\"\n"
            + "                            }\n"
            + "                        }\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        },\n"
            + "        {\n"
            + "            \"feature_id\": \"max2\",\n"
            + "            \"feature_name\": \"max2\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"filtered_max_2\": {\n"
            + "                    \"filter\": {\n"
            + "                        \"bool\": {\n"
            + "                            \"must\": [\n"
            + "                                {\n"
            + "                                    \"range\": {\n"
            + "                                        \"timestamp\": {\n"
            + "                                            \"lt\": %d\n"
            + "                                        }\n"
            + "                                    }\n"
            + "                                }\n"
            + "                            ]\n"
            + "                        }\n"
            + "                    },\n"
            + "                    \"aggregations\": {\n"
            + "                        \"max2\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"transform._doc_count\"\n"
            + "                            }\n"
            + "                        }\n"
            + "                    }\n"
            + "                }\n"
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
            + "    }\n"
            + "}";

        // +1 to make sure it is big enough
        windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        // we have 100 timestamps (2 entities per timestamp). Timestamps are 10 minutes apart. If we subtract 70 * 10 = 700 minutes, we have
        // sparse data.
        long filterTimestamp = trainTime.minus(700, ChronoUnit.MINUTES).toEpochMilli();
        formattedForecaster = String
            .format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, filterTimestamp, filterTimestamp, windowDelayMinutes);
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        validation = (Map<String, Object>) ((Map<String, Object>) responseMap.get("forecaster")).get("feature_attributes");
        assertEquals("Can't create more than 1 feature(s)", validation.get("message"));
    }

    public void testValidateHC() throws Exception {
        Instant trainTime = loadSparseCategoryData(82);
        // add 2 to be safe
        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 2;

        // case 1: index does not exist
        String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"transform._doc_count\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ],\n"
            + "    \"window_delay\": {\n"
            + "        \"period\": {\n"
            + "            \"interval\": 20,\n"
            + "            \"unit\": \"SECONDS\"\n"
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
            + "    \"category_field\": [\"%s\"]\n"
            + "}";

        String formattedForecaster = String
            .format(Locale.ROOT, forecasterDef, SYNTHETIC_DATASET_NAME, windowDelayMinutes, "component1Name,component2Name");

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster model failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> validations = (Map<String, Object>) ((Map<String, Object>) responseMap.get("forecaster")).get("indices");
        assertEquals("index does not exist", validations.get("message"));

        // case 2: invalid categorical field
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"transform._doc_count\"\n"
            + "                    }\n"
            + "                }\n"
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
            + "    \"category_field\": [\"%s\"]\n"
            + "}";

        formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, windowDelayMinutes, "476465");

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster model failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        validations = (Map<String, Object>) ((Map<String, Object>) responseMap.get("forecaster")).get("category_field");
        assertEquals("Can't find the categorical field 476465 in index [rule]", validations.get("message"));

        // case 3: validate data sparsity with one categorical field
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"transform._doc_count\"\n"
            + "                    }\n"
            + "                }\n"
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
            + "    \"category_field\": [\"%s\"]\n"
            + "}";

        formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, windowDelayMinutes, "component1Name");

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster model failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        validations = (Map<String, Object>) ((Map<String, Object>) responseMap.get("model")).get("category_field");
        assertEquals(
            "Data is most likely too sparse with the given category fields. Consider revising category field/s or ingesting more data.",
            validations.get("message")
        );

        // case 4: validate data sparsity with two categorical fields
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_id\": \"sum1\",\n"
            + "            \"feature_name\": \"sum1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"sum1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"transform._doc_count\"\n"
            + "                    }\n"
            + "                }\n"
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
            + "    \"category_field\": [\"%s\", \"%s\"]\n"
            + "}";

        formattedForecaster = String
            .format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, windowDelayMinutes, "component1Name", "component2Name");

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_FORECASTER_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        assertEquals("Validate forecaster model failed", RestStatus.OK, TestHelpers.restStatus(response));
        responseMap = entityAsMap(response);
        validations = (Map<String, Object>) ((Map<String, Object>) responseMap.get("model")).get("category_field");
        assertEquals(
            "Data is most likely too sparse with the given category fields. Consider revising category field/s or ingesting more data.",
            validations.get("message")
        );
    }

    public void testCreate() throws Exception {
        Instant trainTime = loadRuleData(200);
        // case 1: create two features will fail
        String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
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
            + "                \"filtered_max_1\": {\n"
            + "                    \"filter\": {\n"
            + "                        \"bool\": {\n"
            + "                            \"must\": [\n"
            + "                                {\n"
            + "                                    \"range\": {\n"
            + "                                        \"timestamp\": {\n"
            + "                                            \"lt\": %d\n"
            + "                                        }\n"
            + "                                    }\n"
            + "                                }\n"
            + "                            ]\n"
            + "                        }\n"
            + "                    },\n"
            + "                    \"aggregations\": {\n"
            + "                        \"max1\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"transform._doc_count\"\n"
            + "                            }\n"
            + "                        }\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        },\n"
            + "        {\n"
            + "            \"feature_id\": \"max2\",\n"
            + "            \"feature_name\": \"max2\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"importance\": 1,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"filtered_max_2\": {\n"
            + "                    \"filter\": {\n"
            + "                        \"bool\": {\n"
            + "                            \"must\": [\n"
            + "                                {\n"
            + "                                    \"range\": {\n"
            + "                                        \"timestamp\": {\n"
            + "                                            \"lt\": %d\n"
            + "                                        }\n"
            + "                                    }\n"
            + "                                }\n"
            + "                            ]\n"
            + "                        }\n"
            + "                    },\n"
            + "                    \"aggregations\": {\n"
            + "                        \"max2\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"transform._doc_count\"\n"
            + "                            }\n"
            + "                        }\n"
            + "                    }\n"
            + "                }\n"
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
            + "    }\n"
            + "}";

        // +1 to make sure it is big enough
        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        // we have 100 timestamps (2 entities per timestamp). Timestamps are 10 minutes apart. If we subtract 70 * 10 = 700 minutes, we have
        // sparse data.
        long filterTimestamp = trainTime.minus(700, ChronoUnit.MINUTES).toEpochMilli();
        final String formattedForecaster = String
            .format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, filterTimestamp, filterTimestamp, windowDelayMinutes);
        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    String.format(Locale.ROOT, CREATE_FORECASTER),
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(formattedForecaster),
                    null
                )
        );
        MatcherAssert.assertThat(ex.getMessage(), containsString("Can't create more than 1 feature(s)"));

        // case 2: create forecaster with custom index
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
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
            + "                \"filtered_max_1\": {\n"
            + "                    \"filter\": {\n"
            + "                        \"bool\": {\n"
            + "                            \"must\": [\n"
            + "                                {\n"
            + "                                    \"range\": {\n"
            + "                                        \"timestamp\": {\n"
            + "                                            \"lt\": %d\n"
            + "                                        }\n"
            + "                                    }\n"
            + "                                }\n"
            + "                            ]\n"
            + "                        }\n"
            + "                    },\n"
            + "                    \"aggregations\": {\n"
            + "                        \"max1\": {\n"
            + "                            \"max\": {\n"
            + "                                \"field\": \"transform._doc_count\"\n"
            + "                            }\n"
            + "                        }\n"
            + "                    }\n"
            + "                }\n"
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
            + "   \"result_index\": \"opensearch-forecast-result-b\"\n"
            + "}";

        // +1 to make sure it is big enough
        windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        // we have 100 timestamps (2 entities per timestamp). Timestamps are 10 minutes apart. If we subtract 70 * 10 = 700 minutes, we have
        // sparse data.
        String formattedForecaster2 = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, filterTimestamp, windowDelayMinutes);
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, CREATE_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster2),
                null
            );
        Map<String, Object> responseMap = entityAsMap(response);
        assertEquals("opensearch-forecast-result-b", ((Map<String, Object>) responseMap.get("forecaster")).get("result_index"));
    }

    public void testRunOnce() throws Exception {
        Instant trainTime = loadRuleData(200);
        // case 1: happy case
        String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
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
            + "                                \"field\": \"transform._doc_count\"\n"
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
            + "    }\n"
            + "}";

        // +1 to make sure it is big enough
        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        final String formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, windowDelayMinutes);
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, CREATE_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        Map<String, Object> responseMap = entityAsMap(response);
        String forecasterId = (String) responseMap.get("_id");

        // run once
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, RUN_ONCE_FORECASTER, forecasterId),
                ImmutableMap.of(),
                (HttpEntity) null,
                null
            );

        ForecastTaskProfile forecastTaskProfile = (ForecastTaskProfile) waitUntilTaskReachState(
            forecasterId,
            ImmutableSet.of(TaskState.TEST_COMPLETE.name())
        ).get(0);
        assertTrue(forecastTaskProfile != null);
        assertTrue(forecastTaskProfile.getTask().isLatest());

        responseMap = entityAsMap(response);
        String taskId = (String) responseMap.get(EntityTaskProfile.TASK_ID_FIELD);
        assertEquals(taskId, forecastTaskProfile.getTaskId());

        response = searchTaskResult(taskId);
        responseMap = entityAsMap(response);
        int total = (int) (((Map<String, Object>) ((Map<String, Object>) responseMap.get("hits")).get("total")).get("value"));
        assertTrue("actual: " + total, total > 40);

        // case 2: cannot run once while forecaster is started
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, START_FORECASTER, forecasterId),
                ImmutableMap.of(),
                (HttpEntity) null,
                null
            );
        responseMap = entityAsMap(response);
        assertEquals(forecasterId, responseMap.get("_id"));

        // starting another run once before finishing causes error
        Exception ex = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    String.format(Locale.ROOT, RUN_ONCE_FORECASTER, forecasterId),
                    ImmutableMap.of(),
                    (HttpEntity) null,
                    null
                )
        );

        String reason = ex.getMessage();
        assertTrue("actual: " + reason, reason.contains("Cannot run once " + forecasterId + " when real time job is running."));

        // case 3: stop forecaster
        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, STOP_FORECASTER, forecasterId),
                ImmutableMap.of(),
                (HttpEntity) null,
                null
            );
        responseMap = entityAsMap(response);
        assertEquals(forecasterId, responseMap.get("_id"));
    }

    public ForecastTaskProfile getForecastTaskProfile(String forecasterId) throws IOException, ParseException {
        Response profileResponse = TestHelpers
            .makeRequest(
                client(),
                "GET",
                TimeSeriesAnalyticsPlugin.FORECAST_FORECASTERS_URI + "/" + forecasterId + "/_profile/" + ForecastCommonName.FORECAST_TASK,
                ImmutableMap.of(),
                "",
                null
            );
        return parseForecastTaskProfile(profileResponse);
    }

    public Response searchTaskResult(String taskId) throws IOException {
        Response response = TestHelpers
            .makeRequest(
                client(),
                "GET",
                "opensearch-forecast-result*/_search",
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity(
                        "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"task_id\":\"" + taskId + "\"}}]}},\"track_total_hits\":true}"
                    ),
                null
            );
        return response;
    }

    public ForecastTaskProfile parseForecastTaskProfile(Response profileResponse) throws IOException, ParseException {
        String profileResult = EntityUtils.toString(profileResponse.getEntity());
        XContentParser parser = TestHelpers.parser(profileResult);
        ForecastTaskProfile forecastTaskProfile = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if ("forecast_task".equals(fieldName)) {
                forecastTaskProfile = ForecastTaskProfile.parse(parser);
            } else {
                parser.skipChildren();
            }
        }
        return forecastTaskProfile;
    }

    protected List<Object> waitUntilTaskReachState(String forecasterId, Set<String> targetStates) throws InterruptedException {
        List<Object> results = new ArrayList<>();
        int i = 0;
        ForecastTaskProfile forecastTaskProfile = null;
        // Increase retryTimes if some task can't reach done state
        while ((forecastTaskProfile == null || !targetStates.contains(forecastTaskProfile.getTask().getState())) && i < MAX_RETRY_TIMES) {
            try {
                forecastTaskProfile = getForecastTaskProfile(forecasterId);
            } catch (Exception e) {
                logger.error("failed to get ForecastTaskProfile", e);
            } finally {
                Thread.sleep(1000);
            }
            i++;
        }
        assertNotNull(forecastTaskProfile);
        results.add(forecastTaskProfile);
        results.add(i);
        return results;
    }

    public void testCreateDetector() throws Exception {
        // Case 1: users cannot specify forecaster id when creating a forecaster
        Instant trainTime = loadRuleData(200);
        String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
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
            + "                                \"field\": \"transform._doc_count\"\n"
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
            + "    }\n"
            + "}";

        // +1 to make sure it is big enough
        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        final String formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, windowDelayMinutes);
        String blahId = "__blah__";
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, CREATE_FORECASTER),
                ImmutableMap.of(RestHandlerUtils.FORECASTER_ID, blahId),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        Map<String, Object> responseMap = entityAsMap(response);
        String forecasterId = (String) responseMap.get("_id");
        assertNotEquals("response is missing Id", blahId, forecasterId);
    }

    public void testUpdateDetector() throws Exception {
        // Case 1: update non-impactful fields like name or description won't change last breaking change UI time
        Instant trainTime = loadRuleData(200);
        String forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
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
            + "                                \"field\": \"transform._doc_count\"\n"
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
            + "    }\n"
            + "}";

        // +1 to make sure it is big enough
        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        final String formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, windowDelayMinutes);
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, CREATE_FORECASTER),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        Map<String, Object> responseMap = entityAsMap(response);
        String forecasterId = (String) responseMap.get("_id");
        assertEquals(null, responseMap.get("last_ui_breaking_change_time"));

        // changing description won't change last_breaking_change_ui_time
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate1\",\n"
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
            + "                                \"field\": \"transform._doc_count\"\n"
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
            + "    }\n"
            + "}";
        response = TestHelpers
            .makeRequest(
                client(),
                "PUT",
                String.format(Locale.ROOT, UPDATE_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        responseMap = entityAsMap(response);
        assertEquals(null, responseMap.get("last_ui_breaking_change_time"));

        // changing categorical fields changes last_ui_breaking_change_time
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate1\",\n"
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
            + "                                \"field\": \"transform._doc_count\"\n"
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
            + "    \"category_field\": [\"componentName\"]"
            + "}";
        response = TestHelpers
            .makeRequest(
                client(),
                "PUT",
                String.format(Locale.ROOT, UPDATE_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        responseMap = entityAsMap(response);
        assertEquals(responseMap.get("last_update_time"), responseMap.get("last_ui_breaking_change_time"));

        // changing custom result index changes last_ui_breaking_change_time
        forecasterDef = "{\n"
            + "    \"name\": \"Second-Test-Forecaster-4\",\n"
            + "    \"description\": \"ok rate1\",\n"
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
            + "                                \"field\": \"transform._doc_count\"\n"
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
            + "    \"category_field\": [\"componentName\"],"
            + "    \"result_index\": \"opensearch-forecast-result-b\""
            + "}";
        response = TestHelpers
            .makeRequest(
                client(),
                "PUT",
                String.format(Locale.ROOT, UPDATE_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedForecaster),
                null
            );
        responseMap = entityAsMap(response);
        assertEquals(responseMap.get("last_update_time"), responseMap.get("last_ui_breaking_change_time"));
    }
}
