/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest;

import static org.hamcrest.Matchers.containsString;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hc.core5.http.HttpEntity;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.forecast.AbstractForecastSyntheticDataTest;
import org.opensearch.forecast.model.ForecastTaskProfile;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.search.SearchHit;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.EntityTaskProfile;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.util.RestHandlerUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonObject;

/**
 * Test the following Restful API:
 *  - top forecast
 *  - start
 *  - stop
 *  - Create
 *  - run once
 *  - Validate
 *  - Suggest
 *  - update
 */
public class ForecastRestApiIT extends AbstractForecastSyntheticDataTest {
    public static final int MAX_RETRY_TIMES = 200;
    private static final String CITY_NAME = "cityName";
    private static final String CONFIDENCE_INTERVAL_WIDTH = "confidence_interval_width";
    private static final String FORECAST_VALUE = "forecast_value";
    private static final String MIN_CONFIDENCE_INTERVAL = "MIN_CONFIDENCE_INTERVAL_WIDTH";
    private static final String MAX_CONFIDENCE_INTERVAL = "MAX_CONFIDENCE_INTERVAL_WIDTH";

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
            + " \"visitCount\": { \"type\": \"integer\" },"
            + "\"component1Name\": { \"type\": \"keyword\"},"
            + "\"component2Name\": { \"type\": \"keyword\"}"
            + "} } }";
        int phonenixIndex = 0;
        int scottsdaleIndex = 0;
        for (int i = 0; i < trainTestSplit; i++) {
            JsonObject row = data.get(i);

            // Get the value of the "cityName" field
            String cityName = row.get(CITY_NAME).getAsString();

            // Replace the field based on the value of "cityName"
            row.remove(CITY_NAME);  // Remove the original "cityName" field

            if ("Phoenix".equals(cityName)) {
                if (phonenixIndex % 2 == 0) {
                    row.addProperty("component1Name", "server1");
                    row.addProperty("component2Name", "app1");
                } else {
                    row.addProperty("component1Name", "server2");
                    row.addProperty("component2Name", "app1");
                }
                phonenixIndex++;
            } else if ("Scottsdale".equals(cityName)) {
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
        assertEquals(199, historySuggestions);

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
            + "                        \"field\": \"visitCount\"\n"
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
            + "                        \"field\": \"visitCount\"\n"
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
        String categoricalField = CITY_NAME;
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
        assertEquals("actual response: " + responseMap, 0, responseMap.size());

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
            + "                        \"field\": \"visitCount\"\n"
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
            "The selected interval might collect sparse data. Consider changing interval length to: 10 minutes",
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
            + "                        \"field\": \"visitCount\"\n"
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
            + "                        \"field\": \"visitCount\"\n"
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
            + "                    \"field\": \"visitCount\"\n"
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
            + "                                \"field\": \"visitCount\"\n"
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
            + "                        \"field\": \"visitCount2\"\n"
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
            + "                                \"field\": \"visitCount\"\n"
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
            + "                                \"field\": \"visitCount\"\n"
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
            + "                                \"field\": \"visitCount\"\n"
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
            + "                        \"field\": \"visitCount\"\n"
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
            + "                        \"field\": \"visitCount\"\n"
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
            + "                        \"field\": \"visitCount\"\n"
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
            + "                        \"field\": \"visitCount\"\n"
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
            + "                                \"field\": \"visitCount\"\n"
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
            + "                                \"field\": \"visitCount\"\n"
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

        // Case 2: users cannot specify forecaster id when creating a forecaster
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
            + "    }\n"
            + "}";

        // +1 to make sure it is big enough
        windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        final String formattedForecasterId = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, windowDelayMinutes);
        String blahId = "__blah__";
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, CREATE_FORECASTER),
                ImmutableMap.of(RestHandlerUtils.FORECASTER_ID, blahId),
                TestHelpers.toHttpEntity(formattedForecasterId),
                null
            );
        Map<String, Object> responseMap = entityAsMap(response);
        String forecasterId = (String) responseMap.get("_id");
        assertNotEquals("response is missing Id", blahId, forecasterId);
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
            + "   \"result_index\": \"opensearch-forecast-result-b\",\n"
            + "    \"category_field\": [\"%s\"]\n"
            + "}";

        // +1 to make sure it is big enough
        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        final String formattedForecaster = String.format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, windowDelayMinutes, CITY_NAME);
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
        assertEquals("opensearch-forecast-result-b", ((Map<String, Object>) responseMap.get("forecaster")).get("result_index"));

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
            ImmutableSet.of(TaskState.TEST_COMPLETE.name()),
            client()
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

        List<SearchHit> hits = toHits(response);
        long forecastFrom = -1;
        for (SearchHit hit : hits) {
            Map<String, Object> source = hit.getSourceAsMap();
            if (source.get("forecast_value") != null) {
                forecastFrom = (long) (source.get("data_end_time"));
                break;
            }
        }
        assertTrue(forecastFrom != -1);

        // top forecast verification
        // our interval is 10 minutes, thus 600000 milliseconds
        minConfidenceIntervalVerification(forecasterId, forecastFrom, 600000);
        maxConfidenceIntervalVerification(forecasterId, forecastFrom, 600000);
        minForecastValueVerification(forecasterId, forecastFrom, 600000);
        maxForecastValueVerification(forecasterId, forecastFrom, 600000);
        distanceToThresholdGreaterThan(forecasterId, forecastFrom, 600000);
        distanceToThresholdGreaterThanEqual(forecasterId, forecastFrom, 600000);
        distanceToThresholdLessThan(forecasterId, forecastFrom, 600000);
        distanceToThresholdLessThanEqual(forecasterId, forecastFrom, 600000);
        customMaxForecastValue(forecasterId, forecastFrom, 600000);
        customMinForecastValue(forecasterId, forecastFrom, 600000);
        topForecastSizeVerification(forecasterId, forecastFrom);

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

    private void maxForecastValueVerification(String forecasterId, long forecastFrom, long intervalMillis) throws IOException {
        Response response;
        Map<String, Object> responseMap;
        String topForcastRequest;
        List<Object> parsedBuckets;
        double previousValue;
        topForcastRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"split_by\": \"%s\",\n"
                    + "    \"filter_by\": \"BUILD_IN_QUERY\",\n"
                    + "    \"build_in_query\": \"MAX_VALUE_WITHIN_THE_HORIZON\",\n"
                    + "    \"forecast_from\": %d,\n"
                    + "    \"run_once\": true\n"
                    + "}",
                CITY_NAME,
                forecastFrom
            );

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TOP_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(topForcastRequest),
                null
            );
        responseMap = entityAsMap(response);
        parsedBuckets = (List<Object>) responseMap.get("buckets");
        assertTrue("actual content: " + parsedBuckets, parsedBuckets.size() == 2);

        previousValue = Double.MAX_VALUE; // Initialize to positive infinity
        double largestValue = Double.MIN_VALUE;

        largestValue = isDesc(parsedBuckets, previousValue, largestValue, "MAX_VALUE_WITHIN_THE_HORIZON");

        String maxValueRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"size\": 1,\n"
                    + "    \"query\": {\n"
                    + "        \"bool\": {\n"
                    + "            \"filter\": [\n"
                    + "                {\n"
                    + "                    \"range\": {\n"
                    + "                        \"data_end_time\": {\n"
                    + "                            \"from\": %d,\n"
                    + "                            \"to\": %d,\n"
                    + "                            \"include_lower\": true,\n"
                    + "                            \"include_upper\": false\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            ]\n"
                    + "        }\n"
                    + "    },\n"
                    + "    \"sort\": [\n"
                    + "        {\n"
                    + "            \"%s\": {\n"
                    + "                \"order\": \"desc\"\n"
                    + "            }\n"
                    + "        }\n"
                    + "    ]\n"
                    + "}",
                forecastFrom,
                forecastFrom + intervalMillis,
                FORECAST_VALUE
            );

        Response maxValueResponse = TestHelpers
            .makeRequest(client(), "GET", SEARCH_RESULTS, ImmutableMap.of(), TestHelpers.toHttpEntity(maxValueRequest), null);
        List<SearchHit> maxValueHits = toHits(maxValueResponse);
        assertEquals("actual: " + maxValueHits, 1, maxValueHits.size());
        double maxValue = (double) (maxValueHits.get(0).getSourceAsMap().get(FORECAST_VALUE));
        assertEquals(String.format(Locale.ROOT, "actual: %f, expect: %f", maxValue, largestValue), maxValue, largestValue, 0.001);
    }

    private void minForecastValueVerification(String forecasterId, long forecastFrom, long intervalMillis) throws IOException {
        Response response;
        Map<String, Object> responseMap;
        String topForcastRequest;
        List<Object> parsedBuckets;
        Set<String> cities;
        topForcastRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"split_by\": \"%s\",\n"
                    + "    \"filter_by\": \"BUILD_IN_QUERY\",\n"
                    + "    \"build_in_query\": \"MIN_VALUE_WITHIN_THE_HORIZON\",\n"
                    + "    \"forecast_from\": %d,\n"
                    + "    \"run_once\": true\n"
                    + "}",
                CITY_NAME,
                forecastFrom
            );

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TOP_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(topForcastRequest),
                null
            );
        responseMap = entityAsMap(response);
        parsedBuckets = (List<Object>) responseMap.get("buckets");
        assertTrue("actual content: " + parsedBuckets, parsedBuckets.size() == 2);

        double previousValue = -Double.MAX_VALUE; // Initialize to negative infinity
        double smallestValue = Double.MAX_VALUE;
        cities = new HashSet<>();

        smallestValue = isAsc(parsedBuckets, cities, previousValue, smallestValue, "MIN_VALUE_WITHIN_THE_HORIZON");

        String minValueRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"size\": 1,\n"
                    + "    \"query\": {\n"
                    + "        \"bool\": {\n"
                    + "            \"filter\": [\n"
                    + "                {\n"
                    + "                    \"range\": {\n"
                    + "                        \"data_end_time\": {\n"
                    + "                            \"from\": %d,\n"
                    + "                            \"to\": %d,\n"
                    + "                            \"include_lower\": true,\n"
                    + "                            \"include_upper\": false\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            ]\n"
                    + "        }\n"
                    + "    },\n"
                    + "    \"sort\": [\n"
                    + "        {\n"
                    + "            \"%s\": {\n"
                    + "                \"order\": \"asc\"\n"
                    + "            }\n"
                    + "        }\n"
                    + "    ]\n"
                    + "}",
                forecastFrom,
                forecastFrom + intervalMillis,
                FORECAST_VALUE
            );

        Response minValueResponse = TestHelpers
            .makeRequest(client(), "GET", SEARCH_RESULTS, ImmutableMap.of(), TestHelpers.toHttpEntity(minValueRequest), null);
        List<SearchHit> minValueHits = toHits(minValueResponse);
        assertEquals("actual: " + minValueHits, 1, minValueHits.size());
        double minValue = (double) (minValueHits.get(0).getSourceAsMap().get("forecast_value"));
        assertEquals(String.format(Locale.ROOT, "actual: %f, expect: %f", minValue, smallestValue), minValue, smallestValue, 0.001);
    }

    private double isAsc(List<Object> parsedBuckets, Set<String> cities, double previousValue, double smallestValue, String valueKey) {
        for (Object obj : parsedBuckets) {
            assertTrue("Each element in the list must be a Map<String, Object>.", obj instanceof Map);

            @SuppressWarnings("unchecked")
            Map<String, Object> bucket = (Map<String, Object>) obj;

            // Extract value using keys
            Object valueObj = bucket.get(valueKey);
            assertTrue("actual: " + valueObj, valueObj instanceof Number);

            double value = ((Number) valueObj).doubleValue();
            if (smallestValue > value) {
                smallestValue = value;
            }

            // Check ascending order
            assertTrue(String.format(Locale.ROOT, "value %f previousValue %f", value, previousValue), value >= previousValue);

            previousValue = value;

            // Extract the key
            Object keyObj = bucket.get("key");
            assertTrue("actual: " + keyObj, keyObj instanceof Map);

            @SuppressWarnings("unchecked")
            Map<String, Object> keyMap = (Map<String, Object>) keyObj;
            String cityName = (String) keyMap.get(CITY_NAME);

            assertTrue("cityName is null", cityName != null);

            // Check that service is either "Phoenix" or "Scottsdale"
            assertTrue("cityName is " + cityName, cityName.equals("Phoenix") || cityName.equals("Scottsdale"));

            // Check for unique services
            assertTrue("Duplicate city found: " + cityName, cities.add(cityName));
        }
        return smallestValue;
    }

    private void maxConfidenceIntervalVerification(String forecasterId, long forecastFrom, long intervalMillis) throws IOException {
        Response response;
        Map<String, Object> responseMap;
        String topForcastRequest;
        List<Object> parsedBuckets;
        double previousWidth;
        topForcastRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"split_by\": \"%s\",\n"
                    + "    \"filter_by\": \"BUILD_IN_QUERY\",\n"
                    + "    \"build_in_query\": \"MAX_CONFIDENCE_INTERVAL_WIDTH\",\n"
                    + "    \"forecast_from\": %d,\n"
                    + "    \"run_once\": true\n"
                    + "}",
                CITY_NAME,
                forecastFrom
            );

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TOP_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(topForcastRequest),
                null
            );
        responseMap = entityAsMap(response);
        parsedBuckets = (List<Object>) responseMap.get("buckets");
        assertTrue("actual content: " + parsedBuckets, parsedBuckets.size() == 2);

        previousWidth = Double.MAX_VALUE; // Initialize to positive infinity
        double largestWidth = Double.MIN_VALUE;

        largestWidth = isDesc(parsedBuckets, previousWidth, largestWidth, MAX_CONFIDENCE_INTERVAL);

        String maxConfidenceIntervalRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"size\": 1,\n"
                    + "    \"query\": {\n"
                    + "        \"bool\": {\n"
                    + "            \"filter\": [\n"
                    + "                {\n"
                    + "                    \"term\": {\n"
                    + "                        \"horizon_index\": 24\n"
                    + "                    }\n"
                    + "                },\n"
                    + "                {\n"
                    + "                    \"range\": {\n"
                    + "                        \"data_end_time\": {\n"
                    + "                            \"from\": %d,\n"
                    + "                            \"to\": %d,\n"
                    + "                            \"include_lower\": true,\n"
                    + "                            \"include_upper\": false\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            ]\n"
                    + "        }\n"
                    + "    },\n"
                    + "    \"sort\": [\n"
                    + "        {\n"
                    + "            \"%s\": {\n"
                    + "                \"order\": \"desc\"\n"
                    + "            }\n"
                    + "        }\n"
                    + "    ]\n"
                    + "}",
                forecastFrom,
                forecastFrom + intervalMillis,
                CONFIDENCE_INTERVAL_WIDTH
            );

        Response maxConfidenceIntervalResponse = TestHelpers
            .makeRequest(client(), "GET", SEARCH_RESULTS, ImmutableMap.of(), TestHelpers.toHttpEntity(maxConfidenceIntervalRequest), null);
        List<SearchHit> maxConfidenceIntervalHits = toHits(maxConfidenceIntervalResponse);
        assertEquals("actual: " + maxConfidenceIntervalHits, 1, maxConfidenceIntervalHits.size());
        double maxWidth = (double) (maxConfidenceIntervalHits.get(0).getSourceAsMap().get(CONFIDENCE_INTERVAL_WIDTH));
        assertEquals(maxWidth, largestWidth, 0.001);
    }

    private void validateKeyValue(
        Map<String, Object> keyMap,
        String keyName,
        String valueDescription,
        Set<String> expectedValues,
        Set<String> uniqueValuesSet
    ) {
        // Extract the value from the keyMap using the keyName
        String value = (String) keyMap.get(keyName);

        // Ensure the value is not null
        assertTrue(valueDescription + " is null", value != null);

        // Check that the value is one of the expected values
        assertTrue(valueDescription + " is " + value, expectedValues.contains(value));

        // Check for uniqueness in the provided set
        assertTrue("Duplicate " + valueDescription + " found: " + value, uniqueValuesSet.add(value));
    }

    private double isDesc(
        List<Object> parsedBuckets,
        double previousWidth,
        Set<String> uniqueValuesSet,
        double largestWidth,
        String valueKey,
        String keyName,
        String valueDescription,
        Set<String> expectedValues
    ) {
        for (Object obj : parsedBuckets) {
            assertTrue("Each element in the list must be a Map<String, Object>.", obj instanceof Map);

            @SuppressWarnings("unchecked")
            Map<String, Object> bucket = (Map<String, Object>) obj;

            // Extract valueKey
            Object widthObj = bucket.get(valueKey);
            assertTrue("actual: " + widthObj, widthObj instanceof Number);

            double width = ((Number) widthObj).doubleValue();
            if (largestWidth < width) {
                largestWidth = width;
            }

            // Check descending order
            assertTrue(String.format(Locale.ROOT, "width %f previousWidth %f", width, previousWidth), width <= previousWidth);

            previousWidth = width;

            // Extract the key
            Object keyObj = bucket.get("key");
            assertTrue("actual: " + keyObj, keyObj instanceof Map);

            @SuppressWarnings("unchecked")
            Map<String, Object> keyMap = (Map<String, Object>) keyObj;

            // Use the helper method for validation
            validateKeyValue(keyMap, keyName, valueDescription, expectedValues, uniqueValuesSet);
        }
        return largestWidth;
    }

    private double isDesc(List<Object> parsedBuckets, double previousWidth, double largestWidth, String valueKey) {
        Set<String> cities = new HashSet<>();
        Set<String> expectedCities = new HashSet<>(Arrays.asList("Phoenix", "Scottsdale"));
        return isDesc(parsedBuckets, previousWidth, cities, largestWidth, valueKey, CITY_NAME, "cityName", expectedCities);
    }

    private double isDescTwoCategorical(List<Object> parsedBuckets, double previousWidth, double largestWidth, String valueKey) {
        Set<String> regions = new HashSet<>();
        Set<String> expectedRegions = new HashSet<>(Arrays.asList("pdx", "iad"));
        return isDesc(parsedBuckets, previousWidth, regions, largestWidth, valueKey, "region", "regionName", expectedRegions);
    }

    private void minConfidenceIntervalVerification(String forecasterId, long forecastFrom, long intervalInMillis) throws IOException {
        Response response;
        Map<String, Object> responseMap;
        String topForcastRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"split_by\": \"%s\",\n"
                    + "    \"filter_by\": \"BUILD_IN_QUERY\",\n"
                    + "    \"build_in_query\": \"MIN_CONFIDENCE_INTERVAL_WIDTH\",\n"
                    + "    \"forecast_from\": %d,\n"
                    + "    \"run_once\": true\n"
                    + "}",
                CITY_NAME,
                forecastFrom
            );

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TOP_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(topForcastRequest),
                null
            );
        responseMap = entityAsMap(response);
        List<Object> parsedBuckets = (List<Object>) responseMap.get("buckets");
        assertTrue("actual content: " + parsedBuckets, parsedBuckets.size() == 2);

        double previousWidth = -Double.MAX_VALUE; // Initialize to negative infinity
        double smallestWidth = Double.MAX_VALUE;
        Set<String> cities = new HashSet<>();

        smallestWidth = isAsc(parsedBuckets, cities, previousWidth, smallestWidth, MIN_CONFIDENCE_INTERVAL);

        String minConfidenceIntervalRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"size\": 1,\n"
                    + "    \"query\": {\n"
                    + "        \"bool\": {\n"
                    + "            \"filter\": [\n"
                    + "                {\n"
                    + "                    \"term\": {\n"
                    + "                        \"horizon_index\": 24\n"
                    + "                    }\n"
                    + "                },\n"
                    + "                {\n"
                    + "                    \"range\": {\n"
                    + "                        \"data_end_time\": {\n"
                    + "                            \"from\": %d,\n"
                    + "                            \"to\": %d,\n"
                    + "                            \"include_lower\": true,\n"
                    + "                            \"include_upper\": false\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            ]\n"
                    + "        }\n"
                    + "    },\n"
                    + "    \"sort\": [\n"
                    + "        {\n"
                    + "            \"%s\": {\n"
                    + "                \"order\": \"asc\"\n"
                    + "            }\n"
                    + "        }\n"
                    + "    ]\n"
                    + "}",
                forecastFrom,
                forecastFrom + intervalInMillis,
                CONFIDENCE_INTERVAL_WIDTH
            );

        Response minConfidenceIntervalResponse = TestHelpers
            .makeRequest(client(), "GET", SEARCH_RESULTS, ImmutableMap.of(), TestHelpers.toHttpEntity(minConfidenceIntervalRequest), null);
        List<SearchHit> minConfidenceIntervalHits = toHits(minConfidenceIntervalResponse);
        assertEquals("actual: " + minConfidenceIntervalHits, 1, minConfidenceIntervalHits.size());
        double minWidth = (double) (minConfidenceIntervalHits.get(0).getSourceAsMap().get(CONFIDENCE_INTERVAL_WIDTH));
        assertEquals(String.format(Locale.ROOT, "actual: %f, expect: %f", minWidth, smallestWidth), minWidth, smallestWidth, 0.001);
    }

    public Response searchTaskResult(String taskId) throws IOException {
        Response response = TestHelpers
            .makeRequest(
                client(),
                "GET",
                SEARCH_RESULTS,
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity(
                        "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"task_id\":\""
                            + taskId
                            + "\"}}]}},\"track_total_hits\":true,\"size\":10000}"
                    ),
                null
            );
        return response;
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
            + "    \"category_field\": [\"cityName\"]"
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
            + "    \"category_field\": [\"cityName\"],"
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

    private void distanceToThresholdGreaterThan(String forecasterId, long forecastFrom, long intervalMillis) throws IOException {
        distanceToThresholdGreaterTemplate(forecasterId, forecastFrom, false, intervalMillis);
    }

    private void distanceToThresholdGreaterTemplate(String forecasterId, long forecastFrom, boolean equal, long intervalMillis)
        throws IOException {
        Response response;
        Map<String, Object> responseMap;
        String topForcastRequest;
        List<Object> parsedBuckets;
        double previousWidth;
        int threshold = 4587;
        topForcastRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"split_by\": \"%s\",\n"
                    + "    \"filter_by\": \"BUILD_IN_QUERY\",\n"
                    + "    \"build_in_query\": \"DISTANCE_TO_THRESHOLD_VALUE\",\n"
                    + "    \"forecast_from\": %d,\n"
                    + "    \"run_once\": true,\n"
                    + "    \"threshold\": %d,\n"
                    + "    \"relation_to_threshold\": \"%s\""
                    + "}",
                CITY_NAME,
                forecastFrom,
                threshold,
                equal ? "GREATER_THAN_OR_EQUAL_TO" : "GREATER_THAN"
            );

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TOP_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(topForcastRequest),
                null
            );
        responseMap = entityAsMap(response);
        parsedBuckets = (List<Object>) responseMap.get("buckets");
        assertTrue("actual content: " + parsedBuckets, parsedBuckets.size() == 2);

        previousWidth = Double.MAX_VALUE; // Initialize to positive infinity
        double largestValue = Double.MIN_VALUE;

        largestValue = isDesc(parsedBuckets, previousWidth, largestValue, "DISTANCE_TO_THRESHOLD_VALUE");

        String maxDistanceToThresholdRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"size\": 1,\n"
                    + "    \"query\": {\n"
                    + "        \"bool\": {\n"
                    + "            \"filter\": [\n"
                    + "                {\n"
                    + "                    \"range\": {\n"
                    + "                        \"forecast_value\": {\n"
                    + "                            \"%s\": "
                    + threshold
                    + "\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                },\n"
                    + "                {\n"
                    + "                    \"range\": {\n"
                    + "                        \"data_end_time\": {\n"
                    + "                            \"from\": %d,\n"
                    + "                            \"to\": %d,\n"
                    + "                            \"include_lower\": true,\n"
                    + "                            \"include_upper\": false\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            ]\n"
                    + "        }\n"
                    + "    },\n"
                    + "    \"sort\": [\n"
                    + "        {\n"
                    + "            \"forecast_value\": {\n"
                    + "                \"order\": \"desc\"\n"
                    + "            }\n"
                    + "        }\n"
                    + "    ]\n"
                    + "}",
                equal ? "gte" : "gt",
                forecastFrom,
                forecastFrom + intervalMillis
            );

        Response maxDistanceResponse = TestHelpers
            .makeRequest(client(), "GET", SEARCH_RESULTS, ImmutableMap.of(), TestHelpers.toHttpEntity(maxDistanceToThresholdRequest), null);
        List<SearchHit> maxDistanceHits = toHits(maxDistanceResponse);
        assertEquals("actual: " + maxDistanceHits, 1, maxDistanceHits.size());
        double maxValue = (double) (maxDistanceHits.get(0).getSourceAsMap().get("forecast_value"));
        assertEquals(String.format(Locale.ROOT, "actual: %f, expect: %f", maxValue, largestValue), maxValue, largestValue, 0.001);
    }

    private void distanceToThresholdGreaterThanEqual(String forecasterId, long forecastFrom, long intervalMillis) throws IOException {
        distanceToThresholdGreaterTemplate(forecasterId, forecastFrom, true, intervalMillis);
    }

    private void distanceToThresholdLessThan(String forecasterId, long forecastFrom, long intervalMillis) throws IOException {
        distanceToThresholdLessTemplate(forecasterId, forecastFrom, false, intervalMillis);
    }

    private void distanceToThresholdLessTemplate(String forecasterId, long forecastFrom, boolean equal, long intervalMillis)
        throws IOException {
        Response response;
        Map<String, Object> responseMap;
        String topForcastRequest;
        List<Object> parsedBuckets;
        double previousWidth;
        Set<String> cities;
        int threshold = 8000;
        topForcastRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"split_by\": \"%s\",\n"
                    + "    \"filter_by\": \"BUILD_IN_QUERY\",\n"
                    + "    \"build_in_query\": \"DISTANCE_TO_THRESHOLD_VALUE\",\n"
                    + "    \"forecast_from\": %d,\n"
                    + "    \"run_once\": true,\n"
                    + "    \"threshold\": %d,\n"
                    + "    \"relation_to_threshold\": \"%s\""
                    + "}",
                CITY_NAME,
                forecastFrom,
                threshold,
                equal ? "LESS_THAN_OR_EQUAL_TO" : "LESS_THAN"
            );

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TOP_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(topForcastRequest),
                null
            );
        responseMap = entityAsMap(response);
        parsedBuckets = (List<Object>) responseMap.get("buckets");
        assertTrue("actual content: " + parsedBuckets, parsedBuckets.size() == 2);

        previousWidth = Double.MIN_VALUE; // Initialize to negative infinity
        double smallestValue = Double.MAX_VALUE;
        cities = new HashSet<>();

        smallestValue = isAsc(parsedBuckets, cities, previousWidth, smallestValue, "DISTANCE_TO_THRESHOLD_VALUE");

        String maxDistanceToThresholdRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"size\": 1,\n"
                    + "    \"query\": {\n"
                    + "        \"bool\": {\n"
                    + "            \"filter\": [\n"
                    + "                {\n"
                    + "                    \"range\": {\n"
                    + "                        \"forecast_value\": {\n"
                    + "                            \"%s\": "
                    + threshold
                    + "\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                },\n"
                    + "                {\n"
                    + "                    \"range\": {\n"
                    + "                        \"data_end_time\": {\n"
                    + "                            \"from\": %d,\n"
                    + "                            \"to\": %d,\n"
                    + "                            \"include_lower\": true,\n"
                    + "                            \"include_upper\": false\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            ]\n"
                    + "        }\n"
                    + "    },\n"
                    + "    \"sort\": [\n"
                    + "        {\n"
                    + "            \"forecast_value\": {\n"
                    + "                \"order\": \"asc\"\n"
                    + "            }\n"
                    + "        }\n"
                    + "    ]\n"
                    + "}",
                equal ? "lte" : "lt",
                forecastFrom,
                forecastFrom + intervalMillis
            );

        Response maxDistanceResponse = TestHelpers
            .makeRequest(client(), "GET", SEARCH_RESULTS, ImmutableMap.of(), TestHelpers.toHttpEntity(maxDistanceToThresholdRequest), null);
        List<SearchHit> maxDistanceHits = toHits(maxDistanceResponse);
        assertEquals("actual: " + maxDistanceHits, 1, maxDistanceHits.size());
        double maxValue = (double) (maxDistanceHits.get(0).getSourceAsMap().get("forecast_value"));
        assertEquals(String.format(Locale.ROOT, "actual: %f, expect: %f", maxValue, smallestValue), maxValue, smallestValue, 0.001);
    }

    private void distanceToThresholdLessThanEqual(String forecasterId, long forecastFrom, long intervalMillis) throws IOException {
        distanceToThresholdLessTemplate(forecasterId, forecastFrom, true, intervalMillis);
    }

    private void customMaxForecastValue(String forecasterId, long forecastFrom, long intervalMillis) throws IOException {
        customForecastValueTemplate(forecasterId, forecastFrom, true, intervalMillis);
    }

    private void customMinForecastValue(String forecasterId, long forecastFrom, long intervalMillis) throws IOException {
        customForecastValueTemplate(forecasterId, forecastFrom, false, intervalMillis);
    }

    private void customForecastValueTemplate(String forecasterId, long forecastFrom, boolean max, long intervalMillis) throws IOException {
        Response response;
        Map<String, Object> responseMap;
        String topForcastRequest;
        List<Object> parsedBuckets;
        double previousValue;
        topForcastRequest = String
            .format(
                Locale.ROOT,
                "{\n"
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
                    + "                                \"entity.value\": \"S*\"\n"
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
                    + "                    \"%s\": {\n"
                    + "                        \"field\": \"forecast_value\"\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            },\n"
                    + "            \"order\": \"DESC\"\n"
                    + "        }\n"
                    + "    ],\n"
                    + "    \"run_once\": true\n"
                    + "}",
                forecastFrom,
                CITY_NAME,
                max ? "max" : "min"
            );

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TOP_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(topForcastRequest),
                null
            );
        responseMap = entityAsMap(response);
        parsedBuckets = (List<Object>) responseMap.get("buckets");
        assertTrue("actual content: " + parsedBuckets, parsedBuckets.size() == 1);

        previousValue = Double.MAX_VALUE; // Initialize to positive infinity
        double largestValue = Double.MIN_VALUE;

        largestValue = isDesc(parsedBuckets, previousValue, largestValue, "forecast_value_max");

        String maxValueRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"size\": 1,\n"
                    + "    \"sort\": [\n"
                    + "        {\n"
                    + "            \"%s\": {\n"
                    + "                \"order\": \"%s\"\n"
                    + "            }\n"
                    + "        }\n"
                    + "    ],\n"
                    + "    \"query\": {\n"
                    + "        \"bool\": {\n"
                    + "            \"must\": [\n"
                    + "                {\n"
                    + "                    \"nested\": {\n"
                    + "                        \"path\": \"entity\",\n"
                    + "                        \"query\": {\n"
                    + "                            \"bool\": {\n"
                    + "                                \"must\": [\n"
                    + "                                    {\n"
                    + "                                        \"term\": {\n"
                    + "                                            \"entity.name\": \"%s\"\n"
                    + "                                        }\n"
                    + "                                    },\n"
                    + "                                    {\n"
                    + "                                        \"wildcard\": {\n"
                    + "                                            \"entity.value\": \"S*\"\n"
                    + "                                        }\n"
                    + "                                    }\n"
                    + "                                ]\n"
                    + "                            }\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                },\n"
                    + "                {\n"
                    + "                    \"range\": {\n"
                    + "                        \"data_end_time\": {\n"
                    + "                            \"from\": %d,\n"
                    + "                            \"to\": %d,\n"
                    + "                            \"include_lower\": true,\n"
                    + "                            \"include_upper\": false\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            ]\n"
                    + "        }\n"
                    + "    }\n"
                    + "}",
                FORECAST_VALUE,          // First %s
                max ? "desc" : "asc",    // Second %s
                CITY_NAME,               // Third %s
                forecastFrom,            // %d
                forecastFrom + intervalMillis            // %d
            );

        Response maxValueResponse = TestHelpers
            .makeRequest(client(), "GET", SEARCH_RESULTS, ImmutableMap.of(), TestHelpers.toHttpEntity(maxValueRequest), null);
        List<SearchHit> maxValueHits = toHits(maxValueResponse);
        assertEquals("actual: " + maxValueHits, 1, maxValueHits.size());
        double maxValue = (double) (maxValueHits.get(0).getSourceAsMap().get(FORECAST_VALUE));
        assertEquals(String.format(Locale.ROOT, "actual: %f, expect: %f", maxValue, largestValue), maxValue, largestValue, 0.001);
    }

    public void testTopForecast() throws Exception {
        Instant trainTime = loadTwoCategoricalFieldData(200);
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
            + "   \"result_index\": \"opensearch-forecast-result-b\",\n"
            + "    \"category_field\": [%s]\n"
            + "}";

        // +1 to make sure it is big enough
        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes() + 1;
        final String formattedForecaster = String
            .format(Locale.ROOT, forecasterDef, RULE_DATASET_NAME, windowDelayMinutes, "\"account\",\"region\"");
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
        assertEquals("opensearch-forecast-result-b", ((Map<String, Object>) responseMap.get("forecaster")).get("result_index"));

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
            ImmutableSet.of(TaskState.TEST_COMPLETE.name()),
            client()
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

        List<SearchHit> hits = toHits(response);
        long forecastFrom = -1;
        for (SearchHit hit : hits) {
            Map<String, Object> source = hit.getSourceAsMap();
            if (source.get("forecast_value") != null) {
                forecastFrom = (long) (source.get("data_end_time"));
                break;
            }
        }
        assertTrue(forecastFrom != -1);

        // top forecast verification
        customForecastValueDoubleCategories(forecasterId, forecastFrom, true, taskId, 600000);
        customForecastValueDoubleCategories(forecasterId, forecastFrom, false, taskId, 600000);
    }

    private void topForecastSizeVerification(String forecasterId, long forecastFrom) throws IOException {
        Response response;
        Map<String, Object> responseMap;
        String topForcastRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"size\": 1,\n"
                    + "    \"split_by\": \"%s\",\n"
                    + "    \"filter_by\": \"BUILD_IN_QUERY\",\n"
                    + "    \"build_in_query\": \"MIN_CONFIDENCE_INTERVAL_WIDTH\",\n"
                    + "    \"forecast_from\": %d,\n"
                    + "    \"run_once\": true\n"
                    + "}",
                CITY_NAME,
                forecastFrom
            );

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TOP_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(topForcastRequest),
                null
            );
        responseMap = entityAsMap(response);
        List<Object> parsedBuckets = (List<Object>) responseMap.get("buckets");
        assertTrue("actual content: " + parsedBuckets, parsedBuckets.size() == 1);
    }

    private void customForecastValueDoubleCategories(
        String forecasterId,
        long forecastFrom,
        boolean max,
        String taskId,
        long intervalMillis
    ) throws IOException {
        Response response;
        Map<String, Object> responseMap;
        String topForcastRequest;
        List<Object> parsedBuckets;
        double previousValue;
        topForcastRequest = String
            .format(
                Locale.ROOT,
                "{\n"
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
                    + "                                \"entity.value\": \"i*\"\n"
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
                    + "                    \"%s\": {\n"
                    + "                        \"field\": \"forecast_value\"\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            },\n"
                    + "            \"order\": \"DESC\"\n"
                    + "        }\n"
                    + "    ],\n"
                    + "    \"run_once\": true,\n"
                    + "    \"task_id\": \"%s\"\n"
                    + "}",
                forecastFrom,
                "region",
                max ? "max" : "min",
                taskId
            );

        response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, TOP_FORECASTER, forecasterId),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(topForcastRequest),
                null
            );
        responseMap = entityAsMap(response);
        parsedBuckets = (List<Object>) responseMap.get("buckets");
        assertTrue("actual content: " + parsedBuckets, parsedBuckets.size() == 1);

        previousValue = Double.MAX_VALUE; // Initialize to positive infinity
        double largestValue = Double.MIN_VALUE;

        largestValue = isDescTwoCategorical(parsedBuckets, previousValue, largestValue, "forecast_value_max");

        String maxValueRequest = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"size\": 1,\n"
                    + "    \"sort\": [\n"
                    + "        {\n"
                    + "            \"%s\": {\n"
                    + "                \"order\": \"%s\"\n"
                    + "            }\n"
                    + "        }\n"
                    + "    ],\n"
                    + "    \"query\": {\n"
                    + "        \"bool\": {\n"
                    + "            \"must\": [\n"
                    + "                {\n"
                    + "                    \"nested\": {\n"
                    + "                        \"path\": \"entity\",\n"
                    + "                        \"query\": {\n"
                    + "                            \"bool\": {\n"
                    + "                                \"must\": [\n"
                    + "                                    {\n"
                    + "                                        \"term\": {\n"
                    + "                                            \"entity.name\": \"%s\"\n"
                    + "                                        }\n"
                    + "                                    },\n"
                    + "                                    {\n"
                    + "                                        \"wildcard\": {\n"
                    + "                                            \"entity.value\": \"i*\"\n"
                    + "                                        }\n"
                    + "                                    }\n"
                    + "                                ]\n"
                    + "                            }\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                },\n"
                    + "                {\n"
                    + "                    \"range\": {\n"
                    + "                        \"data_end_time\": {\n"
                    + "                            \"from\": %d,\n"
                    + "                            \"to\": %d,\n"
                    + "                            \"include_lower\": true,\n"
                    + "                            \"include_upper\": false\n"
                    + "                        }\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            ]\n"
                    + "        }\n"
                    + "    }\n"
                    + "}",
                FORECAST_VALUE,          // First %s
                max ? "desc" : "asc",    // Second %s
                "region",               // Third %s
                forecastFrom,            // %d
                forecastFrom + intervalMillis // %d
            );

        Response maxValueResponse = TestHelpers
            .makeRequest(client(), "GET", SEARCH_RESULTS, ImmutableMap.of(), TestHelpers.toHttpEntity(maxValueRequest), null);
        List<SearchHit> maxValueHits = toHits(maxValueResponse);
        assertEquals("actual: " + maxValueHits, 1, maxValueHits.size());
        double maxValue = (double) (maxValueHits.get(0).getSourceAsMap().get(FORECAST_VALUE));
        assertEquals(String.format(Locale.ROOT, "actual: %f, expect: %f", maxValue, largestValue), maxValue, largestValue, 0.001);
    }
}
