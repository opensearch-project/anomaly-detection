/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest;

import static org.opensearch.timeseries.util.RestHandlerUtils.SUGGEST;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.forecast.AbstractForecastSyntheticDataTest;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Config;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;

/**
 * Test the following Restful API:
 *  - Suggest
 *
 */
public class ForecastRestApiIT extends AbstractForecastSyntheticDataTest {
    private static final String SYNTHETIC_DATASET_NAME = "synthetic";
    private static final String RULE_DATASET_NAME = "rule";
    private static final String SUGGEST_INTERVAL_URI;
    private static final String SUGGEST_INTERVAL_HORIZON_HISTORY_URI;

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
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        updateClusterSettings(ForecastEnabledSetting.FORECAST_ENABLED, true);
    }

    private static void loadData(String datasetName, int trainTestSplit) throws Exception {
        RestClient client = client();

        String dataFileName = String.format(Locale.ROOT, "org/opensearch/ad/e2e/data/%s.data", datasetName);

        List<JsonObject> data = readJsonArrayWithLimit(dataFileName, trainTestSplit);

        String mapping = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
            + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" } } } }";
        bulkIndexTrainData(datasetName, data, trainTestSplit, client, mapping);
    }

    /**
     * Test suggest API. I wrote multiple cases together to save time in loading data. Cannot load data once for
     * all tests as client is set up in the instance method of OpenSearchRestTestCase and we need client to
     * ingest data. Also, the JUnit 5 feature @BeforeAll is not supported in OpenSearchRestTestCase and the code
     * inside @BeforeAll won't be executed even if I upgrade junit from 4 to 5 in build.gradle.
     * @throws Exception when loading data
     */
    public void testSuggestOneMinute() throws Exception {
        loadData(SYNTHETIC_DATASET_NAME, 200);
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
    }

    public void testSuggestTenMinute() throws Exception {
        loadData(RULE_DATASET_NAME, 200);
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
        loadData(SYNTHETIC_DATASET_NAME, 10);
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
}
