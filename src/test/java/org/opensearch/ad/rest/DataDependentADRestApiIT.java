/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest;

import static org.opensearch.timeseries.util.RestHandlerUtils.VALIDATE;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.Map;

import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;

import com.google.common.collect.ImmutableMap;

/**
 * Different from AnomalyDetectorRestApiIT, this IT focuses on tests requiring delicates setup of data like validate api.
 *
 */
public class DataDependentADRestApiIT extends AbstractADSyntheticDataTest {
    private static final String VALIDATE_DETECTOR_MODEL;

    static {
        VALIDATE_DETECTOR_MODEL = String
            .format(Locale.ROOT, "%s/%s/%s", TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI, VALIDATE, "model");
    }

    public void testTwoFeatureSparse() throws Exception {

        Instant trainTime = loadRuleData(200);

        // case 1: both filters in features cause sparsity
        String detectorDef = "{\n"
            + "    \"name\": \"Second-Test-Detector-4\",\n"
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
            + "    \"detection_interval\": {\n"
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
        long featureFilter = trainTime.minus(700, ChronoUnit.MINUTES).toEpochMilli();
        String formattedDetector = String
            .format(Locale.ROOT, detectorDef, RULE_DATASET_NAME, featureFilter, featureFilter, windowDelayMinutes);
        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                String.format(Locale.ROOT, VALIDATE_DETECTOR_MODEL),
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedDetector),
                null
            );
        assertEquals("Validate forecaster interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        Map<String, Object> validation = (Map<String, Object>) ((Map<String, Object>) responseMap.get("model")).get("feature_attributes");
        String msg = (String) validation.get("message");
        // due to concurrency, feature order can change.
        assertTrue(
            "Data is most likely too sparse when given feature queries are applied. Consider revising feature queries: max1, Data is most likely too sparse when given feature queries are applied. Consider revising feature queries: max2"
                .equals(msg)
                || "Data is most likely too sparse when given feature queries are applied. Consider revising feature queries: max2, Data is most likely too sparse when given feature queries are applied. Consider revising feature queries: max1"
                    .equals(msg)
        );
        Map<String, Object> subIssues = (Map<String, Object>) validation.get("sub_issues");
        assertEquals(
            "Data is most likely too sparse when given feature queries are applied. Consider revising feature queries",
            subIssues.get("max1")
        );
        assertEquals(
            "Data is most likely too sparse when given feature queries are applied. Consider revising feature queries",
            subIssues.get("max2")
        );
    }

    public void testSuggestInterval() throws Exception {
        loadSyntheticData(200);

        String detectorDef = "{\n"
            + "    \"name\": \"test-detector\",\n"
            + "    \"description\": \"Test detector\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_name\": \"feature1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"feature1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ]\n"
            + "}";

        String formattedDetector = String.format(Locale.ROOT, detectorDef, SYNTHETIC_DATASET_NAME);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_suggest/detection_interval",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedDetector),
                null
            );
        assertEquals("Suggest detector interval failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);
        assertTrue("actual: " + responseMap, responseMap.get("interval") != null);
        Map<String, Object> suggestions = (Map<String, Object>) ((Map<String, Object>) responseMap.get("interval")).get("period");
        assertEquals(1, (int) suggestions.get("interval"));
        assertEquals("Minutes", suggestions.get("unit"));
    }

    public void testSuggestMultipleParams() throws Exception {
        loadSyntheticData(200);

        String detectorDef = "{\n"
            + "    \"name\": \"test-detector\",\n"
            + "    \"description\": \"Test detector\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_name\": \"feature1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"feature1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ]\n"
            + "}";

        String formattedDetector = String.format(Locale.ROOT, detectorDef, SYNTHETIC_DATASET_NAME);

        Response response = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_suggest/detection_interval,history,window_delay",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(formattedDetector),
                null
            );
        assertEquals("Suggest detector params failed", RestStatus.OK, TestHelpers.restStatus(response));
        Map<String, Object> responseMap = entityAsMap(response);

        // Check interval suggestion
        Map<String, Object> intervalSuggestions = (Map<String, Object>) ((Map<String, Object>) responseMap.get("interval")).get("period");
        assertEquals(1, (int) intervalSuggestions.get("interval"));
        assertEquals("Minutes", intervalSuggestions.get("unit"));

        // Check history suggestion
        int historySuggestions = ((Integer) responseMap.get("history"));
        assertTrue("History should be positive", historySuggestions > 0);

        // Check window delay suggestion
        Map<String, Object> windowDelaySuggestions = (Map<String, Object>) ((Map<String, Object>) responseMap.get("windowDelay"))
            .get("period");
        assertTrue("Window delay should be non-negative", (int) windowDelaySuggestions.get("interval") >= 0);
        assertEquals("Minutes", windowDelaySuggestions.get("unit"));
    }

    public void testSuggestWithInvalidType() throws Exception {
        loadSyntheticData(200);

        String detectorDef = "{\n"
            + "    \"name\": \"test-detector\",\n"
            + "    \"description\": \"Test detector\",\n"
            + "    \"time_field\": \"timestamp\",\n"
            + "    \"indices\": [\n"
            + "        \"%s\"\n"
            + "    ],\n"
            + "    \"feature_attributes\": [\n"
            + "        {\n"
            + "            \"feature_name\": \"feature1\",\n"
            + "            \"feature_enabled\": true,\n"
            + "            \"aggregation_query\": {\n"
            + "                \"feature1\": {\n"
            + "                    \"sum\": {\n"
            + "                        \"field\": \"Feature1\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        }\n"
            + "    ]\n"
            + "}";

        String formattedDetector = String.format(Locale.ROOT, detectorDef, SYNTHETIC_DATASET_NAME);

        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    TestHelpers.AD_BASE_DETECTORS_URI + "/_suggest/invalid_type",
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(formattedDetector),
                    null
                )
        );
        assertTrue(
            "Expect contains suggest type doesn't exist message; but got" + exception.getMessage(),
            exception.getMessage().contains("The given suggest type doesn't exist")
        );
    }
}
