/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;

/**
 * Simple Real-Time Batch Stats IT.
 *
 * This test runs a basic real-time detector with 2 entities and 3-minute frequency,
 * validates that the number of results is in the ballpark of expected values,
 * and checks the stats API for execution request count.
 * Uses startDetector instead of simulateStartDetector.
 */
public class RealTimeFrequencySmokeIT extends AbstractADSyntheticDataTest {
    private static final Logger LOG = (Logger) LogManager.getLogger(RealTimeFrequencySmokeIT.class);

    // Test parameters
    private static final String DATASET = "simple_rt_batch_stats";
    private static final String CATEGORY_FIELD = "entity";
    private static final int INTERVAL_MINUTES = 1;     // bucket_span
    private static final int FREQUENCY_MINUTES = 3;    // 3 minutes frequency
    private static final int NUM_ENTITIES = 2;         // Only 2 entities for faster test

    // Data volume: enough for initialization and some test data
    private static final int TRAIN_MINUTES = 45;       // Training period
    private static final int TEST_MINUTES = FREQUENCY_MINUTES;        // Test period
    private static final int TOTAL_MINUTES = TRAIN_MINUTES + TEST_MINUTES + 5; // some buffer
    private static final int DATA_SIZE = NUM_ENTITIES * TOTAL_MINUTES;

    public void testSimpleRealTimeBatchWithStats() throws Exception {
        RestClient client = client();

        // 1) Generate and ingest synthetic multi-entity data at 1-minute interval.
        List<JsonObject> data = genUniformSingleFeatureData(
            INTERVAL_MINUTES,
            NUM_ENTITIES * TRAIN_MINUTES,
            NUM_ENTITIES,
            CATEGORY_FIELD,
            MISSING_MODE.NO_MISSING_DATA,
            0,
            0,
            DATA_SIZE
        ).data;

        ingestUniformSingleFeatureData(-1, data, DATASET, CATEGORY_FIELD);

        // Extract the first and last data item's timestamps as begin and end variables
        String firstTimestampStr = data.get(0).get("timestamp").getAsString();
        String lastTimestampStr = data.get(data.size() - 1).get("timestamp").getAsString();

        Instant dataBeginTime = parseMilliseconds(firstTimestampStr);
        Instant dataEndTime = parseMilliseconds(lastTimestampStr);

        LOG.info("Data time range: begin={}, end={}", dataBeginTime, dataEndTime);

        // Choose the last training point's data-time and compute window delay to align to now().
        long windowDelayMinutes = getWindowDelayMinutes(data, NUM_ENTITIES * TRAIN_MINUTES - 1, "timestamp");

        // 2) Create an HC detector with 3-minute frequency
        String detectorJson = String
            .format(
                Locale.ROOT,
                "{ "
                    + "\"name\": \"Simple-RT-Batch-Stats-1min\","
                    + " \"description\": \"Simple IT for RT batch stats validation\","
                    + " \"time_field\": \"timestamp\","
                    + " \"indices\": [\"%s\"],"
                    + " \"category_field\": [\"%s\"],"
                    + " \"feature_attributes\": ["
                    + "   { \"feature_name\": \"sum_data\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"data\": { \"sum\": { \"field\": \"data\" } } } }"
                    + " ],"
                    + " \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"MINUTES\" } },"
                    + " \"window_delay\": { \"period\": { \"interval\": %d, \"unit\": \"MINUTES\" } },"
                    + " \"frequency\": { \"period\": { \"interval\": %d, \"unit\": \"MINUTES\" } },"
                    + " \"schema_version\": 2"
                    + " }",
                DATASET,
                CATEGORY_FIELD,
                INTERVAL_MINUTES,
                windowDelayMinutes,
                FREQUENCY_MINUTES
            );

        String detectorId = createDetector(client, detectorJson);
        LOG.info("Created detector {}", detectorId);

        // 3) Start detector (not simulate)
        startDetector(detectorId, client);
        LOG.info("Started detector {}", detectorId);

        // 4) Wait for 4 minutes to allow detector to run multiple intervals
        LOG.info("Waiting for 4 minutes to allow detector execution...");
        Thread.sleep(4 * 60 * 1000L); // 4 minutes in milliseconds

        // 5) Count RT results for this detector
        long rtExecutionLowerBound = dataBeginTime.toEpochMilli();
        long rtTotal = countResults(detectorId, rtExecutionLowerBound, false /*historical*/);
        LOG.info("Real-time results count: {}", rtTotal);

        // 6) Validate that results are in the ballpark of expected values
        // Expected: 2 entities * (40 default history + 3 frequency duration)
        int expectedResults = NUM_ENTITIES * (TimeSeriesSettings.NUM_MIN_SAMPLES + FREQUENCY_MINUTES);
        int minExpected = (int) (expectedResults * 0.5);  // Allow 50% variance down
        int maxExpected = (int) (expectedResults * 2.0);  // Allow 100% variance up (more lenient for timing variations)

        LOG.info("Expected results range: {} - {} (target: {})", minExpected, maxExpected, expectedResults);

        if (rtTotal < minExpected || rtTotal > maxExpected) {
            LOG.error("Results count {} is outside expected range [{}, {}]", rtTotal, minExpected, maxExpected);
            fail("Real-time results count " + rtTotal + " is not in ballpark of expected " + expectedResults);
        } else {
            LOG.info("Results validation passed: {} results (expected range: {}-{})", rtTotal, minExpected, maxExpected);
        }

        // 7) Call stats API and check ad_execute_request_count
        LOG.info("Checking stats API for execution request count...");
        Response statsResponse = TestHelpers
            .makeRequest(
                client,
                "GET",
                TimeSeriesAnalyticsPlugin.LEGACY_AD_BASE + "/_local/stats/ad_execute_request_count",
                ImmutableMap.of(),
                "",
                null
            );

        assertEquals("Get stats failed", RestStatus.OK, TestHelpers.restStatus(statsResponse));

        Map<String, Object> statsMap = entityAsMap(statsResponse);
        LOG.info("Stats response: {}", statsMap);

        // Parse the ad_execute_request_count from the nested structure
        Map<String, Object> nodes = (Map<String, Object>) statsMap.get("nodes");
        if (nodes != null && !nodes.isEmpty()) {
            // Get the first node's stats
            Map.Entry<String, Object> firstNode = nodes.entrySet().iterator().next();
            @SuppressWarnings("unchecked")
            Map<String, Object> nodeStats = (Map<String, Object>) firstNode.getValue();

            Number adExecuteRequestCount = (Number) nodeStats.get("ad_execute_request_count");
            if (adExecuteRequestCount != null) {
                int executeCount = adExecuteRequestCount.intValue();
                LOG.info("AD execute request count: {}", executeCount);

                // Check that execute count is 2 (twice in 4 minutes)
                if (executeCount != 2) {
                    LOG.error("AD execute request count {} is not 2", executeCount);
                    fail("Expected ad_execute_request_count to be 2, but got " + executeCount);
                } else {
                    LOG.info("Stats validation passed: ad_execute_request_count is {}", executeCount);
                }
            } else {
                LOG.error("ad_execute_request_count not found in node stats");
                fail("ad_execute_request_count not found in stats response");
            }
        } else {
            LOG.error("No nodes found in stats response");
            fail("No nodes found in stats response");
        }
    }

    /**
     * Parses a timestamp string into an Instant object.
     * Supports epoch milliseconds format.
     * 
     * @param timestampStr the timestamp string to parse
     * @return the parsed Instant
     * @throws DateTimeParseException if the timestamp format is not recognized
     */
    private Instant parseMilliseconds(String timestampStr) {
        return Instant.ofEpochMilli(Long.parseLong(timestampStr));
    }
}
