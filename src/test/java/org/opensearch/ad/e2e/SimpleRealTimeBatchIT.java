/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.client.RestClient;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.gson.JsonObject;

/**
 * Simple Real-Time Batch AD IT.
 *
 * This test runs a basic real-time detector with only 2 entities and validates that
 * the number of results is in the ballpark of the expected DATA_SIZE.
 * No historical analysis is performed - just real-time detection validation.
 */
public class SimpleRealTimeBatchIT extends AbstractADSyntheticDataTest {
    private static final Logger LOG = (Logger) LogManager.getLogger(SimpleRealTimeBatchIT.class);

    // Test parameters - simplified version
    private static final String DATASET = "simple_rt_batch";
    private static final String CATEGORY_FIELD = "entity";
    private static final int INTERVAL_MINUTES = 1;     // bucket_span
    private static final int FREQUENCY_MINUTES = 30; // 30 minutes
    private static final int NUM_ENTITIES = 2;         // Only 2 entities for faster test

    // Data volume: ~1 hour worth of 1-minute buckets for NUM_ENTITIES
    // Keep it small for quick validation
    private static final int TRAIN_MINUTES = 90;       // ~90 minutes of "training"
    private static final int TEST_MINUTES = FREQUENCY_MINUTES;        // 30 minutes worth of test data
    private static final int TOTAL_MINUTES = TRAIN_MINUTES + TEST_MINUTES + 5; // some buffer
    private static final int DATA_SIZE = NUM_ENTITIES * TOTAL_MINUTES;

    public void testSimpleRealTimeBatch() throws Exception {
        RestClient client = client();

        // 1) Generate and ingest synthetic multi-entity data at 1-minute interval.
        // Use uniform single-feature generator: fields {timestamp (epoch millis), data (double), entity (keyword)}
        List<JsonObject> data = genUniformSingleFeatureData(
            INTERVAL_MINUTES,
            NUM_ENTITIES * TRAIN_MINUTES, // trainTestSplit controls how far back from now data starts
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

        // 2) Create an HC detector (category_field: entity) with 1-minute interval and 1-day frequency.
        String detectorJson = String
            .format(
                Locale.ROOT,
                "{ "
                    + "\"name\": \"Simple-RT-Batch-1min\"," // name
                    + " \"description\": \"Simple IT for RT batch validation\"," // description
                    + " \"time_field\": \"timestamp\"," // time field
                    + " \"indices\": [\"%s\"]," // index
                    + " \"category_field\": [\"%s\"]," // HC
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

        // 3) Simulate cold start: run for one bucket immediately after the last train minute
        Instant trainDataTime = getDataTimeOfEpochMillis("timestamp", data, NUM_ENTITIES * TRAIN_MINUTES - 1);
        Duration windowDelay = Duration.ofMinutes(windowDelayMinutes);
        Instant execBeginCold = dataToExecutionTime(trainDataTime, windowDelay); // shift data-time → execution-time
        Instant execEndCold = execBeginCold.plus(INTERVAL_MINUTES, ChronoUnit.MINUTES);

        simulateStartDetector(detectorId, execBeginCold, execEndCold, client, NUM_ENTITIES);
        List<JsonObject> initResults = simulateWaitForInitDetector(
            detectorId,
            client,
            trainDataTime.plus(INTERVAL_MINUTES, ChronoUnit.MINUTES),
            NUM_ENTITIES
        );
        LOG.info("Cold start produced {} results (expected {} entities)", initResults.size(), NUM_ENTITIES);

        // 4) Simulate a scheduled run a few minutes later
        Instant execBeginNext = execBeginCold.plus(FREQUENCY_MINUTES, ChronoUnit.MINUTES);
        Instant execEndNext = execBeginNext.plus(INTERVAL_MINUTES, ChronoUnit.MINUTES);

        // Record start time of real-time run
        Instant rtStartTime = Instant.now();
        runDetectionResult(detectorId, execBeginNext, execEndNext, client, NUM_ENTITIES);

        // Give the system a brief moment to flush results
        Thread.sleep(2_000L);

        // Count RT results for this detector since the cold start execution time
        long rtExecutionLowerBound = dataBeginTime.toEpochMilli();
        long rtTotal = countResults(detectorId, rtExecutionLowerBound, false /*historical*/);
        LOG.info("Initial real-time results count: {}", rtTotal);

        int consecutiveNoIncrease = 0;
        long previousCount = rtTotal;
        final int MAX_CONSECUTIVE_CHECKS = 3;
        final int WAIT_INTERVAL_MS = 2_000;

        while (consecutiveNoIncrease < MAX_CONSECUTIVE_CHECKS) {
            Thread.sleep(WAIT_INTERVAL_MS);
            long currentCount = countResults(detectorId, rtExecutionLowerBound, false /*historical*/);

            if (currentCount > previousCount) {
                LOG.info("Real-time results increased from {} to {}", previousCount, currentCount);
                consecutiveNoIncrease = 0;
                previousCount = currentCount;
            } else if (currentCount == previousCount) {
                consecutiveNoIncrease++;
                LOG.info("Real-time results stable at {} (consecutive no-increase: {})", currentCount, consecutiveNoIncrease);
            } else {
                LOG.warn("Real-time results decreased from {} to {} - this is unexpected", previousCount, currentCount);
                consecutiveNoIncrease = 0;
                previousCount = currentCount;
            }
        }

        rtTotal = previousCount;
        // Record end time of real-time run
        Instant rtEndTime = Instant.now();
        Duration rtDuration = Duration.between(rtStartTime, rtEndTime);
        LOG.info("Final real-time results count after stabilization: {}", rtTotal);
        LOG.info("Real-time run duration: {} milliseconds", rtDuration.toMillis());

        // 5) Validate that results are in the ballpark of DATA_SIZE
        // For real-time detection, we expect results roughly proportional to the test data period
        // Since we have TEST_MINUTES minutes of test data and NUM_ENTITIES entities,
        // we expect roughly NUM_ENTITIES * (TEST_MINUTES + 40) results where 40 is the default history size
        int expectedResults = NUM_ENTITIES * (TEST_MINUTES + TimeSeriesSettings.NUM_MIN_SAMPLES + TimeSeriesSettings.DEFAULT_SHINGLE_SIZE);
        int minExpected = (int) (expectedResults * 0.05);  // Allow 5% variance down
        int maxExpected = (int) (expectedResults * 1.05);  // Allow 5% variance up

        LOG.info("Expected results range: {} - {} (target: {})", minExpected, maxExpected, expectedResults);

        if (rtTotal < minExpected || rtTotal > maxExpected) {
            LOG.error("Results count {} is outside expected range [{}, {}]", rtTotal, minExpected, maxExpected);
            fail("Real-time results count " + rtTotal + " is not in ballpark of expected " + expectedResults);
        } else {
            LOG.info("Results validation passed: {} results (expected range: {}-{})", rtTotal, minExpected, maxExpected);
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
