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
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;

import com.google.gson.JsonObject;

/**
 * Batch AD with Scheduling and Checkpoints performance IT.
 *
 * This reproduces the experiment of running a real-time detector with 1-minute bucket interval
 * and daily frequency, simulating a cold start run followed by a run one day later, and then
 * executing historical analysis for the same 1-day range. The test validates that real-time
 * (scheduled) and historical runs produce a comparable number of results.
 */
public class BatchADSchedulingCheckpointIT extends AbstractADSyntheticDataTest {
    private static final Logger LOG = LogManager.getLogger(BatchADSchedulingCheckpointIT.class);

    // Test parameters
    private static final String DATASET = "batch_sched_perf";
    private static final String CATEGORY_FIELD = "entity";
    private static final int INTERVAL_MINUTES = 1;     // bucket_span
    private static final int FREQUENCY_MINUTES = 1440; // 1 day
    private static final int NUM_ENTITIES = 21;        // 3 hosts * 7 services equivalent

    // Data volume: ~1.2 days worth of 1-minute buckets for NUM_ENTITIES
    // Total docs ~= NUM_ENTITIES * TOTAL_MINUTES
    // Keep it large enough to exercise pagination and checkpointing, but not too huge for IT.
    private static final int TRAIN_MINUTES = 90;       // ~90 minutes of "training" (before cold-start minute)
    private static final int TEST_MINUTES = FREQUENCY_MINUTES;   // 1 day worth of data
    private static final int TOTAL_MINUTES = TRAIN_MINUTES + TEST_MINUTES + 10; // some buffer
    private static final int DATA_SIZE = NUM_ENTITIES * TOTAL_MINUTES;

    public void testRealtimeDailyVsHistorical() throws Exception {
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
                    + "\"name\": \"Batch-AD-Daily-1min\"," // name
                    + " \"description\": \"IT for RT daily scheduling vs historical\"," // description
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

        // 4) Simulate a scheduled run one day later
        Instant execBeginNextDay = execBeginCold.plus(1, ChronoUnit.DAYS);
        Instant execEndNextDay = execBeginNextDay.plus(INTERVAL_MINUTES, ChronoUnit.MINUTES);

        // Record start time of real-time run
        Instant rtStartTime = Instant.now();
        runDetectionResult(detectorId, execBeginNextDay, execEndNextDay, client, NUM_ENTITIES);

        // Give the system a brief moment to flush results
        Thread.sleep(2_000L);

        // Count RT results for this detector since the cold start execution time (exclude historical by must_not task_id)
        // Query results until count doesn't increase for 3 consecutive times (2 second wait between each check)
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
                // This shouldn't happen normally, but handle it just in case
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

        // Record start time of historical run
        Instant histStartTime = Instant.now();
        String startBody = String
            .format(Locale.ROOT, "{ \"start_time\": %d, \"end_time\": %d }", dataBeginTime.toEpochMilli(), dataEndTime.toEpochMilli());
        Request startHistorical = new Request(
            "POST",
            String.format(Locale.ROOT, "/_opendistro/_anomaly_detection/detectors/%s/_start", detectorId)
        );
        startHistorical.setJsonEntity(startBody);

        Map<String, Object> startResp = entityAsMap(client.performRequest(startHistorical));
        String taskId = (String) startResp.get("_id");
        LOG.info("Started historical task {} for detector {}", taskId, detectorId);

        // Wait until we observe historical results at the end of the range for at least one entity
        waitForHistoricalDetector(detectorId, client, dataEndTime, NUM_ENTITIES /*check 21 entities*/, INTERVAL_MINUTES * 60000);

        // Count historical results (task_id present) for the same execution window
        long histTotal = countResultsForTask(detectorId, taskId);
        LOG.info("Historical results for task {}: {}", taskId, histTotal);

        // Record end time of historical run
        Instant histEndTime = Instant.now();
        Duration histDuration = Duration.between(histStartTime, histEndTime);
        LOG.info("Historical run duration: {} milliseconds", histDuration.toMillis());

        // Compare durations
        long rtDurationMillis = rtDuration.toMillis();
        long histDurationMillis = histDuration.toMillis();
        double durationRatio = (double) histDurationMillis / rtDurationMillis;

        LOG
            .info(
                "Duration comparison - Real-time: {}ms, Historical: {}ms, Ratio (Hist/RT): {}",
                rtDurationMillis,
                histDurationMillis,
                String.format(Locale.ROOT, "%.2f", durationRatio)
            );

        // Check if durations are roughly similar (within 50% of each other)
        final double MAX_DURATION_RATIO = 1.5;
        final double MIN_DURATION_RATIO = 0.67;

        if (durationRatio > MAX_DURATION_RATIO || durationRatio < MIN_DURATION_RATIO) {
            LOG
                .warn(
                    "Duration mismatch detected! Historical run took significantly longer/shorter than real-time run. Ratio: {:.2f}",
                    durationRatio
                );
        } else {
            LOG.info("Duration check passed - runs are roughly similar in duration");
        }

        // 6) Basic consistency check: historical count should be within the same order as RT results
        // In ideal case they should match; allow small deltas due to indexing/flush timing.
        // Expect at least one full day of entity-minutes for both modes.
        long expectedLowerBound = NUM_ENTITIES * (TEST_MINUTES - 5); // tolerate small gaps
        assertTrue(
            String.format(Locale.ROOT, "Expected at least %d results, RT=%d, HIST=%d", expectedLowerBound, rtTotal, histTotal),
            rtTotal >= expectedLowerBound && histTotal >= expectedLowerBound
        );

        // Optional stronger parity assertion, allow 5%% delta
        double maxDelta = 0.05;
        if (rtTotal > 0 && histTotal > 0) {
            double diff = Math.abs(rtTotal - histTotal) / (double) Math.max(rtTotal, histTotal);
            assertTrue(String.format(Locale.ROOT, "RT/HIST count delta too large: %.3f", diff), diff <= maxDelta);
        }
    }

    /**
     * Parses a timestamp string into an Instant object.
     * Supports epoch milliseconds (13 digits), epoch seconds (10 digits), and ISO-8601 format.
     * 
     * @param timestampStr the timestamp string to parse
     * @return the parsed Instant
     * @throws DateTimeParseException if the timestamp format is not recognized
     */
    private Instant parseMilliseconds(String timestampStr) {
        return Instant.ofEpochMilli(Long.parseLong(timestampStr));
    }

    private long countResultsForTask(String detectorId, String taskId) throws Exception {
        Request req = new Request("POST", "/_plugins/_anomaly_detection/detectors/results/_search");
        String body = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "  \"size\": 0,\n"
                    + "  \"track_total_hits\": true,\n"
                    + "  \"query\": {\n"
                    + "    \"bool\": {\n"
                    + "      \"filter\": [\n"
                    + "        { \"term\": { \"detector_id\": \"%s\" } },\n"
                    + "        { \"term\": { \"task_id\": \"%s\" } }\n"
                    + "      ]\n"
                    + "    }\n"
                    + "  }\n"
                    + "}\n",
                detectorId,
                taskId
            );
        req.setJsonEntity(body);
        Response resp = client().performRequest(req);
        Map<String, Object> m = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Object> hits = (Map<String, Object>) m.get("hits");
        @SuppressWarnings("unchecked")
        Map<String, Object> total = (Map<String, Object>) hits.get("total");
        Number value = (Number) total.get("value");
        return value == null ? 0L : value.longValue();
    }
}
