/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.ad.rest.ADRestTestUtils;
import org.opensearch.timeseries.model.TaskState;

import com.google.gson.JsonObject;

/**
 * Test that is meant to run with job scheduler to test if we have at least consecutive results generated.
 *
 */
public class SingleStreamSmokeIT extends AbstractADSyntheticDataTest {

    public void testGenerateResult() throws Exception {
        String datasetName = "synthetic";
        String dataFileName = String.format(Locale.ROOT, "data/%s.data", datasetName);
        int intervalsToWait = 3;

        List<JsonObject> data = getData(dataFileName);

        String mapping = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
            + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" } } } }";
        int trainTestSplit = 1500;
        // train data plus a few data points for real time inference
        bulkIndexTrainData(datasetName, data, trainTestSplit + intervalsToWait + 3, client(), mapping);

        long windowDelayMinutes = getWindowDelayMinutes(data, trainTestSplit - 1, "timestamp");
        int intervalMinutes = 1;

        // single-stream detector can use window delay 0 here because we give the run api the actual data time
        String detector = String
            .format(
                Locale.ROOT,
                "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"Feature1\" } } } }, { \"feature_name\""
                    + ": \"feature 2\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"Feature2\": { \"sum\": { \"field\": "
                    + "\"Feature2\" } } } }], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                    + "\"window_delay\": { \"period\": {\"interval\": %d, \"unit\": \"MINUTES\"}},"
                    + "\"schema_version\": 0 }",
                datasetName,
                intervalMinutes,
                windowDelayMinutes
            );
        String detectorId = createDetector(client(), detector);

        startDetector(detectorId, client());

        long waitMinutes = intervalMinutes * (intervalsToWait + 1);
        // wait for scheduler to trigger AD
        Thread.sleep(Duration.ofMinutes(waitMinutes));

        List<JsonObject> results = getAnomalyResultByExecutionTime(
            detectorId,
            Instant.now(),
            1,
            client(),
            true,
            waitMinutes * 60000,
            intervalsToWait
        );

        assertTrue(
            String.format(Locale.ROOT, "Expect at least %d but got %d", intervalsToWait, results.size()),
            results.size() >= intervalsToWait
        );
    }

    /**
     * Test that is meant to check if quick start/stop would set task state to STOPPED. We have a delayed thread in the startDetector method
     * to update task state to RUNNING. Before updating task state to running, TaskManager.updateLatestRealtimeTask should set task state to
     * STOPPED if job is disabled.
     *
     */
    public void testStartStopDetector() throws Exception {
        String datasetName = "synthetic";
        String dataFileName = String.format(Locale.ROOT, "data/%s.data", datasetName);

        List<JsonObject> data = getData(dataFileName);

        String mapping = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
            + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" } } } }";
        int trainTestSplit = 1500;
        // train data plus a few data points for real time inference
        bulkIndexTrainData(datasetName, data, trainTestSplit + 5, client(), mapping);

        long windowDelayMinutes = getWindowDelayMinutes(data, trainTestSplit - 1, "timestamp");
        int intervalMinutes = 1;

        // single-stream detector can use window delay 0 here because we give the run api the actual data time
        String detector = String
            .format(
                Locale.ROOT,
                "{ \"name\": \"test-stop\", \"description\": \"test-stop\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"Feature1\" } } } }, { \"feature_name\""
                    + ": \"feature 2\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"Feature2\": { \"sum\": { \"field\": "
                    + "\"Feature2\" } } } }], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                    + "\"window_delay\": { \"period\": {\"interval\": %d, \"unit\": \"MINUTES\"}},"
                    + "\"schema_version\": 0 }",
                datasetName,
                intervalMinutes,
                windowDelayMinutes
            );
        String detectorId = createDetector(client(), detector);

        // Step 1: Start the detector
        startDetector(detectorId, client());

        // Step 2: Stop the detector
        ADRestTestUtils.stopRealtimeJob(client(), detectorId);

        // Step 3: Wait for 1 minute
        Thread.sleep(Duration.ofMinutes(1).toMillis());

        // Step 4: Check if the task status is stopped
        List<JsonObject> tasks = getTasks(detectorId, 1, (hits, expectedSize) -> hits.size() >= expectedSize, client());
        assertFalse("Expected at least one task", tasks.isEmpty());

        JsonObject task = tasks.get(0);

        String taskState = task.get("state").getAsString();

        assertEquals("Task state should be STOPPED", TaskState.STOPPED.name(), taskState);
    }

}
