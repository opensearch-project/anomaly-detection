/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.util.List;
import java.util.Locale;

import org.opensearch.ad.AbstractADSyntheticDataTest;

import com.google.gson.JsonObject;

/**
 * Test if real time and historical run together, historical won't reset real time's latest flag
 *
 */
public class MixedRealtimeHistoricalIT extends AbstractADSyntheticDataTest {

    public void testMixed() throws Exception {
        String datasetName = "synthetic";
        String dataFileName = String.format(Locale.ROOT, "data/%s.data", datasetName);
        int intervalsToWait = 3;

        List<JsonObject> data = getData(dataFileName);

        String mapping = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
            + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" } } } }";
        int trainTestSplit = 1500;
        // train data plus a few data points for real time inference
        int totalDataToIngest = trainTestSplit + intervalsToWait + 3;
        bulkIndexTrainData(datasetName, data, totalDataToIngest, client(), mapping);

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

        startHistorical(
            detectorId,
            getDataTimeofISOFormat("timestamp", data, 0),
            getDataTimeofISOFormat("timestamp", data, totalDataToIngest),
            client(),
            1
        );

        int size = 2;
        List<JsonObject> results = getTasks(detectorId, size, (h, eSize) -> h.size() >= eSize, client());

        assertEquals(String.format(Locale.ROOT, "Expected %d, but got %d", size, results.size()), size, results.size());
        for (int i = 0; i < size; i++) {
            assert (getLatest(results, i));
        }
    }

}
