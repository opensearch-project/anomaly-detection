/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import com.google.gson.JsonObject;

public class RealTimeRuleIT extends AbstractRuleTestCase {
    public void testRuleWithDateNanos() throws Exception {
        // TODO: this test case will run for a much longer time and timeout with security enabled
        if (!isHttps()) {
            disableResourceNotFoundFaultTolerence();

            String datasetName = "rule";
            int intervalMinutes = 10;
            int numberOfEntities = 2;
            int trainTestSplit = 100;

            TrainResult trainResult = ingestTrainDataAndCreateDetector(
                datasetName,
                intervalMinutes,
                numberOfEntities,
                trainTestSplit,
                true,
                // ingest just enough for finish the test
                (trainTestSplit + 1) * numberOfEntities
            );

            startRealTimeDetector(trainResult, numberOfEntities, intervalMinutes, false);
            List<JsonObject> data = trainResult.data;
            LOG.info("scoring data at {}", data.get(trainResult.rawDataTrainTestSplit).get("timestamp").getAsString());

            // one run call will evaluate all entities within an interval
            int numberEntitiesScored = findGivenTimeEntities(trainResult.rawDataTrainTestSplit, data);
            // an entity might have missing values (e.g., at timestamp 1694713200000).
            // Use a map to record the number of times we have seen them.
            // data start time -> the number of entities
            TreeMap<String, Integer> entityMap = new TreeMap<>();
            // rawDataTrainTestSplit is the actual index of next test data.
            assertFalse(
                scoreOneResult(
                    data.get(trainResult.rawDataTrainTestSplit).get("timestamp").getAsString(),
                    entityMap,
                    trainResult.windowDelay,
                    intervalMinutes,
                    trainResult.detectorId,
                    client(),
                    numberEntitiesScored
                )
            );

            assertEquals("expected 1 timestamp but got " + entityMap.size(), 1, entityMap.size());

            for (Map.Entry<String, Integer> entry : entityMap.entrySet()) {
                String beginTimeStampAsString = entry.getKey();
                Instant begin = Instant.ofEpochMilli(Long.parseLong(beginTimeStampAsString));
                Instant end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
                try {
                    List<JsonObject> sourceList = getRealTimeAnomalyResult(trainResult.detectorId, end, numberEntitiesScored, client());

                    assertTrue(
                        String
                            .format(
                                Locale.ROOT,
                                "the number of results is %d at %s, expected %d ",
                                sourceList.size(),
                                beginTimeStampAsString,
                                numberEntitiesScored
                            ),
                        sourceList.size() == numberEntitiesScored
                    );
                    for (int j = 0; j < numberEntitiesScored; j++) {
                        JsonObject source = sourceList.get(j);
                        double anomalyGrade = getAnomalyGrade(source);
                        assertTrue("anomalyGrade cannot be negative", anomalyGrade >= 0);
                    }
                } catch (Exception e) {
                    LOG.error("failed to get detection results", e);
                    assertTrue(false);
                }
            }
        }
    }
}
