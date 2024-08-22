/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.opensearch.client.RestClient;

import com.google.gson.JsonObject;

public class HistoricalRuleModelPerfIT extends AbstractRuleModelPerfTestCase {
    static final Logger LOG = (Logger) LogManager.getLogger(HistoricalRuleModelPerfIT.class);

    public void testRule() throws Exception {
        // TODO: this test case will run for a much longer time and timeout with security enabled
        if (!isHttps()) {
            disableResourceNotFoundFaultTolerence();
            // there are 8 entities in the data set. Each one needs 1500 rows as training data.
            Map<String, Double> minPrecision = new HashMap<>();
            minPrecision.put("Phoenix", 0.4);
            minPrecision.put("Scottsdale", 0.5);
            Map<String, Double> minRecall = new HashMap<>();
            minRecall.put("Phoenix", 0.9);
            minRecall.put("Scottsdale", 0.6);
            verifyRule("rule", 10, minPrecision.size(), 1500, minPrecision, minRecall, 20);
        }
    }

    public void verifyRule(
        String datasetName,
        int intervalMinutes,
        int numberOfEntities,
        int trainTestSplit,
        Map<String, Double> minPrecision,
        Map<String, Double> minRecall,
        int maxError
    ) throws Exception {

        String labelFileName = String.format(Locale.ROOT, "data/%s.label", datasetName);
        Map<String, List<Entry<Instant, Instant>>> anomalies = getAnomalyWindowsMap(labelFileName);

        TrainResult trainResult = ingestTrainDataAndCreateDetector(datasetName, intervalMinutes, numberOfEntities, trainTestSplit, false);
        startHistoricalDetector(trainResult, numberOfEntities, intervalMinutes, false);

        Triple<Map<String, double[]>, Integer, Map<String, Set<Integer>>> results = getTestResults(
            trainResult.detectorId,
            trainResult.data,
            trainResult.rawDataTrainTestSplit,
            intervalMinutes,
            anomalies,
            client(),
            numberOfEntities,
            trainResult.windowDelay
        );
        verifyTestResults(results, anomalies, minPrecision, minRecall, maxError);
    }

    private Triple<Map<String, double[]>, Integer, Map<String, Set<Integer>>> getTestResults(
        String detectorId,
        List<JsonObject> data,
        int rawTrainTestSplit,
        int intervalMinutes,
        Map<String, List<Entry<Instant, Instant>>> anomalies,
        RestClient client,
        int numberOfEntities,
        Duration windowDelay
    ) throws Exception {

        Map<String, double[]> res = new HashMap<>();
        int errors = 0;
        // an entity might have missing values (e.g., at timestamp 1694713200000).
        // Use a map to record the number of times we have seen them.
        // data start time -> the number of entities
        TreeMap<String, Integer> entityMap = new TreeMap<>();
        // historical won't detect the last point as
        // 1) ParseUtils.batchFeatureQuery uses left closed right open range to construct query
        // 2) ADBatchTaskRunner.runNextPiece will stop when the next piece start time is larger than or equal to dataEnd time
        // 3) ADBatchTaskRunner.getDateRangeOfSourceData will make data start/end time equal to min/max data time.
        for (int i = rawTrainTestSplit; i < data.size() - numberOfEntities; i++) {
            entityMap.compute(data.get(i).get("timestamp").getAsString(), (key, oldValue) -> (oldValue == null) ? 1 : oldValue + 1);
        }

        // hash set to dedup
        Map<String, Set<Integer>> foundWindow = new HashMap<>();
        long intervalMillis = intervalMinutes * 60000;

        // Iterate over the TreeMap in ascending order of keys
        for (Map.Entry<String, Integer> entry : entityMap.entrySet()) {
            String beginTimeStampAsString = entry.getKey();
            int entitySize = entry.getValue();
            Instant begin = Instant.ofEpochMilli(Long.parseLong(beginTimeStampAsString));
            Instant end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
            try {
                List<JsonObject> sourceList = getAnomalyResultByDataTime(detectorId, end, entitySize, client, true, intervalMillis);
                analyzeResults(anomalies, res, foundWindow, beginTimeStampAsString, entitySize, begin, sourceList);
            } catch (Exception e) {
                errors++;
                LOG.error("failed to get detection results", e);
            }
        }
        return Triple.of(res, errors, foundWindow);
    }
}
