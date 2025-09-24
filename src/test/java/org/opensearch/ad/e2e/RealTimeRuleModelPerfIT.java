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

public class RealTimeRuleModelPerfIT extends AbstractRuleModelPerfTestCase {
    static final Logger LOG = (Logger) LogManager.getLogger(RealTimeRuleModelPerfIT.class);

    public void testRule() throws Exception {
        // TODO: this test case will run for a much longer time and timeout with security enabled
        if (!isHttps()) {
            disableResourceNotFoundFaultTolerence();
            // there are 2 entities in the data set, totalling 18140 rows in the training data (with missing values).
            Map<String, Double> minPrecision = new HashMap<>();
            minPrecision.put("Phoenix", 0.5);
            minPrecision.put("Scottsdale", 0.5);
            Map<String, Double> minRecall = new HashMap<>();
            minRecall.put("Phoenix", 0.7);
            minRecall.put("Scottsdale", 0.3);
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
        verifyRule(datasetName, intervalMinutes, numberOfEntities, trainTestSplit, minPrecision, minRecall, maxError, false);
    }

    public void verifyRule(
        String datasetName,
        int intervalMinutes,
        int numberOfEntities,
        int trainTestSplit,
        Map<String, Double> minPrecision,
        Map<String, Double> minRecall,
        int maxError,
        boolean useDateNanos
    ) throws Exception {

        String labelFileName = String.format(Locale.ROOT, "data/%s.label", datasetName);
        Map<String, List<Entry<Instant, Instant>>> anomalies = getAnomalyWindowsMap(labelFileName);

        TrainResult trainResult = ingestTrainDataAndCreateDetector(
            datasetName,
            intervalMinutes,
            numberOfEntities,
            trainTestSplit,
            useDateNanos
        );
        startRealTimeDetector(trainResult, numberOfEntities, intervalMinutes, false);

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
        for (int i = rawTrainTestSplit; i < data.size(); i++) {
            if (scoreOneResult(
                data.get(i).get("timestamp").getAsString(),
                entityMap,
                windowDelay,
                intervalMinutes,
                detectorId,
                client,
                numberOfEntities
            )) {
                errors++;
            }
        }

        // hash set to dedup
        Map<String, Set<Integer>> foundWindow = new HashMap<>();

        // Iterate over the TreeMap in ascending order of keys
        for (Map.Entry<String, Integer> entry : entityMap.entrySet()) {
            String beginTimeStampAsString = entry.getKey();
            Instant begin = Instant.ofEpochMilli(Long.parseLong(beginTimeStampAsString));
            Instant end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
            try {
                // since the aggregation is sum, we will have feature value 0 when there is missing values.
                // So we need to verify the number of results is equal to the number of entities numberOfEntities.
                List<JsonObject> sourceList = getRealTimeAnomalyResult(detectorId, end, numberOfEntities, client);

                analyzeResults(anomalies, res, foundWindow, beginTimeStampAsString, numberOfEntities, begin, sourceList);
            } catch (Exception e) {
                errors++;
                LOG.error("failed to get detection results", e);
            }
        }
        return Triple.of(res, errors, foundWindow);
    }
}
