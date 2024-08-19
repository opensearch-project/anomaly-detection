/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.tuple.Triple;

import com.google.gson.JsonObject;

public abstract class AbstractRuleModelPerfTestCase extends AbstractRuleTestCase {
    protected void verifyTestResults(
        Triple<Map<String, double[]>, Integer, Map<String, Set<Integer>>> testResults,
        Map<String, List<Entry<Instant, Instant>>> anomalies,
        Map<String, Double> minPrecision,
        Map<String, Double> minRecall,
        int maxError
    ) {
        Map<String, double[]> resultMap = testResults.getLeft();
        Map<String, Set<Integer>> foundWindows = testResults.getRight();

        for (Entry<String, double[]> entry : resultMap.entrySet()) {
            String entity = entry.getKey();
            double[] testResultsArray = entry.getValue();
            double positives = testResultsArray[0];
            double truePositives = testResultsArray[1];

            // precision = predicted anomaly points that are true / predicted anomaly points
            double precision = positives > 0 ? truePositives / positives : 0;
            double minPrecisionValue = minPrecision.getOrDefault(entity, .4);
            assertTrue(
                String
                    .format(
                        Locale.ROOT,
                        "precision expected at least %f but got %f. positives %f, truePositives %f",
                        minPrecisionValue,
                        precision,
                        positives,
                        truePositives
                    ),
                precision >= minPrecisionValue
            );

            // recall = windows containing predicted anomaly points / total anomaly windows
            int anomalyWindow = anomalies.getOrDefault(entity, new ArrayList<>()).size();
            int foundWindowSize = foundWindows.getOrDefault(entity, new HashSet<>()).size();
            double recall = anomalyWindow > 0 ? foundWindowSize * 1.0d / anomalyWindow : 0;
            double minRecallValue = minRecall.getOrDefault(entity, .7);
            assertTrue(
                String
                    .format(
                        Locale.ROOT,
                        "recall should be at least %f but got %f. anomalyWindow %d, foundWindowSize %d ",
                        minRecallValue,
                        recall,
                        anomalyWindow,
                        foundWindowSize
                    ),
                recall >= minRecallValue
            );

            LOG.info("Entity {}, Precision: {}, Window recall: {}", entity, precision, recall);
        }

        int errors = testResults.getMiddle();
        assertTrue(errors <= maxError);
    }

    protected void analyzeResults(
        Map<String, List<Entry<Instant, Instant>>> anomalies,
        Map<String, double[]> res,
        Map<String, Set<Integer>> foundWindow,
        String beginTimeStampAsString,
        int entitySize,
        Instant begin,
        List<JsonObject> sourceList
    ) {
        assertTrue(
            String
                .format(
                    Locale.ROOT,
                    "the number of results is %d at %s, expected %d ",
                    sourceList.size(),
                    beginTimeStampAsString,
                    entitySize
                ),
            sourceList.size() == entitySize
        );
        for (int j = 0; j < entitySize; j++) {
            JsonObject source = sourceList.get(j);
            double anomalyGrade = getAnomalyGrade(source);
            assertTrue("anomalyGrade cannot be negative", anomalyGrade >= 0);
            if (anomalyGrade > 0) {
                String entity = getEntity(source);
                double[] entityResult = res.computeIfAbsent(entity, key -> new double[] { 0, 0 });
                // positive++
                entityResult[0]++;
                Instant anomalyTime = getAnomalyTime(source, begin);
                LOG.info("Found anomaly: entity {}, time {} result {}.", entity, anomalyTime, source);
                int anomalyWindow = isAnomaly(anomalyTime, anomalies.getOrDefault(entity, new ArrayList<>()));
                if (anomalyWindow != -1) {
                    LOG.info("True anomaly: entity {}, time {}.", entity, begin);
                    // truePositives++;
                    entityResult[1]++;
                    Set<Integer> window = foundWindow.computeIfAbsent(entity, key -> new HashSet<>());
                    window.add(anomalyWindow);
                }
            }
        }
    }
}
