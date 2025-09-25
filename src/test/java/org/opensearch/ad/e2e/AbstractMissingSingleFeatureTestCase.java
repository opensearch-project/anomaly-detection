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
import java.util.NavigableSet;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.timeseries.AbstractSyntheticDataTest;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;

import com.google.gson.JsonObject;

public abstract class AbstractMissingSingleFeatureTestCase extends MissingIT {
    protected static final Logger LOG = (Logger) LogManager.getLogger(AbstractMissingSingleFeatureTestCase.class);

    @Override
    protected String genDetector(
        int trainTestSplit,
        long windowDelayMinutes,
        boolean hc,
        ImputationMethod imputation,
        long trainTimeMillis,
        String name
    ) {
        StringBuilder sb = new StringBuilder();
        // common part
        sb
            .append(
                "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature1\": { \"avg\": { \"field\": \"data\" } } } }"
                    + "], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                    + "\"history\": %d,"
            );

        if (windowDelayMinutes > 0) {
            sb
                .append(
                    String
                        .format(
                            Locale.ROOT,
                            "\"window_delay\": { \"period\": {\"interval\": %d, \"unit\": \"MINUTES\"}},",
                            windowDelayMinutes
                        )
                );
        }
        if (hc) {
            sb.append("\"category_field\": [\"%s\"], ");
        }

        switch (imputation) {
            case ZERO:
                sb.append("\"imputation_option\": { \"method\": \"zero\" },");
                break;
            case PREVIOUS:
                sb.append("\"imputation_option\": { \"method\": \"previous\" },");
                break;
            case FIXED_VALUES:
                sb
                    .append(
                        "\"imputation_option\": { \"method\": \"fixed_values\", \"default_fill\": [{ \"feature_name\" : \"feature 1\", \"data\": 1 }] },"
                    );
                break;
        }
        // end
        sb.append("\"schema_version\": 0}");

        if (hc) {
            return String.format(Locale.ROOT, sb.toString(), datasetName, intervalMinutes, trainTestSplit - 1, categoricalField);
        } else {
            return String.format(Locale.ROOT, sb.toString(), datasetName, intervalMinutes, trainTestSplit - 1);
        }

    }

    @Override
    protected AbstractSyntheticDataTest.GenData genData(
        int trainTestSplit,
        int numberOfEntities,
        AbstractSyntheticDataTest.MISSING_MODE missingMode
    ) throws Exception {
        return genUniformSingleFeatureData(
            intervalMinutes,
            trainTestSplit,
            numberOfEntities,
            categoricalField,
            missingMode,
            continuousImputeStartIndex,
            continuousImputeEndIndex,
            randomDoubles
        );
    }

    @Override
    protected double extractFeatureValue(JsonObject source) {
        JsonObject feature0 = getFeature(source, 0);
        return feature0.get("data").getAsDouble();
    }

    protected void verifyGrade(Integer testIndex, ImputationMethod imputation, double anomalyGrade) {
        if (testIndex == continuousImputeStartIndex || testIndex == continuousImputeEndIndex + 1) {
            switch (imputation) {
                case ZERO:
                    assertTrue(Double.compare(anomalyGrade, 0) > 0);
                    break;
                case PREVIOUS:
                    assertEquals(anomalyGrade, 0, EPSILON);
                    break;
                case FIXED_VALUES:
                    assertTrue(Double.compare(anomalyGrade, 0) > 0);
                    break;
                default:
                    assertTrue(false);
                    break;
            }
        } else {
            assertEquals("testIndex: " + testIndex, 0, anomalyGrade, EPSILON);
        }
    }

    protected boolean verifyConfidence(Integer testIndex, double confidence, Double lastConfidence) {
        if (lastConfidence == null) {
            return false;
        }

        // we will see confidence increasing again after some point in shingle size (default 8)
        if (testIndex <= continuousImputeStartIndex || testIndex >= continuousImputeEndIndex + 8) {
            assertTrue(
                String.format(Locale.ROOT, "confidence: %f, lastConfidence: %f, testIndex: %d", confidence, lastConfidence, testIndex),
                Double.compare(confidence, lastConfidence) >= 0
            );
        } else if (testIndex > continuousImputeStartIndex && testIndex <= continuousImputeEndIndex) {
            assertTrue(
                String.format(Locale.ROOT, "confidence: %f, lastConfidence: %f, testIndex: %d", confidence, lastConfidence, testIndex),
                Double.compare(confidence, lastConfidence) <= 0
            );
        }
        return true;
    }

    @Override
    protected void runTest(
        long firstDataStartTime,
        AbstractSyntheticDataTest.GenData dataGenerated,
        Duration windowDelay,
        String detectorId,
        int numberOfEntities,
        AbstractSyntheticDataTest.MISSING_MODE mode,
        ImputationMethod imputation,
        int numberOfMissingToCheck,
        boolean realTime
    ) {
        int errors = 0;
        List<JsonObject> data = dataGenerated.data;
        long lastDataStartTime = data.get(data.size() - 1).get("timestamp").getAsLong();
        long dataStartTime = firstDataStartTime + intervalMillis;
        NavigableSet<Long> missingTimestamps = dataGenerated.missingTimestamps;
        NavigableSet<Pair<Long, String>> missingEntities = dataGenerated.missingEntities;

        // we might miss timestamps at the end
        if (mode == AbstractSyntheticDataTest.MISSING_MODE.MISSING_TIMESTAMP
            || mode == AbstractSyntheticDataTest.MISSING_MODE.CONTINUOUS_IMPUTE) {
            lastDataStartTime = Math.max(missingTimestamps.last(), lastDataStartTime);
        } else if (mode == AbstractSyntheticDataTest.MISSING_MODE.MISSING_ENTITY) {
            lastDataStartTime = Math.max(missingEntities.last().getLeft(), lastDataStartTime);
        }
        // an entity might have missing values (e.g., at timestamp 1694713200000).
        // Use a map to record the number of times we have seen them.
        // data start time -> the number of entities
        TreeMap<String, Integer> entityMap = new TreeMap<>();

        int missingIndex = 0;
        Map<String, Double> lastConfidence = new HashMap<>();

        // look two default shingle size after imputation area
        long continuousModeStopTime = dataStartTime + intervalMillis * (continuousImputeEndIndex + 16);

        // exit when reaching last date time or we have seen at least three missing values.
        // In continuous impute mode, we may want to read a few more points to check if confidence increases or not
        LOG.info("lastDataStartTime: {}, dataStartTime: {}", lastDataStartTime, dataStartTime);
        // test data 0 is used trigger cold start
        int testIndex = 1;
        while (lastDataStartTime >= dataStartTime && (missingIndex <= numberOfMissingToCheck && continuousModeStopTime >= dataStartTime)) {
            // no need to call _run api in each interval in historical case
            if (realTime
                && scoreOneResult(
                    String.valueOf(dataStartTime),
                    entityMap,
                    windowDelay,
                    intervalMinutes,
                    detectorId,
                    client(),
                    numberOfEntities
                )) {
                errors++;
            }

            LOG.info("test index: {}", testIndex);

            Instant begin = Instant.ofEpochMilli(dataStartTime);
            Instant end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
            try {
                List<JsonObject> sourceList = null;
                if (realTime) {
                    sourceList = getRealTimeAnomalyResult(detectorId, end, numberOfEntities, client());
                } else {
                    sourceList = getAnomalyResultByDataTime(detectorId, end, numberOfEntities, client(), true, intervalMillis);
                }

                assertTrue(
                    String
                        .format(
                            Locale.ROOT,
                            "the number of results is %d at %s, expected %d ",
                            sourceList.size(),
                            end.toEpochMilli(),
                            numberOfEntities
                        ),
                    sourceList.size() == numberOfEntities
                );

                // used to track if any entity within a timestamp has imputation and then we increment
                // missingIndex outside the loop. Used in MISSING_TIMESTAMP mode.
                boolean imputed = false;
                for (int j = 0; j < numberOfEntities; j++) {
                    JsonObject source = sourceList.get(j);
                    JsonObject feature0 = getFeature(source, 0);
                    double dataValue = feature0.get("data").getAsDouble();

                    JsonObject imputed0 = getImputed(source, 0);

                    String entity = getEntity(source);

                    if (mode == AbstractSyntheticDataTest.MISSING_MODE.MISSING_TIMESTAMP && missingTimestamps.contains(dataStartTime)) {
                        verifyImputation(imputation, lastSeen, dataValue, imputed0, entity);
                        imputed = true;
                    } else if (mode == AbstractSyntheticDataTest.MISSING_MODE.MISSING_ENTITY
                        && missingEntities.contains(Pair.of(dataStartTime, entity))) {
                        verifyImputation(imputation, lastSeen, dataValue, imputed0, entity);
                        missingIndex++;
                    } else if (mode == AbstractSyntheticDataTest.MISSING_MODE.CONTINUOUS_IMPUTE) {
                        int imputeIndex = getIndex(missingTimestamps, dataStartTime);
                        double grade = getAnomalyGrade(source);
                        verifyGrade(testIndex, imputation, grade);

                        if (imputeIndex >= 0) {
                            imputed = true;
                        }

                        double confidence = getConfidence(source);
                        verifyConfidence(testIndex, confidence, lastConfidence.get(entity));
                        lastConfidence.put(entity, confidence);
                    } else {
                        assertEquals(
                            String.format(Locale.ROOT, "dataStartTime: %d, missingTimestamps: %s", dataStartTime, missingTimestamps),
                            null,
                            imputed0
                        );
                    }

                    lastSeen.put(entity, dataValue);
                }
                if (imputed) {
                    missingIndex++;
                }
            } catch (Exception e) {
                errors++;
                LOG.error("failed to get detection results", e);
            } finally {
                testIndex++;
            }

            dataStartTime += intervalMillis;
        }

        // at least numberOfMissingToCheck missing value imputation is seen
        assertTrue(
            String.format(Locale.ROOT, "missingIndex %d, numberOfMissingToCheck %d", missingIndex, numberOfMissingToCheck),
            missingIndex >= numberOfMissingToCheck
        );
        assertTrue(errors < maxError);
    }

    /**
     *
     * @param <E> element type
     * @param set ordered set
     * @param element element to compare
     * @return the index of the element that is less than or equal to the given element.
     */
    protected static <E> int getIndex(NavigableSet<E> set, E element) {
        if (!set.contains(element)) {
            return -1;
        }
        return set.headSet(element, true).size() - 1;
    }
}
