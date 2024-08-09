/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.client.RestClient;
import org.opensearch.timeseries.AbstractSyntheticDataTest;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;

import com.google.gson.JsonObject;

public abstract class MissingIT extends AbstractADSyntheticDataTest {
    protected static double min = 200.0;
    protected static double max = 240.0;
    protected static int dataSize = 400;

    protected static List<Double> randomDoubles;
    protected static String datasetName = "missing";

    protected int intervalMinutes = 10;
    public long intervalMillis = intervalMinutes * 60000L;
    protected String categoricalField = "componentName";
    protected int maxError = 20;
    protected int trainTestSplit = 100;

    public int continuousImputeStartIndex = 11;
    public int continuousImputeEndIndex = 35;

    protected Map<String, Double> lastSeen = new HashMap<>();

    @BeforeClass
    public static void setUpOnce() {
        // Generate the list of doubles
        randomDoubles = generateUniformRandomDoubles(dataSize, min, max);
    }

    protected void verifyImputation(
        ImputationMethod imputation,
        Map<String, Double> lastSeen,
        double dataValue,
        JsonObject imputed0,
        String entity
    ) {
        assertTrue(imputed0.get("imputed").getAsBoolean());
        switch (imputation) {
            case ZERO:
                assertEquals(0, dataValue, EPSILON);
                break;
            case PREVIOUS:
                // if we have recorded lastSeen
                Double entityValue = lastSeen.get(entity);
                if (entityValue != null && !areDoublesEqual(entityValue, -1)) {
                    assertEquals(entityValue, dataValue, EPSILON);
                }
                break;
            case FIXED_VALUES:
                assertEquals(1, dataValue, EPSILON);
                break;
            default:
                assertTrue(false);
                break;
        }
    }

    protected TrainResult createAndStartRealTimeDetector(
        int numberOfEntities,
        int trainTestSplit,
        List<JsonObject> data,
        ImputationMethod imputation,
        boolean hc,
        long trainTimeMillis
    ) throws Exception {
        TrainResult trainResult = createDetector(numberOfEntities, trainTestSplit, data, imputation, hc, trainTimeMillis);
        List<JsonObject> result = startRealTimeDetector(trainResult, numberOfEntities, intervalMinutes, true);
        recordLastSeenFromResult(result);

        return trainResult;
    }

    protected TrainResult createAndStartHistoricalDetector(
        int numberOfEntities,
        int trainTestSplit,
        List<JsonObject> data,
        ImputationMethod imputation,
        boolean hc,
        long trainTimeMillis
    ) throws Exception {
        TrainResult trainResult = createDetector(numberOfEntities, trainTestSplit, data, imputation, hc, trainTimeMillis);
        List<JsonObject> result = startHistoricalDetector(trainResult, numberOfEntities, intervalMinutes, true);
        recordLastSeenFromResult(result);

        return trainResult;
    }

    protected void recordLastSeenFromResult(List<JsonObject> result) {
        for (int j = 0; j < result.size(); j++) {
            JsonObject source = result.get(j);
            lastSeen.put(getEntity(source), extractFeatureValue(source));
        }
    }

    protected TrainResult createDetector(
        int numberOfEntities,
        int trainTestSplit,
        List<JsonObject> data,
        ImputationMethod imputation,
        boolean hc,
        long trainTimeMillis
    ) throws Exception {
        Instant trainTime = Instant.ofEpochMilli(trainTimeMillis);

        Duration windowDelay = getWindowDelay(trainTimeMillis);
        String detector = genDetector(trainTestSplit, windowDelay.toMinutes(), hc, imputation, trainTimeMillis);

        RestClient client = client();
        String detectorId = createDetector(client, detector);
        LOG.info("Created detector {}", detectorId);

        return new TrainResult(detectorId, data, trainTestSplit * numberOfEntities, windowDelay, trainTime);
    }

    protected Duration getWindowDelay(long trainTimeMillis) {
        /*
         * AD accepts windowDelay in the unit of minutes. Thus, we need to convert the delay in minutes. This will
         * make it easier to search for results based on data end time. Otherwise, real data time and the converted
         * data time from request time.
         * Assume x = real data time. y= real window delay. y'= window delay in minutes. If y and y' are different,
         * x + y - y' != x.
         */
        long currentTime = System.currentTimeMillis();
        long windowDelayMinutes = (trainTimeMillis - currentTime) / 60000;
        LOG.info("train time {}, current time {}, window delay {}", trainTimeMillis, currentTime, windowDelayMinutes);
        return Duration.ofMinutes(windowDelayMinutes);
    }

    protected void ingestUniformSingleFeatureData(int ingestDataSize, List<JsonObject> data) throws Exception {
        ingestUniformSingleFeatureData(ingestDataSize, data, datasetName, categoricalField);
    }

    protected JsonObject createJsonObject(long timestamp, String component, double dataValue) {
        return createJsonObject(timestamp, component, dataValue, categoricalField);
    }

    protected abstract String genDetector(
        int trainTestSplit,
        long windowDelayMinutes,
        boolean hc,
        ImputationMethod imputation,
        long trainTimeMillis
    );

    protected abstract AbstractSyntheticDataTest.GenData genData(
        int trainTestSplit,
        int numberOfEntities,
        AbstractSyntheticDataTest.MISSING_MODE missingMode
    ) throws Exception;

    protected abstract void runTest(
        long firstDataStartTime,
        AbstractSyntheticDataTest.GenData dataGenerated,
        Duration windowDelay,
        String detectorId,
        int numberOfEntities,
        AbstractSyntheticDataTest.MISSING_MODE mode,
        ImputationMethod imputation,
        int numberOfMissingToCheck,
        boolean realTime
    );

    protected abstract double extractFeatureValue(JsonObject source);
}
