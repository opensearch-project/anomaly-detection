/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.timeseries.AbstractSyntheticDataTest;
import org.opensearch.timeseries.dataprocessor.ImputationMethod;

import com.google.gson.JsonObject;

public class MissingMultiFeatureIT extends MissingIT {

    public void testSingleStream() throws Exception {
        int numberOfEntities = 1;

        AbstractSyntheticDataTest.MISSING_MODE mode = AbstractSyntheticDataTest.MISSING_MODE.NO_MISSING_DATA;
        ImputationMethod method = ImputationMethod.ZERO;

        AbstractSyntheticDataTest.GenData dataGenerated = genData(trainTestSplit, numberOfEntities, mode);

        // only ingest train data to avoid validation error as we use latest data time as starting point.
        // otherwise, we will have too many missing points.
        ingestUniformSingleFeatureData(
            trainTestSplit + numberOfEntities * 6, // we only need a few to verify and trigger train.
            dataGenerated.data
        );

        TrainResult trainResult = createAndStartRealTimeDetector(
            numberOfEntities,
            trainTestSplit,
            dataGenerated.data,
            method,
            false,
            dataGenerated.testStartTime
        );

        runTest(
            dataGenerated.testStartTime,
            dataGenerated,
            trainResult.windowDelay,
            trainResult.detectorId,
            numberOfEntities,
            mode,
            method,
            3,
            true
        );
    }

    public void testHCFixed() throws Exception {
        int numberOfEntities = 2;

        AbstractSyntheticDataTest.MISSING_MODE mode = AbstractSyntheticDataTest.MISSING_MODE.NO_MISSING_DATA;
        ImputationMethod method = ImputationMethod.FIXED_VALUES;

        AbstractSyntheticDataTest.GenData dataGenerated = genData(trainTestSplit, numberOfEntities, mode);

        // only ingest train data to avoid validation error as we use latest data time as starting point.
        // otherwise, we will have too many missing points.
        ingestUniformSingleFeatureData(
            trainTestSplit + numberOfEntities * 6, // we only need a few to verify and trigger train.
            dataGenerated.data
        );

        TrainResult trainResult = createAndStartRealTimeDetector(
            numberOfEntities,
            trainTestSplit,
            dataGenerated.data,
            method,
            true,
            dataGenerated.testStartTime
        );

        runTest(
            dataGenerated.testStartTime,
            dataGenerated,
            trainResult.windowDelay,
            trainResult.detectorId,
            numberOfEntities,
            mode,
            method,
            3,
            true
        );
    }

    public void testHCPrevious() throws Exception {
        lastSeen.clear();
        int numberOfEntities = 2;

        AbstractSyntheticDataTest.MISSING_MODE mode = AbstractSyntheticDataTest.MISSING_MODE.NO_MISSING_DATA;
        ImputationMethod method = ImputationMethod.PREVIOUS;

        AbstractSyntheticDataTest.GenData dataGenerated = genData(trainTestSplit, numberOfEntities, mode);

        // only ingest train data to avoid validation error as we use latest data time as starting point.
        // otherwise, we will have too many missing points.
        ingestUniformSingleFeatureData(
            trainTestSplit + numberOfEntities * 6, // we only need a few to verify and trigger train.
            dataGenerated.data
        );

        TrainResult trainResult = createAndStartRealTimeDetector(
            numberOfEntities,
            trainTestSplit,
            dataGenerated.data,
            method,
            true,
            dataGenerated.testStartTime
        );

        runTest(
            dataGenerated.testStartTime,
            dataGenerated,
            trainResult.windowDelay,
            trainResult.detectorId,
            numberOfEntities,
            mode,
            method,
            3,
            true
        );
    }

    @Override
    protected String genDetector(
        int trainTestSplit,
        long windowDelayMinutes,
        boolean hc,
        ImputationMethod imputation,
        long trainTimeMillis
    ) {
        StringBuilder sb = new StringBuilder();

        // feature with filter so that we only get data for training and test will have missing value on this feature
        String featureWithFilter = String
            .format(
                Locale.ROOT,
                "{\n"
                    + "    \"feature_id\": \"feature1\",\n"
                    + "    \"feature_name\": \"feature 1\",\n"
                    + "    \"feature_enabled\": true,\n"
                    + "    \"importance\": 1,\n"
                    + "    \"aggregation_query\": {\n"
                    + "        \"Feature1\": {\n"
                    + "            \"filter\": {\n"
                    + "                \"bool\": {\n"
                    + "                    \"must\": [\n"
                    + "                        {\n"
                    + "                            \"range\": {\n"
                    + "                                \"timestamp\": {\n"
                    + "                                    \"lte\": %d\n"
                    + "                                }\n"
                    + "                            }\n"
                    + "                        }\n"
                    + "                    ]\n"
                    + "                }\n"
                    + "            },\n"
                    + "            \"aggregations\": {\n"
                    + "                \"deny_max1\": {\n"
                    + "                    \"max\": {\n"
                    + "                        \"field\": \"data\"\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            }\n"
                    + "        }\n"
                    + "    }\n"
                    + "}",
                trainTimeMillis
            );

        // common part
        sb
            .append(
                "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"%s\"], \"feature_attributes\": [{  \"feature_id\": \"feature2\", \"feature_name\": \"feature 2\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature2\": { \"avg\": { \"field\": \"data\" } } } },"
                    + featureWithFilter
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
                        "\"imputation_option\": { \"method\": \"fixed_values\", \"defaultFill\": [{ \"feature_name\" : \"feature 1\", \"data\": 1 }, { \"feature_name\" : \"feature 2\", \"data\": 2 }] },"
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
        List<JsonObject> data = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        long intervalMillis = intervalMinutes * 60000L;
        long oldestTime = currentTime - intervalMillis * trainTestSplit / numberOfEntities;
        int entityIndex = 0;
        NavigableSet<Pair<Long, String>> missingEntities = new TreeSet<>();
        NavigableSet<Long> missingTimestamps = new TreeSet<>();
        long testStartTime = 0;

        for (int i = 0; i < randomDoubles.size(); i++) {
            // we won't miss the train time (the first point triggering cold start)
            if (oldestTime > currentTime && testStartTime == 0) {
                LOG.info("test start time {}, index {}, current time {}", oldestTime, data.size(), currentTime);
                testStartTime = oldestTime;
            }
            JsonObject jsonObject = createJsonObject(oldestTime, "entity" + entityIndex, randomDoubles.get(i));
            data.add(jsonObject);
            entityIndex = (entityIndex + 1) % numberOfEntities;
            if (entityIndex == 0) {
                oldestTime += intervalMillis;
            }
        }
        return new AbstractSyntheticDataTest.GenData(data, missingEntities, missingTimestamps, testStartTime);
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

        long dataStartTime = firstDataStartTime + intervalMinutes * 60000;
        int missingIndex = 0;

        // an entity might have missing values (e.g., at timestamp 1694713200000).
        // Use a map to record the number of times we have seen them.
        // data start time -> the number of entities
        TreeMap<String, Integer> entityMap = new TreeMap<>();

        // exit when reaching last date time or we have seen at least three missing values
        while (lastDataStartTime >= dataStartTime && missingIndex <= numberOfMissingToCheck) {
            if (scoreOneResult(
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

            Instant begin = Instant.ofEpochMilli(dataStartTime);
            Instant end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
            try {
                List<JsonObject> sourceList = getRealTimeAnomalyResult(detectorId, end, numberOfEntities, client());

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

                for (int j = 0; j < numberOfEntities; j++) {
                    JsonObject source = sourceList.get(j);

                    double dataValue = extractFeatureValue(source);

                    String entity = getEntity(source);

                    JsonObject imputed1 = getImputed(source, "feature1");
                    // the feature starts missing since train time.
                    verifyImputation(imputation, lastSeen, dataValue, imputed1, entity);

                    JsonObject imputed2 = getImputed(source, "feature2");
                    assertTrue(!imputed2.get("imputed").getAsBoolean());
                }
                missingIndex++;
            } catch (Exception e) {
                errors++;
                LOG.error("failed to get detection results", e);
            }

            dataStartTime += intervalMinutes * 60000;
        }

        assertTrue(missingIndex > numberOfMissingToCheck);
        assertTrue(errors < maxError);
    }

    @Override
    protected double extractFeatureValue(JsonObject source) {
        for (int i = 0; i < 2; i++) {
            JsonObject feature = getFeature(source, i);
            if (feature.get("feature_name").getAsString().equals("feature 1")) {
                return feature.get("data").getAsDouble();
            }
        }
        throw new IllegalArgumentException("Fail to find feature 1");
    }
}
