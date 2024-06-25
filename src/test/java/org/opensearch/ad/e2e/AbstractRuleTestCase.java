/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.TreeMap;

import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.client.RestClient;

import com.google.gson.JsonObject;

public class AbstractRuleTestCase extends AbstractADSyntheticDataTest {

    protected static class TrainResult {
        String detectorId;
        List<JsonObject> data;
        // actual index of training data. As we have multiple entities,
        // trainTestSplit means how many groups of entities are used for training.
        // rawDataTrainTestSplit is the actual index of training data.
        int rawDataTrainTestSplit;
        Duration windowDelay;

        public TrainResult(String detectorId, List<JsonObject> data, int rawDataTrainTestSplit, Duration windowDelay) {
            this.detectorId = detectorId;
            this.data = data;
            this.rawDataTrainTestSplit = rawDataTrainTestSplit;
            this.windowDelay = windowDelay;
        }
    }

    /**
     * Ingest all of the data in file datasetName
     *
     * @param datasetName data set file name
     * @param intervalMinutes detector interval
     * @param numberOfEntities number of entities in the file
     * @param trainTestSplit used to calculate train start time
     * @param useDateNanos whether to use nano date type in detector timestamp
     * @return TrainResult for the following method calls
     * @throws Exception failing to ingest data
     */
    protected TrainResult ingestTrainData(
        String datasetName,
        int intervalMinutes,
        int numberOfEntities,
        int trainTestSplit,
        boolean useDateNanos
    ) throws Exception {
        return ingestTrainData(datasetName, intervalMinutes, numberOfEntities, trainTestSplit, useDateNanos, -1);
    }

    protected TrainResult ingestTrainData(
        String datasetName,
        int intervalMinutes,
        int numberOfEntities,
        int trainTestSplit,
        boolean useDateNanos,
        int ingestDataSize
    ) throws Exception {
        String dataFileName = String.format(Locale.ROOT, "data/%s.data", datasetName);

        List<JsonObject> data = getData(dataFileName);

        RestClient client = client();
        String categoricalField = "componentName";
        String mapping = String
            .format(
                Locale.ROOT,
                "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\":"
                    + (useDateNanos ? "\"date_nanos\"" : "\"date\"")
                    + "},"
                    + " \"transform._doc_count\": { \"type\": \"integer\" },"
                    + "\"%s\": { \"type\": \"keyword\"} } } }",
                categoricalField
            );

        if (ingestDataSize <= 0) {
            bulkIndexData(data, datasetName, client, mapping, data.size());
        } else {
            bulkIndexData(data, datasetName, client, mapping, ingestDataSize);
        }

        // we need to account that interval can have multiple entity record
        int rawDataTrainTestSplit = trainTestSplit * numberOfEntities;
        String trainTimeStr = data.get(rawDataTrainTestSplit - 1).get("timestamp").getAsString();
        Instant trainTime = Instant.ofEpochMilli(Long.parseLong(trainTimeStr));
        /*
         * The {@code CompositeRetriever.PageIterator.hasNext()} method checks if a request is expired
         * relative to the current system time. This method is designed to ensure that the execution time
         * is set to either the current time or a future time to prevent premature expirations in our tests.
         *
         * Also, AD accepts windowDelay in the unit of minutes. Thus, we need to convert the delay in minutes. This will
         * make it easier to search for results based on data end time. Otherwise, real data time and the converted
         * data time from request time.
         * Assume x = real data time. y= real window delay. y'= window delay in minutes. If y and y' are different,
         * x + y - y' != x.
         */
        long windowDelayMinutes = Duration.between(trainTime, Instant.now()).toMinutes();
        Duration windowDelay = Duration.ofMinutes(windowDelayMinutes);

        String detector = String
            .format(
                Locale.ROOT,
                "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"transform._doc_count\" } } } }"
                    + "], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                    + "\"category_field\": [\"%s\"], "
                    + "\"window_delay\": { \"period\": {\"interval\": %d, \"unit\": \"MINUTES\"}},"
                    + "\"history\": %d,"
                    + "\"schema_version\": 0,"
                    + "\"rules\": [{\"action\": \"ignore_anomaly\", \"conditions\": [{\"feature_name\": \"feature 1\", \"threshold_type\": \"actual_over_expected_ratio\", \"operator\": \"lte\", \"value\": 0.3}, "
                    + "{\"feature_name\": \"feature 1\", \"threshold_type\": \"expected_over_actual_ratio\", \"operator\": \"lte\", \"value\": 0.3}"
                    + "]}]"
                    + "}",
                datasetName,
                intervalMinutes,
                categoricalField,
                windowDelayMinutes,
                trainTestSplit - 1
            );
        String detectorId = createDetector(client, detector);
        LOG.info("Created detector {}", detectorId);

        Instant executeBegin = dataToExecutionTime(trainTime, windowDelay);
        Instant executeEnd = executeBegin.plus(intervalMinutes, ChronoUnit.MINUTES);
        Instant dataEnd = trainTime.plus(intervalMinutes, ChronoUnit.MINUTES);

        LOG.info("start detector {}", detectorId);
        simulateStartDetector(detectorId, executeBegin, executeEnd, client, numberOfEntities);
        int resultsToWait = findTrainTimeEntities(rawDataTrainTestSplit - 1, data);
        LOG.info("wait for initting detector {}. {} results are expected.", detectorId, resultsToWait);
        simulateWaitForInitDetector(detectorId, client, dataEnd, resultsToWait);

        return new TrainResult(detectorId, data, rawDataTrainTestSplit, windowDelay);
    }

    /**
     * Assume the data is sorted in time. The method look up and below startIndex
     * and return how many timestamps equal to timestampStr.
     * @param startIndex where to start look for timestamp
     * @return how many timestamps equal to timestampStr
     */
    protected int findTrainTimeEntities(int startIndex, List<JsonObject> data) {
        String timestampStr = data.get(startIndex).get("timestamp").getAsString();
        int count = 1;
        for (int i = startIndex - 1; i >= 0; i--) {
            String trainTimeStr = data.get(i).get("timestamp").getAsString();
            if (trainTimeStr.equals(timestampStr)) {
                count++;
            } else {
                break;
            }
        }
        for (int i = startIndex + 1; i < data.size(); i++) {
            String trainTimeStr = data.get(i).get("timestamp").getAsString();
            if (trainTimeStr.equals(timestampStr)) {
                count++;
            } else {
                break;
            }
        }
        return count;
    }

    protected Instant dataToExecutionTime(Instant instant, Duration windowDelay) {
        return instant.plus(windowDelay);
    }

    /**
     *
     * @param testData current data to score
     * @param entityMap a map to record the number of times we have seen a timestamp. Used to detect missing values.
     * @param windowDelay ingestion delay
     * @param intervalMinutes detector interval
     * @param detectorId detector Id
     * @param client RestFul client
     * @param numberOfEntities the number of entities.
     * @return whether we erred out.
     */
    protected boolean scoreOneResult(
        JsonObject testData,
        TreeMap<String, Integer> entityMap,
        Duration windowDelay,
        int intervalMinutes,
        String detectorId,
        RestClient client,
        int numberOfEntities
    ) {
        String beginTimeStampAsString = testData.get("timestamp").getAsString();
        Integer newCount = entityMap.compute(beginTimeStampAsString, (key, oldValue) -> (oldValue == null) ? 1 : oldValue + 1);
        if (newCount > 1) {
            // we have seen this timestamp before. Without this line, we will get rcf IllegalArgumentException about out of order tuples
            return false;
        }
        Instant begin = dataToExecutionTime(Instant.ofEpochMilli(Long.parseLong(beginTimeStampAsString)), windowDelay);
        Instant end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
        try {
            runDetectionResult(detectorId, begin, end, client, numberOfEntities);
        } catch (Exception e) {
            LOG.error("failed to run detection result", e);
            return true;
        }
        return false;
    }

}
