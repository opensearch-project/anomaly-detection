/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.e2e;

import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.client.RestClient;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public abstract class AbstractRuleTestCase extends AbstractADSyntheticDataTest {
    String categoricalField = "cityName";

    /**
     * Ingest all of the data in file datasetName and create detector
     *
     * @param datasetName data set file name
     * @param intervalMinutes detector interval
     * @param numberOfEntities number of entities in the file
     * @param trainTestSplit used to calculate train start time
     * @param useDateNanos whether to use nano date type in detector timestamp
     * @return TrainResult for the following method calls
     * @throws Exception failing to ingest data
     */
    protected TrainResult ingestTrainDataAndCreateDetector(
        String datasetName,
        int intervalMinutes,
        int numberOfEntities,
        int trainTestSplit,
        boolean useDateNanos
    ) throws Exception {
        return ingestTrainDataAndCreateDetector(datasetName, intervalMinutes, numberOfEntities, trainTestSplit, useDateNanos, -1, true);
    }

    protected TrainResult ingestTrainDataAndCreateDetector(
        String datasetName,
        int intervalMinutes,
        int numberOfEntities,
        int trainTestSplit,
        boolean useDateNanos,
        int ingestDataSize,
        boolean relative
    ) throws Exception {
        TrainResult trainResult = ingestTrainData(
            datasetName,
            intervalMinutes,
            numberOfEntities,
            trainTestSplit,
            useDateNanos,
            ingestDataSize
        );

        String detector = genDetector(datasetName, intervalMinutes, trainTestSplit, trainResult, relative);
        String detectorId = createDetector(client(), detector);
        LOG.info("Created detector {}", detectorId);
        trainResult.detectorId = detectorId;

        return trainResult;
    }

    protected String genDetector(String datasetName, int intervalMinutes, int trainTestSplit, TrainResult trainResult, boolean relative) {
        // Determine threshold types and values based on the 'relative' parameter
        String thresholdType1;
        String thresholdType2;
        double value;
        if (relative) {
            thresholdType1 = "actual_over_expected_ratio";
            thresholdType2 = "expected_over_actual_ratio";
            value = 0.2;
        } else {
            thresholdType1 = "actual_over_expected_margin";
            thresholdType2 = "expected_over_actual_margin";
            value = 3000.0;
        }

        // Generate the detector JSON string with the appropriate threshold types and values
        String detector = String
            .format(
                Locale.ROOT,
                "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"visitCount\" } } } }"
                    + "], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                    + "\"category_field\": [\"%s\"], "
                    + "\"window_delay\": { \"period\": {\"interval\": %d, \"unit\": \"MINUTES\"}},"
                    + "\"history\": %d,"
                    + "\"schema_version\": 0,"
                    + "\"rules\": [{\"action\": \"ignore_anomaly\", \"conditions\": ["
                    + "{ \"feature_name\": \"feature 1\", \"threshold_type\": \"%s\", \"operator\": \"lte\", \"value\": %f }, "
                    + "{ \"feature_name\": \"feature 1\", \"threshold_type\": \"%s\", \"operator\": \"lte\", \"value\": %f }"
                    + "]}]"
                    + "}",
                datasetName,
                intervalMinutes,
                categoricalField,
                trainResult.windowDelay.toMinutes(),
                trainTestSplit - 1,
                thresholdType1,
                value,
                thresholdType2,
                value
            );
        return detector;
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
        String mapping = String
            .format(
                Locale.ROOT,
                "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\":"
                    + (useDateNanos ? "\"date_nanos\"" : "\"date\"")
                    + "},"
                    + " \"visitCount\": { \"type\": \"integer\" },"
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
        return new TrainResult(null, data, rawDataTrainTestSplit, windowDelay, trainTime, "timestamp");
    }

    public Map<String, List<Entry<Instant, Instant>>> getAnomalyWindowsMap(String labelFileName) throws Exception {
        JsonObject jsonObject = JsonParser
            .parseReader(new FileReader(new File(getClass().getResource(labelFileName).toURI()), Charset.defaultCharset()))
            .getAsJsonObject();

        Map<String, List<Entry<Instant, Instant>>> map = new HashMap<>();
        for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
            List<Entry<Instant, Instant>> anomalies = new ArrayList<>();
            JsonElement value = entry.getValue();
            if (value.isJsonArray()) {
                for (JsonElement elem : value.getAsJsonArray()) {
                    JsonElement beginElement = elem.getAsJsonArray().get(0);
                    JsonElement endElement = elem.getAsJsonArray().get(1);
                    Instant begin = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(beginElement.getAsString()));
                    Instant end = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(endElement.getAsString()));
                    anomalies.add(new SimpleEntry<>(begin, end));
                }
            }
            map.put(entry.getKey(), anomalies);
        }
        return map;
    }
}
