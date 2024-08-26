/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.ad.e2e;

import static org.opensearch.timeseries.TestHelpers.toHttpEntity;

import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.opensearch.ad.AbstractADSyntheticDataTest;
import org.opensearch.client.RestClient;
import org.opensearch.timeseries.TestHelpers;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SingleStreamModelPerfIT extends AbstractADSyntheticDataTest {
    protected static final Logger LOG = (Logger) LogManager.getLogger(SingleStreamModelPerfIT.class);

    public void testDataset() throws Exception {
        // TODO: this test case will run for a much longer time and timeout with security enabled
        if (!isHttps()) {
            disableResourceNotFoundFaultTolerence();
            verifyAnomaly("synthetic", 1, 1500, 8, .4, .7, 10);
        }
    }

    private void verifyAnomaly(
        String datasetName,
        int intervalMinutes,
        int trainTestSplit,
        int shingleSize,
        double minPrecision,
        double minRecall,
        double maxError
    ) throws Exception {
        RestClient client = client();

        String dataFileName = String.format(Locale.ROOT, "data/%s.data", datasetName);
        String labelFileName = String.format(Locale.ROOT, "data/%s.label", datasetName);

        List<JsonObject> data = getData(dataFileName);
        List<Entry<Instant, Instant>> anomalies = getAnomalyWindows(labelFileName);

        String mapping = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
            + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" } } } }";
        bulkIndexTrainData(datasetName, data, trainTestSplit, client, mapping);

        long windowDelayMinutes = getWindowDelayMinutes(data, trainTestSplit - 1, "timestamp");
        Duration windowDelay = Duration.ofMinutes(windowDelayMinutes);

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
        String detectorId = createDetector(client, detector);

        Instant dataStartTime = Instant
            .from(DateTimeFormatter.ISO_INSTANT.parse(data.get(trainTestSplit - 1).get("timestamp").getAsString()));
        Instant dataEndTime = dataStartTime.plus(intervalMinutes, ChronoUnit.MINUTES);
        Instant trainTime = dataToExecutionTime(dataStartTime, windowDelay);
        Instant executionStartTime = trainTime;
        Instant executionEndTime = executionStartTime.plus(intervalMinutes, ChronoUnit.MINUTES);

        simulateStartDetector(detectorId, executionStartTime, executionEndTime, client, 1);
        simulateWaitForInitDetector(detectorId, client, dataEndTime, 1);
        bulkIndexTestData(data, datasetName, trainTestSplit, client);
        double[] testResults = getTestResults(detectorId, data, trainTestSplit, intervalMinutes, anomalies, client, 1, windowDelay);
        verifyTestResults(testResults, anomalies, minPrecision, minRecall, maxError);
    }

    private void verifyTestResults(
        double[] testResults,
        List<Entry<Instant, Instant>> anomalies,
        double minPrecision,
        double minRecall,
        double maxError
    ) {

        double positives = testResults[0];
        double truePositives = testResults[1];
        double positiveAnomalies = testResults[2];
        double errors = testResults[3];

        // precision = predicted anomaly points that are true / predicted anomaly points
        double precision = positives > 0 ? truePositives / positives : 0;
        assertTrue(
            String.format(Locale.ROOT, "precision expected at least %f but got %f", minPrecision, precision),
            precision >= minPrecision
        );

        // recall = windows containing predicted anomaly points / total anomaly windows
        double recall = anomalies.size() > 0 ? positiveAnomalies / anomalies.size() : 0;
        assertTrue(String.format(Locale.ROOT, "recall should be at least %f but got %f", recall, minRecall), recall >= minRecall);

        assertTrue(errors <= maxError);
        LOG.info("Precision: {}, Window recall: {}", precision, recall);
    }

    private double[] getTestResults(
        String detectorId,
        List<JsonObject> data,
        int trainTestSplit,
        int intervalMinutes,
        List<Entry<Instant, Instant>> anomalies,
        RestClient client,
        int entitySize,
        Duration windowDelay
    ) throws Exception {

        int errors = 0;
        for (int i = trainTestSplit; i < data.size(); i++) {
            Instant begin = dataToExecutionTime(
                Instant.from(DateTimeFormatter.ISO_INSTANT.parse(data.get(i).get("timestamp").getAsString())),
                windowDelay
            );
            Instant end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
            try {
                runDetectionResult(detectorId, begin, end, client, entitySize);
            } catch (Exception e) {
                errors++;
                LOG.error("failed to run detection result", e);
            }
        }

        double positives = 0;
        double truePositives = 0;
        Set<Integer> positiveAnomalies = new HashSet<>();

        for (int i = trainTestSplit; i < data.size(); i++) {
            Instant begin = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(data.get(i).get("timestamp").getAsString()));
            Instant end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
            try {
                List<JsonObject> sourceList = getRealTimeAnomalyResult(detectorId, end, 1, client);
                assertTrue("expect 1 result, but got " + sourceList.size(), sourceList.size() == 1);
                double anomalyGrade = getAnomalyGrade(sourceList.get(0));
                assertTrue("anomalyGrade cannot be negative", anomalyGrade >= 0);
                if (anomalyGrade > 0) {
                    positives++;
                    int result = isAnomaly(begin, anomalies);
                    if (result != -1) {
                        truePositives++;
                        positiveAnomalies.add(result);
                    }
                }
            } catch (Exception e) {
                errors++;
                LOG.error("failed to get detection results", e);
            }
        }
        return new double[] { positives, truePositives, positiveAnomalies.size(), errors };
    }

    private List<Entry<Instant, Instant>> getAnomalyWindows(String labalFileName) throws Exception {
        JsonArray windows = JsonParser
            .parseReader(new FileReader(new File(getClass().getResource(labalFileName).toURI()), Charset.defaultCharset()))
            .getAsJsonArray();
        List<Entry<Instant, Instant>> anomalies = new ArrayList<>(windows.size());
        for (int i = 0; i < windows.size(); i++) {
            JsonArray window = windows.get(i).getAsJsonArray();
            Instant begin = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(window.get(0).getAsString()));
            Instant end = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(window.get(1).getAsString()));
            anomalies.add(new SimpleEntry<>(begin, end));
        }
        return anomalies;
    }

    private void bulkIndexTestData(List<JsonObject> data, String datasetName, int trainTestSplit, RestClient client) throws Exception {
        StringBuilder bulkRequestBuilder = new StringBuilder();
        for (int i = trainTestSplit; i < data.size(); i++) {
            bulkRequestBuilder.append("{ \"index\" : { \"_index\" : \"" + datasetName + "\", \"_id\" : \"" + i + "\" } }\n");
            bulkRequestBuilder.append(data.get(i).toString()).append("\n");
        }
        TestHelpers
            .makeRequest(
                client,
                "POST",
                "_bulk?refresh=true",
                null,
                toHttpEntity(bulkRequestBuilder.toString()),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
        Thread.sleep(1_000);
        waitAllSyncheticDataIngested(data.size(), datasetName, client);
    }
}
