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

import static org.opensearch.ad.TestHelpers.toHttpEntity;

import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.opensearch.ad.TestHelpers;
import org.opensearch.client.RestClient;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class SingleStreamModelPerfIT extends AbstractSyntheticDataTest {
    protected static final Logger LOG = (Logger) LogManager.getLogger(SingleStreamModelPerfIT.class);

    public void testDataset() throws Exception {
        // TODO: this test case will run for a much longer time and timeout with security enabled
        if (!isHttps()) {
            disableResourceNotFoundFaultTolerence();
            verifyAnomaly("synthetic", 1, 1500, 8, .4, .9, 10);
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

        bulkIndexTrainData(datasetName, data, trainTestSplit, client, null);
        // single-stream detector can use window delay 0 here because we give the run api the actual data time
        String detectorId = createDetector(datasetName, intervalMinutes, client, null, 0);
        simulateSingleStreamStartDetector(detectorId, data, trainTestSplit, shingleSize, intervalMinutes, client);
        bulkIndexTestData(data, datasetName, trainTestSplit, client);
        double[] testResults = getTestResults(detectorId, data, trainTestSplit, intervalMinutes, anomalies, client);
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
        double precision = positives > 0 ? truePositives / positives : 1;
        assertTrue(precision >= minPrecision);

        // recall = windows containing predicted anomaly points / total anomaly windows
        double recall = anomalies.size() > 0 ? positiveAnomalies / anomalies.size() : 1;
        assertTrue(recall >= minRecall);

        assertTrue(errors <= maxError);
        LOG.info("Precision: {}, Window recall: {}", precision, recall);
    }

    private double[] getTestResults(
        String detectorId,
        List<JsonObject> data,
        int trainTestSplit,
        int intervalMinutes,
        List<Entry<Instant, Instant>> anomalies,
        RestClient client
    ) throws Exception {

        double positives = 0;
        double truePositives = 0;
        Set<Integer> positiveAnomalies = new HashSet<>();
        double errors = 0;
        for (int i = trainTestSplit; i < data.size(); i++) {
            Instant begin = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(data.get(i).get("timestamp").getAsString()));
            Instant end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
            try {
                Map<String, Object> response = getDetectionResult(detectorId, begin, end, client);
                double anomalyGrade = (double) response.get("anomalyGrade");
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
                logger.error("failed to get detection results", e);
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

    /**
     * Simulate starting detector without waiting for job scheduler to run. Our build process is already very slow (takes 10 mins+)
     * to finish integration tests. This method triggers run API to simulate job scheduler execution in a fast-paced way.
     * @param detectorId Detector Id
     * @param data Data in Json format
     * @param trainTestSplit Training data size
     * @param shingleSize Shingle size
     * @param intervalMinutes Detector Interval
     * @param client OpenSearch Client
     * @throws Exception when failing to query/indexing from/to OpenSearch
     */
    private void simulateSingleStreamStartDetector(
        String detectorId,
        List<JsonObject> data,
        int trainTestSplit,
        int shingleSize,
        int intervalMinutes,
        RestClient client
    ) throws Exception {

        Instant trainTime = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(data.get(trainTestSplit - 1).get("timestamp").getAsString()));

        Instant begin = null;
        Instant end = null;
        for (int i = 0; i < shingleSize; i++) {
            begin = trainTime.minus(intervalMinutes * (shingleSize - 1 - i), ChronoUnit.MINUTES);
            end = begin.plus(intervalMinutes, ChronoUnit.MINUTES);
            try {
                getDetectionResult(detectorId, begin, end, client);
            } catch (Exception e) {}
        }
        // It takes time to wait for model initialization
        long startTime = System.currentTimeMillis();
        do {
            try {
                Thread.sleep(5_000);
                getDetectionResult(detectorId, begin, end, client);
                break;
            } catch (Exception e) {
                long duration = System.currentTimeMillis() - startTime;
                // we wait at most 60 secs
                if (duration > 60_000) {
                    throw new RuntimeException(e);
                }
            }
        } while (true);
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

    private int isAnomaly(Instant time, List<Entry<Instant, Instant>> labels) {
        for (int i = 0; i < labels.size(); i++) {
            Entry<Instant, Instant> window = labels.get(i);
            if (time.compareTo(window.getKey()) >= 0 && time.compareTo(window.getValue()) <= 0) {
                return i;
            }
        }
        return -1;
    }
}
