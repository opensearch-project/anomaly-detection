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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.e2e;

import static org.opensearch.ad.TestHelpers.toHttpEntity;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BACKOFF_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.opensearch.ad.ODFERestTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.WarningsHandler;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.json.JsonXContent;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class DetectionResultEvalutationIT extends ODFERestTestCase {
    protected static final Logger LOG = (Logger) LogManager.getLogger(DetectionResultEvalutationIT.class);

    public void testDataset() throws Exception {
        // TODO: this test case will run for a much longer time and timeout with security enabled
        if (!isHttps()) {
            disableResourceNotFoundFaultTolerence();
            verifyAnomaly("synthetic", 1, 1500, 8, .5, .9, 10);
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

        String dataFileName = String.format("data/%s.data", datasetName);
        String labelFileName = String.format("data/%s.label", datasetName);

        List<JsonObject> data = getData(dataFileName);
        List<Entry<Instant, Instant>> anomalies = getAnomalyWindows(labelFileName);

        bulkIndexTrainData(datasetName, data, trainTestSplit, client);
        String detectorId = createDetector(datasetName, intervalMinutes, client);
        startDetector(detectorId, data, trainTestSplit, shingleSize, intervalMinutes, client);
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
        LOG.info("Verifying test results: " + Arrays.toString(testResults));
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
            } catch (Exception e) {
                errors++;
                e.printStackTrace();
            }
        }
        Thread.sleep(5_000);

        Request request = new Request("POST", "/_plugins/_anomaly_detection/detectors/results/_search");
        request
            .setJsonEntity(
                String
                    .format(
                        Locale.ROOT,
                        "{ \"query\": { \"bool\": { \"filter\": [ { \"term\": { \"detector_id\": \"%s\"} },"
                            + "{ \"range\": { \"anomaly_grade\": { \"gt\": 0} } } ] } }, \"size\": 10000 }",
                        detectorId
                    )
            );
        Response response = client.performRequest(request);
        JsonObject json = new JsonParser().parse(new InputStreamReader(response.getEntity().getContent())).getAsJsonObject();
        for (JsonElement e : json.getAsJsonObject("hits").getAsJsonArray("hits")) {
            JsonObject object = e.getAsJsonObject();
            positives++;
            int result = isAnomaly(
                Instant.ofEpochMilli(e.getAsJsonObject().getAsJsonObject("_source").getAsJsonPrimitive("data_start_time").getAsLong()),
                anomalies
            );
            if (result != -1) {
                truePositives++;
                positiveAnomalies.add(result);
            }
        }

        return new double[] { positives, truePositives, positiveAnomalies.size(), errors };
    }

    private void startDetector(
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

    private String createDetector(String datasetName, int intervalMinutes, RestClient client) throws Exception {
        Request request = new Request("POST", "/_opendistro/_anomaly_detection/detectors/");
        String requestBody = String
            .format(
                Locale.ROOT,
                "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"Feature1\" } } } }, { \"feature_name\""
                    + ": \"feature 2\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"Feature2\": { \"sum\": { \"field\": "
                    + "\"Feature2\" } } } }], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                    + "\"schema_version\": 0 }",
                datasetName,
                intervalMinutes
            );
        request.setJsonEntity(requestBody);
        Map<String, Object> response = entityAsMap(client.performRequest(request));
        String detectorId = (String) response.get("_id");
        Thread.sleep(1_000);
        return detectorId;
    }

    private List<Entry<Instant, Instant>> getAnomalyWindows(String labalFileName) throws Exception {
        JsonArray windows = new JsonParser()
            .parse(new FileReader(new File(getClass().getResource(labalFileName).toURI())))
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

    private void bulkIndexTrainData(String datasetName, List<JsonObject> data, int trainTestSplit, RestClient client) throws Exception {
        Request request = new Request("PUT", datasetName);
        String requestBody = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
            + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" } } } }";
        request.setJsonEntity(requestBody);
        setWarningHandler(request, false);
        client.performRequest(request);
        Thread.sleep(1_000);

        StringBuilder bulkRequestBuilder = new StringBuilder();
        for (int i = 0; i < trainTestSplit; i++) {
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
    }

    private void setWarningHandler(Request request, boolean strictDeprecationMode) {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.setWarningsHandler(strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());
    }

    private List<JsonObject> getData(String datasetFileName) throws Exception {
        JsonArray jsonArray = new JsonParser()
            .parse(new FileReader(new File(getClass().getResource(datasetFileName).toURI())))
            .getAsJsonArray();
        List<JsonObject> list = new ArrayList<>(jsonArray.size());
        jsonArray.iterator().forEachRemaining(i -> list.add(i.getAsJsonObject()));
        return list;
    }

    private Map<String, Object> getDetectionResult(String detectorId, Instant begin, Instant end, RestClient client) {
        try {
            Request request = new Request("POST", String.format("/_opendistro/_anomaly_detection/detectors/%s/_run", detectorId));
            request
                .setJsonEntity(
                    String.format(Locale.ROOT, "{ \"period_start\": %d, \"period_end\": %d }", begin.toEpochMilli(), end.toEpochMilli())
                );
            return entityAsMap(client.performRequest(request));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * In real time AD, we mute a node for a detector if that node keeps returning
     * ResourceNotFoundException (5 times in a row).  This is a problem for batch mode
     * testing as we issue a large amount of requests quickly. Due to the speed, we
     * won't be able to finish cold start before the ResourceNotFoundException mutes
     * a node.  Since our test case has only one node, there is no other nodes to fall
     * back on.  Here we disable such fault tolerance by setting max retries before
     * muting to a large number and the actual wait time during muting to 0.
     *
     * @throws IOException when failing to create http request body
     */
    private void disableResourceNotFoundFaultTolerence() throws IOException {
        XContentBuilder settingCommand = JsonXContent.contentBuilder();

        settingCommand.startObject();
        settingCommand.startObject("persistent");
        settingCommand.field(MAX_RETRY_FOR_UNRESPONSIVE_NODE.getKey(), 100_000);
        settingCommand.field(BACKOFF_MINUTES.getKey(), 0);
        settingCommand.endObject();
        settingCommand.endObject();
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(Strings.toString(settingCommand));

        adminClient().performRequest(request);
    }
}
