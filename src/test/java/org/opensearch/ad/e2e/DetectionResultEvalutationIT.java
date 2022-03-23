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
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BACKOFF_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.opensearch.ad.ODFERestTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.WarningsHandler;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.support.XContentMapValues;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

public class DetectionResultEvalutationIT extends ODFERestTestCase {
    protected static final Logger LOG = (Logger) LogManager.getLogger(DetectionResultEvalutationIT.class);

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

        String dataFileName = String.format("data/%s.data", datasetName);
        String labelFileName = String.format("data/%s.label", datasetName);

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
                e.printStackTrace();
            }
        }
        return new double[] { positives, truePositives, positiveAnomalies.size(), errors };
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

    /**
     * Simulate starting the given HCAD detector.
     * @param detectorId Detector Id
     * @param data Data in Json format
     * @param trainTestSplit Training data size
     * @param shingleSize Shingle size
     * @param intervalMinutes Detector Interval
     * @param client OpenSearch Client
     * @throws Exception when failing to query/indexing from/to OpenSearch
     */
    private void simulateHCADStartDetector(
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
        long duration = 0;
        do {
            /*
             * single stream detectors will throw exception if not finding models in the
             * callback, while HCAD detectors will return early, record the exception in
             * node state, and throw exception in the next run. HCAD did it this way since
             * it does not know when current run is gonna finish (e.g, we may have millions
             * of entities to process in one run). So for single-stream detector test case,
             * we can check the exception to see if models are initialized or not. So HCAD,
             * we have to either wait for next runs or use profile API. Here I chose profile
             * API since it is faster. Will add these explanation in the comments.
             */
            Thread.sleep(5_000);
            String initProgress = profileDetectorInitProgress(detectorId, client);
            if (initProgress.equals("100%")) {
                break;
            }
            try {
                getDetectionResult(detectorId, begin, end, client);
            } catch (Exception e) {}
            duration = System.currentTimeMillis() - startTime;
        } while (duration <= 60_000);
    }

    private String createDetector(String datasetName, int intervalMinutes, RestClient client, String categoryField, long windowDelayInMins)
        throws Exception {
        Request request = new Request("POST", "/_plugins/_anomaly_detection/detectors/");
        String requestBody = null;
        if (Strings.isEmpty(categoryField)) {
            requestBody = String
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
                    windowDelayInMins
                );
        } else {
            requestBody = String
                .format(
                    Locale.ROOT,
                    "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                        + ", \"indices\": [\"%s\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                        + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"Feature1\" } } } }, { \"feature_name\""
                        + ": \"feature 2\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"Feature2\": { \"sum\": { \"field\": "
                        + "\"Feature2\" } } } }], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }, "
                        + "\"category_field\": [\"%s\"], "
                        + "\"window_delay\": { \"period\": {\"interval\": %d, \"unit\": \"MINUTES\"}},"
                        + "\"schema_version\": 0  }",
                    datasetName,
                    intervalMinutes,
                    categoryField,
                    windowDelayInMins
                );
        }

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

    private void bulkIndexTrainData(String datasetName, List<JsonObject> data, int trainTestSplit, RestClient client, String categoryField)
        throws Exception {
        Request request = new Request("PUT", datasetName);
        String requestBody = null;
        if (Strings.isEmpty(categoryField)) {
            requestBody = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
                + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" } } } }";
        } else {
            requestBody = String
                .format(
                    Locale.ROOT,
                    "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
                        + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" },"
                        + "\"%s\": { \"type\": \"keyword\"} } } }",
                    categoryField
                );
        }

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
        waitAllSyncheticDataIngested(trainTestSplit, datasetName, client);
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

    private void waitAllSyncheticDataIngested(int expectedSize, String datasetName, RestClient client) throws Exception {
        int maxWaitCycles = 3;
        do {
            Request request = new Request("POST", String.format(Locale.ROOT, "/%s/_search", datasetName));
            request
                .setJsonEntity(
                    String
                        .format(
                            Locale.ROOT,
                            "{\"query\": {"
                                + "        \"match_all\": {}"
                                + "    },"
                                + "    \"size\": 1,"
                                + "    \"sort\": ["
                                + "       {"
                                + "         \"timestamp\": {"
                                + "           \"order\": \"desc\""
                                + "         }"
                                + "       }"
                                + "   ]}"
                        )
                );
            // Make sure all of the test data has been ingested
            // Expected response:
            // "_index":"synthetic","_type":"_doc","_id":"10080","_score":null,"_source":{"timestamp":"2019-11-08T00:00:00Z","Feature1":156.30028000000001,"Feature2":100.211205,"host":"host1"},"sort":[1573171200000]}
            Response response = client.performRequest(request);
            JsonObject json = new JsonParser().parse(new InputStreamReader(response.getEntity().getContent())).getAsJsonObject();
            JsonArray hits = json.getAsJsonObject("hits").getAsJsonArray("hits");
            if (hits != null
                && hits.size() == 1
                && expectedSize - 1 == hits.get(0).getAsJsonObject().getAsJsonPrimitive("_id").getAsLong()) {
                break;
            } else {
                request = new Request("POST", String.format(Locale.ROOT, "/%s/_refresh", datasetName));
                client.performRequest(request);
            }
        } while (maxWaitCycles-- >= 0);
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

    public void testValidationIntervalRecommendation() throws Exception {
        RestClient client = client();
        long recDetectorIntervalMillis = 180000;
        long recDetectorIntervalMinutes = recDetectorIntervalMillis / 60000;
        List<JsonObject> data = createData(2000, recDetectorIntervalMillis);
        indexTrainData("validation", data, 2000, client);
        long detectorInterval = 1;
        String requestBody = String
            .format(
                Locale.ROOT,
                "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"validation\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"Feature1\" } } } }, { \"feature_name\""
                    + ": \"feature 2\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"Feature2\": { \"sum\": { \"field\": "
                    + "\"Feature2\" } } } }], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }"
                    + ",\"window_delay\":{\"period\":{\"interval\":10,\"unit\":\"Minutes\"}}}",
                detectorInterval
            );
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate/model",
                ImmutableMap.of(),
                toHttpEntity(requestBody),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> messageMap = (Map<String, Map<String, String>>) XContentMapValues
            .extractValue("model", responseMap);
        assertEquals(
            CommonErrorMessages.DETECTOR_INTERVAL_REC + recDetectorIntervalMinutes,
            messageMap.get("detection_interval").get("message")
        );
    }

    public void testValidationWindowDelayRecommendation() throws Exception {
        RestClient client = client();
        long recDetectorIntervalMillis = 180000;
        // this would be equivalent to the window delay in this data test
        long recDetectorIntervalMinutes = recDetectorIntervalMillis / 60000;
        List<JsonObject> data = createData(2000, recDetectorIntervalMillis);
        indexTrainData("validation", data, 2000, client);
        long detectorInterval = 4;
        String requestBody = String
            .format(
                Locale.ROOT,
                "{ \"name\": \"test\", \"description\": \"test\", \"time_field\": \"timestamp\""
                    + ", \"indices\": [\"validation\"], \"feature_attributes\": [{ \"feature_name\": \"feature 1\", \"feature_enabled\": "
                    + "\"true\", \"aggregation_query\": { \"Feature1\": { \"sum\": { \"field\": \"Feature1\" } } } }, { \"feature_name\""
                    + ": \"feature 2\", \"feature_enabled\": \"true\", \"aggregation_query\": { \"Feature2\": { \"sum\": { \"field\": "
                    + "\"Feature2\" } } } }], \"detection_interval\": { \"period\": { \"interval\": %d, \"unit\": \"Minutes\" } }"
                    + ",\"window_delay\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}}}",
                detectorInterval
            );
        Response resp = TestHelpers
            .makeRequest(
                client(),
                "POST",
                TestHelpers.AD_BASE_DETECTORS_URI + "/_validate/model",
                ImmutableMap.of(),
                toHttpEntity(requestBody),
                null
            );
        Map<String, Object> responseMap = entityAsMap(resp);
        @SuppressWarnings("unchecked")
        Map<String, Map<String, String>> messageMap = (Map<String, Map<String, String>>) XContentMapValues
            .extractValue("model", responseMap);
        // adding plus one since window delay always rounds up another minute
        assertEquals(
            String
                .format(Locale.ROOT, CommonErrorMessages.WINDOW_DELAY_REC, +recDetectorIntervalMinutes + 1, recDetectorIntervalMinutes + 1),
            messageMap.get("window_delay").get("message")
        );
    }

    private List<JsonObject> createData(int numOfDataPoints, long detectorIntervalMS) {
        List<JsonObject> list = new ArrayList<>();
        for (int i = 1; i < numOfDataPoints; i++) {
            long valueFeature1 = randomLongBetween(1, 10000000);
            long valueFeature2 = randomLongBetween(1, 10000000);
            JsonObject obj = new JsonObject();
            JsonElement element = new JsonPrimitive(Instant.now().toEpochMilli() - (detectorIntervalMS * i));
            obj.add("timestamp", element);
            obj.add("Feature1", new JsonPrimitive(valueFeature1));
            obj.add("Feature2", new JsonPrimitive(valueFeature2));
            list.add(obj);
        }
        return list;
    }

    private void indexTrainData(String datasetName, List<JsonObject> data, int trainTestSplit, RestClient client) throws Exception {
        Request request = new Request("PUT", datasetName);
        String requestBody = "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
            + " \"Feature1\": { \"type\": \"long\" }, \"Feature2\": { \"type\": \"long\" } } } }";
        request.setJsonEntity(requestBody);
        client.performRequest(request);
        Thread.sleep(1_000);
        data.stream().limit(trainTestSplit).forEach(r -> {
            try {
                Request req = new Request("POST", String.format("/%s/_doc/", datasetName));
                req.setJsonEntity(r.toString());
                client.performRequest(req);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(1_000);
    }

    public void testRestartHCADDetector() throws Exception {
        // TODO: this test case will run for a much longer time and timeout with security enabled
        if (!isHttps()) {
            int maxRetries = 3;
            int i = 0;
            for (; i < maxRetries; i++) {
                try {
                    disableResourceNotFoundFaultTolerence();
                    verifyRestart("synthetic", 1, 8);
                    break;
                } catch (Throwable throwable) {
                    LOG.info("Retry restart test case", throwable);
                    cleanUpCluster();
                    wipeAllODFEIndices();
                }
            }
            assertTrue("failed all retries", i < maxRetries);
        }
    }

    private void verifyRestart(String datasetName, int intervalMinutes, int shingleSize) throws Exception {
        RestClient client = client();

        String dataFileName = String.format("data/%s.data", datasetName);

        List<JsonObject> data = getData(dataFileName);

        String categoricalField = "host";
        String tsField = "timestamp";

        Clock clock = Clock.systemUTC();
        long currentMilli = clock.millis();
        int trainTestSplit = 1500;

        // e.g., 2019-11-01T00:03:00Z
        String pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        // calculate the gap between current time and the beginning of last shingle
        // the gap is used to adjust input training data's time so that the last
        // few items of training data maps to current time. We need this adjustment
        // because CompositeRetriever will compare expiry time with current time in hasNext
        // method. The expiry time is calculated using request (one parameter of the run API)
        // end time plus some fraction of interval. If the expiry time is less than
        // current time, CompositeRetriever thinks this request expires and refuses to start
        // querying. So this adjustment is to make the following simulateHCADStartDetector work.
        String lastTrainShingleStartTime = data.get(trainTestSplit - shingleSize).getAsJsonPrimitive(tsField).getAsString();
        Date date = simpleDateFormat.parse(lastTrainShingleStartTime);
        long diff = currentMilli - date.getTime();
        TimeUnit time = TimeUnit.MINUTES;
        // by the time we trigger the run API, a few seconds have passed. +5 to make the adjusted time more than current time.
        long gap = time.convert(diff, TimeUnit.MILLISECONDS) + 5;

        Calendar c = Calendar.getInstance();
        c.setTimeZone(TimeZone.getTimeZone("UTC"));

        // only change training data as we only need to make sure detector is fully initialized
        for (int i = 0; i < trainTestSplit; i++) {
            JsonObject row = data.get(i);
            // add categorical field since the original data is for single-stream detectors
            row.addProperty(categoricalField, "host1");

            String dateString = row.getAsJsonPrimitive(tsField).getAsString();
            date = simpleDateFormat.parse(dateString);
            c.setTime(date);
            c.add(Calendar.MINUTE, (int) gap);
            String adjustedDate = simpleDateFormat.format(c.getTime());
            row.addProperty(tsField, adjustedDate);
        }

        bulkIndexTrainData(datasetName, data, trainTestSplit, client, categoricalField);

        String detectorId = createDetector(datasetName, intervalMinutes, client, categoricalField, 0);
        // cannot stop without actually starting detector because ad complains no ad job index
        startDetector(detectorId, client);
        // it would be long if we wait for the job actually run the work periodically; speed it up by using simulateHCADStartDetector
        simulateHCADStartDetector(detectorId, data, trainTestSplit, shingleSize, intervalMinutes, client);
        String initProgress = profileDetectorInitProgress(detectorId, client);
        assertEquals("init progress is " + initProgress, "100%", initProgress);
        stopDetector(detectorId, client);
        // restart detector
        startDetector(detectorId, client);
        simulateHCADStartDetector(detectorId, data, trainTestSplit, shingleSize, intervalMinutes, client);
        initProgress = profileDetectorInitProgress(detectorId, client);
        assertEquals("init progress is " + initProgress, "100%", initProgress);
    }

    private void stopDetector(String detectorId, RestClient client) throws Exception {
        Request request = new Request("POST", String.format(Locale.ROOT, "/_plugins/_anomaly_detection/detectors/%s/_stop", detectorId));

        Map<String, Object> response = entityAsMap(client.performRequest(request));
        String responseDetectorId = (String) response.get("_id");
        assertEquals(detectorId, responseDetectorId);
    }

    private void startDetector(String detectorId, RestClient client) throws Exception {
        Request request = new Request("POST", String.format(Locale.ROOT, "/_plugins/_anomaly_detection/detectors/%s/_start", detectorId));

        Map<String, Object> response = entityAsMap(client.performRequest(request));
        String responseDetectorId = (String) response.get("_id");
        assertEquals(detectorId, responseDetectorId);
    }

    private String profileDetectorInitProgress(String detectorId, RestClient client) throws Exception {
        Request request = new Request(
            "GET",
            String.format(Locale.ROOT, "/_plugins/_anomaly_detection/detectors/%s/_profile/init_progress", detectorId)
        );

        Map<String, Object> response = entityAsMap(client.performRequest(request));
        /*
         * Example response:
         * {
         *   "init_progress": {
         *      "percentage": "100%"
         *    }
         *   }
         */
        return (String) ((Map<String, Object>) response.get("init_progress")).get("percentage");
    }
}
