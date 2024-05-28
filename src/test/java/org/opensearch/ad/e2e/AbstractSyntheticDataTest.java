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
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.opensearch.ad.ODFERestTestCase;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.WarningsHandler;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AbstractSyntheticDataTest extends ODFERestTestCase {
    protected static final Logger LOG = (Logger) LogManager.getLogger(AbstractSyntheticDataTest.class);

    private static int batchSize = 1000;

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
    protected void disableResourceNotFoundFaultTolerence() throws IOException {
        XContentBuilder settingCommand = JsonXContent.contentBuilder();

        settingCommand.startObject();
        settingCommand.startObject("persistent");
        settingCommand.field(TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE.getKey(), 100_000);
        settingCommand.field(TimeSeriesSettings.BACKOFF_MINUTES.getKey(), 0);
        settingCommand.endObject();
        settingCommand.endObject();
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(settingCommand.toString());

        adminClient().performRequest(request);
    }

    protected List<JsonObject> getData(String datasetFileName) throws Exception {
        JsonArray jsonArray = JsonParser
            .parseReader(new FileReader(new File(getClass().getResource(datasetFileName).toURI()), Charset.defaultCharset()))
            .getAsJsonArray();
        List<JsonObject> list = new ArrayList<>(jsonArray.size());
        jsonArray.iterator().forEachRemaining(i -> list.add(i.getAsJsonObject()));
        return list;
    }

    protected JsonArray getHits(RestClient client, Request request) throws IOException {
        Response response = client.performRequest(request);
        return parseHits(response);
    }

    protected JsonArray parseHits(Response response) throws IOException {
        JsonObject json = JsonParser
            .parseReader(new InputStreamReader(response.getEntity().getContent(), Charset.defaultCharset()))
            .getAsJsonObject();
        JsonObject hits = json.getAsJsonObject("hits");
        if (hits == null) {
            return null;
        }
        return hits.getAsJsonArray("hits");
    }

    protected void runDetectionResult(String detectorId, Instant begin, Instant end, RestClient client, int entitySize) throws IOException,
        InterruptedException {
        // trigger run in current interval
        Request request = new Request("POST", String.format(Locale.ROOT, "/_opendistro/_anomaly_detection/detectors/%s/_run", detectorId));
        request
            .setJsonEntity(
                String.format(Locale.ROOT, "{ \"period_start\": %d, \"period_end\": %d }", begin.toEpochMilli(), end.toEpochMilli())
            );
        int statusCode = client.performRequest(request).getStatusLine().getStatusCode();
        assert (statusCode >= 200 && statusCode < 300);

        // wait for 50 milliseconds per entity before next query
        Thread.sleep(50 * entitySize);
    }

    protected List<JsonObject> getAnomalyResult(String detectorId, Instant end, int entitySize, RestClient client) {
        try {
            Request request = new Request("POST", "/_plugins/_anomaly_detection/detectors/results/_search");

            String jsonTemplate = "{\n"
                + "    \"query\": {\n"
                + "        \"bool\": {\n"
                + "            \"filter\": [\n"
                + "                {\n"
                + "                    \"term\": {\n"
                + "                        \"detector_id\": \"%s\"\n"
                + "                    }\n"
                + "                },\n"
                + "                {\n"
                + "                    \"range\": {\n"
                + "                        \"anomaly_grade\": {\n"
                + "                            \"gte\": 0\n"
                + "                        }\n"
                + "                    }\n"
                + "                },\n"
                + "                {\n"
                + "                    \"range\": {\n"
                + "                        \"data_end_time\": {\n"
                + "                            \"gte\": %d,\n"
                + "                            \"lte\": %d\n"
                + "                        }\n"
                + "                    }\n"
                + "                }\n"
                + "            ]\n"
                + "        }\n"
                + "    }\n"
                + "}";

            long dateEndTime = end.toEpochMilli();
            String formattedJson = String.format(Locale.ROOT, jsonTemplate, detectorId, dateEndTime, dateEndTime);
            request.setJsonEntity(formattedJson);

            // wait until results are available
            // max wait for 60_000 milliseconds
            int maxWaitCycles = 30;
            do {
                JsonArray hits = getHits(client, request);
                if (hits != null && hits.size() == entitySize) {
                    assertTrue("empty response", hits != null);
                    assertTrue("returned more than " + hits.size() + " results.", hits.size() == entitySize);
                    List<JsonObject> res = new ArrayList<>();
                    for (int i = 0; i < entitySize; i++) {
                        JsonObject source = hits.get(i).getAsJsonObject().get("_source").getAsJsonObject();
                        res.add(source);
                    }

                    return res;
                } else {
                    LOG.info("wait for result, previous result: {}", hits);
                    client.performRequest(new Request("POST", String.format(Locale.ROOT, "/%s/_refresh", ".opendistro-anomaly-results*")));
                }
                Thread.sleep(2_000 * entitySize);
            } while (maxWaitCycles-- >= 0);

            return new ArrayList<>();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected double getAnomalyGrade(JsonObject source) {
        return source.get("anomaly_grade").getAsDouble();
    }

    protected String getEntity(JsonObject source) {
        return source.get("entity").getAsJsonArray().get(0).getAsJsonObject().get("value").getAsString();
    }

    /**
     * We can detect anomaly late. If yes, use approx_anomaly_start_time; otherwise, use defaultVal.
     * @param source source response containing anomaly result.
     * @param defaultVal default anomaly time. Usually data end time.
     * @return anomaly event time.
     */
    protected Instant getAnomalyTime(JsonObject source, Instant defaultVal) {
        JsonElement anomalyTime = source.get("approx_anomaly_start_time");
        if (anomalyTime != null) {
            long epochhMillis = anomalyTime.getAsLong();
            return Instant.ofEpochMilli(epochhMillis);
        }
        return defaultVal;
    }

    protected void createIndex(String datasetName, RestClient client, String mapping) throws IOException, InterruptedException {
        Request request = new Request("PUT", datasetName);
        request.setJsonEntity(mapping);
        setWarningHandler(request, false);
        client.performRequest(request);
        Thread.sleep(1_000);
    }

    protected void bulkIndexTrainData(String datasetName, List<JsonObject> data, int trainTestSplit, RestClient client, String mapping)
        throws Exception {
        createIndex(datasetName, client, mapping);

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

    protected String createDetector(RestClient client, String detectorJson) throws Exception {
        Request request = new Request("POST", "/_plugins/_anomaly_detection/detectors/");

        request.setJsonEntity(detectorJson);
        Map<String, Object> response = entityAsMap(client.performRequest(request));
        String detectorId = (String) response.get("_id");
        Thread.sleep(1_000);
        return detectorId;
    }

    @Override
    protected void waitAllSyncheticDataIngested(int expectedSize, String datasetName, RestClient client) throws Exception {
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
            JsonArray hits = getHits(client, request);
            LOG.info("Latest synthetic data:" + hits);
            if (hits != null
                && hits.size() == 1
                && expectedSize - 1 == hits.get(0).getAsJsonObject().getAsJsonPrimitive("_id").getAsLong()) {
                break;
            } else {
                request = new Request("POST", String.format(Locale.ROOT, "/%s/_refresh", datasetName));
                client.performRequest(request);
            }
            Thread.sleep(1_000);
        } while (maxWaitCycles-- >= 0);
    }

    protected void setWarningHandler(Request request, boolean strictDeprecationMode) {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.setWarningsHandler(strictDeprecationMode ? WarningsHandler.STRICT : WarningsHandler.PERMISSIVE);
        request.setOptions(options.build());
    }

    protected void startDetector(String detectorId, RestClient client) throws Exception {
        Request request = new Request("POST", String.format(Locale.ROOT, "/_plugins/_anomaly_detection/detectors/%s/_start", detectorId));

        Map<String, Object> response = entityAsMap(client.performRequest(request));
        String responseDetectorId = (String) response.get("_id");
        assertEquals(detectorId, responseDetectorId);
    }

    protected String profileDetectorInitProgress(String detectorId, RestClient client) throws Exception {
        Request request = new Request(
            "GET",
            String.format(Locale.ROOT, "/_plugins/_anomaly_detection/detectors/%s/_profile/init_progress", detectorId)
        );

        Map<String, Object> response = entityAsMap(client.performRequest(request));
        LOG.info("profile response: {}", response);

        Object initProgress = response.get("init_progress");
        if (initProgress == null) {
            return "0%";
        }

        Object percent = ((Map<String, Object>) initProgress).get("percentage");

        if (percent == null) {
            return "0%";
        }

        return (String) percent;
    }

    /**
     * Wait for cold start to finish.
     * @param detectorId Detector Id
     * @param client OpenSearch Client
     * @throws Exception when failing to query/indexing from/to OpenSearch
     */
    protected void waitForInitDetector(String detectorId, RestClient client) throws Exception {

        long startTime = System.currentTimeMillis();
        long duration = 0;
        do {
            /*
             * Detectors will return early, record the exception in
             * node state, and throw exception in the next run. We did it this way since
             * we do not know when current run is gonna finish (e.g, we may have millions
             * of entities to process in one run). Thus,
             * we have to either wait for next runs or use profile API. Here I chose profile
             * API since it is faster.
             */
            Thread.sleep(10_000);
            String initProgress = profileDetectorInitProgress(detectorId, client);
            if (initProgress.equals("100%")) {
                break;
            }

            duration = System.currentTimeMillis() - startTime;
        } while (duration <= 60_000);
    }

    /**
     * Wait for cold start to finish without starting detector job. As profile
     * API depends on job to exist, we cannot use profile API we didn't actually
     * start a job. We simulated job by triggering run APIs. Thus, we change to
     * verify if latest point has result or not. If yes, job is started. Otherwise,
     * no.
     *
     * @param detectorId Detector Id
     * @param client OpenSearch Client
     * @param end date end time of the most recent detection period
     * @param entitySize the number of entities
     * @throws Exception when failing to query/indexing from/to OpenSearch
     */
    protected void simulateWaitForInitDetector(String detectorId, RestClient client, Instant end, int entitySize) throws Exception {

        long startTime = System.currentTimeMillis();
        long duration = 0;
        do {

            Thread.sleep(1_000);

            List<JsonObject> sourceList = getAnomalyResult(detectorId, end, entitySize, client);
            if (sourceList.size() > 0 && getAnomalyGrade(sourceList.get(0)) >= 0) {
                break;
            }

            duration = System.currentTimeMillis() - startTime;
        } while (duration <= 60_000);

        assertTrue("time out while waiting for initing detector", duration <= 60_000);
    }

    protected void bulkIndexData(List<JsonObject> data, String datasetName, RestClient client, String mapping) throws Exception {
        createIndex(datasetName, client, mapping);
        StringBuilder bulkRequestBuilder = new StringBuilder();
        LOG.info("data size {}", data.size());
        int count = 0;
        for (int i = 0; i < data.size(); i++) {
            bulkRequestBuilder.append("{ \"index\" : { \"_index\" : \"" + datasetName + "\", \"_id\" : \"" + i + "\" } }\n");
            bulkRequestBuilder.append(data.get(i).toString()).append("\n");
            count++;
            if (count >= batchSize || i == data.size() - 1) {
                count = 0;
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
        }

        waitAllSyncheticDataIngested(data.size(), datasetName, client);
        LOG.info("data ingestion complete");
    }

    /**
     * Simulate starting detector without waiting for job scheduler to run. Our build process is already very slow (takes 10 mins+)
     * to finish integration tests. This method triggers run API to simulate job scheduler execution in a fast-paced way.
     * @param detectorId detector id
     * @param begin data start time
     * @param end data end time
     * @param client OpenSearch Client
     * @param entitySize number of entities
     * @throws Exception when failing to query/indexing from/to OpenSearch
     */
    protected void simulateStartDetector(String detectorId, Instant begin, Instant end, RestClient client, int entitySize)
        throws Exception {
        runDetectionResult(detectorId, begin, end, client, entitySize);
    }

    protected int isAnomaly(Instant time, List<Entry<Instant, Instant>> labels) {
        for (int i = 0; i < labels.size(); i++) {
            Entry<Instant, Instant> window = labels.get(i);
            if (time.compareTo(window.getKey()) >= 0 && time.compareTo(window.getValue()) <= 0) {
                return i;
            }
        }
        return -1;
    }
}
