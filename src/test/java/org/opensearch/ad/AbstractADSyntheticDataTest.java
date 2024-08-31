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

package org.opensearch.ad;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MODEL_MAX_SIZE_PERCENTAGE;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.timeseries.AbstractSyntheticDataTest;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AbstractADSyntheticDataTest extends AbstractSyntheticDataTest {
    protected static class TrainResult {
        public String detectorId;
        public List<JsonObject> data;
        // actual index of training data. As we have multiple entities,
        // trainTestSplit means how many groups of entities are used for training.
        // rawDataTrainTestSplit is the actual index of training data.
        public int rawDataTrainTestSplit;
        public Duration windowDelay;
        public Instant trainTime;
        // first data time in data
        public Instant firstDataTime;
        // last data time in data
        public Instant finalDataTime;

        public TrainResult(
            String detectorId,
            List<JsonObject> data,
            int rawDataTrainTestSplit,
            Duration windowDelay,
            Instant trainTime,
            String timeStampField
        ) {
            this.detectorId = detectorId;
            this.data = data;
            this.rawDataTrainTestSplit = rawDataTrainTestSplit;
            this.windowDelay = windowDelay;
            this.trainTime = trainTime;

            this.firstDataTime = getDataTimeOfEpochMillis(timeStampField, data, 0);
            this.finalDataTime = getDataTimeOfEpochMillis(timeStampField, data, data.size() - 1);
        }
    }

    public static final Logger LOG = (Logger) LogManager.getLogger(AbstractADSyntheticDataTest.class);

    protected static final double EPSILON = 1e-3;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        // increase the AD memory percentage. Since enabling jacoco coverage instrumentation,
        // the memory is not enough to finish HistoricalAnalysisRestApiIT.
        updateClusterSettings(AD_MODEL_MAX_SIZE_PERCENTAGE.getKey(), 0.5);
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

    protected void startHistorical(String detectorId, Instant begin, Instant end, RestClient client, int entitySize) throws IOException,
        InterruptedException {
        // trigger run in current interval
        Request request = new Request(
            "POST",
            String.format(Locale.ROOT, "/_opendistro/_anomaly_detection/detectors/%s/_start", detectorId)
        );
        request
            .setJsonEntity(
                String.format(Locale.ROOT, "{ \"start_time\": %d, \"end_time\": %d }", begin.toEpochMilli(), end.toEpochMilli())
            );
        int statusCode = client.performRequest(request).getStatusLine().getStatusCode();
        assert (statusCode >= 200 && statusCode < 300);

        // wait for 50 milliseconds per entity before next query
        Thread.sleep(50 * entitySize);
    }

    protected Map<String, Object> preview(String detector, Instant begin, Instant end, RestClient client) throws IOException,
        InterruptedException {
        LOG.info("preview detector {}", detector);
        // trigger run in current interval
        Request request = new Request("POST", "/_plugins/_anomaly_detection/detectors/_preview");
        request
            .setJsonEntity(
                String
                    .format(
                        Locale.ROOT,
                        "{ \"period_start\": %d, \"period_end\": %d, \"detector\": %s }",
                        begin.toEpochMilli(),
                        end.toEpochMilli(),
                        detector
                    )
            );
        Response response = client.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        assert (statusCode >= 200 && statusCode < 300);

        return entityAsMap(response);
    }

    protected Map<String, Object> previewWithFailure(String detector, Instant begin, Instant end, RestClient client) throws IOException,
        InterruptedException {
        // trigger run in current interval
        Request request = new Request("POST", "/_plugins/_anomaly_detection/detectors/_preview");
        request
            .setJsonEntity(
                String
                    .format(
                        Locale.ROOT,
                        "{ \"period_start\": %d, \"period_end\": %d, \"detector\": %s }",
                        begin.toEpochMilli(),
                        end.toEpochMilli(),
                        detector
                    )
            );
        Response response = client.performRequest(request);
        int statusCode = response.getStatusLine().getStatusCode();
        assert (statusCode == 400);

        return entityAsMap(response);
    }

    protected List<JsonObject> getAnomalyResultByDataTime(
        String detectorId,
        Instant end,
        int entitySize,
        RestClient client,
        boolean approximateEndTime,
        long rangeDurationMillis
    ) throws InterruptedException {
        return getAnomalyResult(
            detectorId,
            end,
            entitySize,
            client,
            approximateEndTime,
            rangeDurationMillis,
            "data_end_time",
            (h, eSize) -> h.size() == eSize,
            entitySize
        );
    }

    protected List<JsonObject> getAnomalyResultByExecutionTime(
        String detectorId,
        Instant end,
        int entitySize,
        RestClient client,
        boolean approximateEndTime,
        long rangeDurationMillis,
        int expectedResultSize
    ) throws InterruptedException {
        return getAnomalyResult(
            detectorId,
            end,
            entitySize,
            client,
            approximateEndTime,
            rangeDurationMillis,
            "execution_end_time",
            (h, eSize) -> h.size() >= eSize,
            expectedResultSize
        );
    }

    protected List<JsonObject> getAnomalyResult(
        String detectorId,
        Instant end,
        int entitySize,
        RestClient client,
        boolean approximateEndTime,
        long rangeDurationMillis,
        String endTimeField,
        ConditionChecker checker,
        int expectedResultSize
    ) throws InterruptedException {
        Request request = new Request("POST", "/_plugins/_anomaly_detection/detectors/results/_search");

        String jsonTemplatePrefix = "{\n"
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
            + "                        \"%s\": {\n";

        StringBuilder jsonTemplate = new StringBuilder();
        jsonTemplate.append(jsonTemplatePrefix);

        if (approximateEndTime) {
            // we may get two interval results if using gte
            jsonTemplate.append("                            \"gt\": %d,\n                            \"lte\": %d\n");
        } else {
            jsonTemplate.append("                            \"gte\": %d,\n                            \"lte\": %d\n");
        }

        jsonTemplate
            .append(
                "                        }\n"
                    + "                    }\n"
                    + "                }\n"
                    + "            ]\n"
                    + "        }\n"
                    + "    }\n"
                    + "}"
            );

        long dateEndTime = end.toEpochMilli();
        String formattedJson = null;

        if (approximateEndTime) {
            formattedJson = String
                .format(Locale.ROOT, jsonTemplate.toString(), detectorId, endTimeField, dateEndTime - rangeDurationMillis, dateEndTime);
        } else {
            formattedJson = String.format(Locale.ROOT, jsonTemplate.toString(), detectorId, endTimeField, dateEndTime, dateEndTime);
        }

        request.setJsonEntity(formattedJson);

        // wait until results are available
        // max wait for 60_000 milliseconds
        int maxWaitCycles = 30;
        do {
            try {
                JsonArray hits = getHits(client, request);
                if (hits != null && checker.checkCondition(hits, entitySize)) {
                    List<JsonObject> res = new ArrayList<>();
                    for (int i = 0; i < hits.size(); i++) {
                        JsonObject source = hits.get(i).getAsJsonObject().get("_source").getAsJsonObject();
                        res.add(source);
                    }

                    return res;
                } else {
                    LOG.info("wait for result, previous result: {}, size: {}", hits, hits.size());
                    client.performRequest(new Request("POST", String.format(Locale.ROOT, "/%s/_refresh", ".opendistro-anomaly-results*")));
                }
                Thread.sleep(2_000 * entitySize);
            } catch (Exception e) {
                LOG.warn("Exception while waiting for result", e);
                Thread.sleep(2_000 * entitySize);
            }
        } while (maxWaitCycles-- >= 0);

        // leave some debug information before returning empty
        try {
            String matchAll = "{\n" + "  \"size\": 1000,\n" + "  \"query\": {\n" + "    \"match_all\": {}\n" + "  }\n" + "}";
            request.setJsonEntity(matchAll);
            JsonArray hits = getHits(client, request);
            LOG.info("Query: {}", formattedJson);
            LOG.info("match all result: {}", hits);
        } catch (Exception e) {
            LOG.warn("Exception while waiting for match all result", e);
        }

        return new ArrayList<>();
    }

    protected List<JsonObject> getRealTimeAnomalyResult(String detectorId, Instant end, int entitySize, RestClient client)
        throws InterruptedException {
        return getAnomalyResultByDataTime(detectorId, end, entitySize, client, false, 0);
    }

    public double getAnomalyGrade(JsonObject source) {
        return source.get("anomaly_grade").getAsDouble();
    }

    public double getConfidence(JsonObject source) {
        return source.get("confidence").getAsDouble();
    }

    public String getEntity(JsonObject source) {
        JsonElement element = source.get("entity");
        if (element == null) {
            // single stream
            return "dummy";
        }
        return element.getAsJsonArray().get(0).getAsJsonObject().get("value").getAsString();
    }

    /**
     * We can detect anomaly late. If yes, use approx_anomaly_start_time; otherwise, use defaultVal.
     * @param source source response containing anomaly result.
     * @param defaultVal default anomaly time. Usually data end time.
     * @return anomaly event time.
     */
    public Instant getAnomalyTime(JsonObject source, Instant defaultVal) {
        JsonElement anomalyTime = source.get("approx_anomaly_start_time");
        if (anomalyTime != null) {
            long epochhMillis = anomalyTime.getAsLong();
            return Instant.ofEpochMilli(epochhMillis);
        }
        return defaultVal;
    }

    public JsonObject getFeature(JsonObject source, int index) {
        JsonArray featureDataArray = source.getAsJsonArray("feature_data");

        // Get the index element from the JsonArray
        return featureDataArray.get(index).getAsJsonObject();
    }

    public JsonObject getImputed(JsonObject source, int index) {
        JsonArray featureDataArray = source.getAsJsonArray("feature_imputed");
        if (featureDataArray == null) {
            return null;
        }

        // Get the index element from the JsonArray
        return featureDataArray.get(index).getAsJsonObject();
    }

    protected JsonObject getImputed(JsonObject source, String featureId) {
        JsonArray featureDataArray = source.getAsJsonArray("feature_imputed");
        if (featureDataArray == null) {
            return null;
        }

        for (int i = 0; i < featureDataArray.size(); i++) {
            // Get the index element from the JsonArray
            JsonObject jsonObject = featureDataArray.get(i).getAsJsonObject();
            if (jsonObject.get("feature_id").getAsString().equals(featureId)) {
                return jsonObject;
            }
        }
        return null;
    }

    protected String createDetector(RestClient client, String detectorJson) throws Exception {
        Request request = new Request("POST", "/_plugins/_anomaly_detection/detectors/");

        request.setJsonEntity(detectorJson);
        Map<String, Object> response = entityAsMap(client.performRequest(request));
        String detectorId = (String) response.get("_id");
        Thread.sleep(1_000);
        return detectorId;
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
     * @param entitySize the number of entity results to wait for
     * @return initial result
     * @throws Exception when failing to query/indexing from/to OpenSearch
     */
    protected List<JsonObject> simulateWaitForInitDetector(String detectorId, RestClient client, Instant end, int entitySize)
        throws Exception {

        long startTime = System.currentTimeMillis();
        long duration = 0;
        do {

            Thread.sleep(1_000);

            List<JsonObject> sourceList = getRealTimeAnomalyResult(detectorId, end, entitySize, client);
            if (sourceList.size() > 0 && getAnomalyGrade(sourceList.get(0)) >= 0) {
                return sourceList;
            }

            duration = System.currentTimeMillis() - startTime;
        } while (duration <= 60_000);

        assertTrue("time out while waiting for initing detector", false);
        return null;
    }

    protected List<JsonObject> waitForHistoricalDetector(
        String detectorId,
        RestClient client,
        Instant end,
        int entitySize,
        int intervalMillis
    ) throws Exception {

        long startTime = System.currentTimeMillis();
        long duration = 0;
        do {

            Thread.sleep(1_000);

            List<JsonObject> sourceList = getAnomalyResultByDataTime(detectorId, end, entitySize, client, true, intervalMillis);
            if (sourceList.size() > 0 && getAnomalyGrade(sourceList.get(0)) >= 0) {
                return sourceList;
            }

            duration = System.currentTimeMillis() - startTime;
        } while (duration <= 60_000);

        assertTrue("time out while waiting for historical detector to finish", false);
        return null;
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

    public int isAnomaly(Instant time, List<Entry<Instant, Instant>> labels) {
        for (int i = 0; i < labels.size(); i++) {
            Entry<Instant, Instant> window = labels.get(i);
            if (time.compareTo(window.getKey()) >= 0 && time.compareTo(window.getValue()) <= 0) {
                return i;
            }
        }
        return -1;
    }

    protected List<JsonObject> getData(String datasetFileName) throws Exception {
        JsonArray jsonArray = JsonParser
            .parseReader(new FileReader(new File(getClass().getResource(datasetFileName).toURI()), Charset.defaultCharset()))
            .getAsJsonArray();
        List<JsonObject> list = new ArrayList<>(jsonArray.size());
        jsonArray.iterator().forEachRemaining(i -> list.add(i.getAsJsonObject()));
        return list;
    }

    protected Instant dataToExecutionTime(Instant instant, Duration windowDelay) {
        return instant.plus(windowDelay);
    }

    /**
     * Assume the data is sorted in time. The method look up and below startIndex
     * and return how many timestamps equal to timestampStr.
     * @param startIndex where to start look for timestamp
     * @return how many timestamps equal to timestampStr
     */
    protected int findGivenTimeEntities(int startIndex, List<JsonObject> data) {
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

    /**
     *
     * @param beginTimeStampAsString data start time in string
     * @param entityMap a map to record the number of times we have seen a timestamp. Used to detect missing values.
     * @param windowDelay ingestion delay
     * @param intervalMinutes detector interval
     * @param detectorId detector Id
     * @param client RestFul client
     * @param numberOfEntities the number of entities.
     * @return whether we erred out.
     */
    protected boolean scoreOneResult(
        String beginTimeStampAsString,
        TreeMap<String, Integer> entityMap,
        Duration windowDelay,
        int intervalMinutes,
        String detectorId,
        RestClient client,
        int numberOfEntities
    ) {
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

    protected List<JsonObject> startRealTimeDetector(
        TrainResult trainResult,
        int numberOfEntities,
        int intervalMinutes,
        boolean imputeEnabled
    ) throws Exception {
        Instant executeBegin = dataToExecutionTime(trainResult.trainTime, trainResult.windowDelay);
        Instant executeEnd = executeBegin.plus(intervalMinutes, ChronoUnit.MINUTES);
        Instant dataEnd = trainResult.trainTime.plus(intervalMinutes, ChronoUnit.MINUTES);

        LOG.info("start detector {}, dataStart {}, dataEnd {}", trainResult.detectorId, trainResult.trainTime, dataEnd);
        simulateStartDetector(trainResult.detectorId, executeBegin, executeEnd, client(), numberOfEntities);
        int resultsToWait = numberOfEntities;
        if (!imputeEnabled) {
            resultsToWait = findGivenTimeEntities(trainResult.rawDataTrainTestSplit - 1, trainResult.data);
        }
        LOG.info("wait for initting detector {}. {} results are expected.", trainResult.detectorId, resultsToWait);
        return simulateWaitForInitDetector(trainResult.detectorId, client(), dataEnd, resultsToWait);
    }

    protected List<JsonObject> startHistoricalDetector(
        TrainResult trainResult,
        int numberOfEntities,
        int intervalMinutes,
        boolean imputeEnabled
    ) throws Exception {
        LOG.info("start historical detector {}", trainResult.detectorId);
        startHistorical(trainResult.detectorId, trainResult.firstDataTime, trainResult.finalDataTime, client(), numberOfEntities);
        int resultsToWait = numberOfEntities;
        if (!imputeEnabled) {
            findGivenTimeEntities(trainResult.data.size() - 1, trainResult.data);
        }
        LOG
            .info(
                "wait for historical detector {} at {}. {} results are expected.",
                trainResult.detectorId,
                trainResult.finalDataTime,
                resultsToWait
            );
        return waitForHistoricalDetector(
            trainResult.detectorId,
            client(),
            trainResult.finalDataTime,
            resultsToWait,
            intervalMinutes * 60000
        );
    }

    protected long getWindowDelayMinutes(List<JsonObject> data, int trainTestSplit, String timestamp) {
        // e.g., "2019-11-02T00:59:00Z"
        String trainTimeStr = data.get(trainTestSplit - 1).get("timestamp").getAsString();
        Instant trainTime = Instant.from(DateTimeFormatter.ISO_INSTANT.parse(trainTimeStr));
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
        return Duration.between(trainTime, Instant.now()).toMinutes();
    }

    public static boolean areDoublesEqual(double d1, double d2) {
        return Math.abs(d1 - d2) < EPSILON;
    }

    @FunctionalInterface
    public interface ConditionChecker {
        boolean checkCondition(JsonArray hits, int expectedSize);
    }

    protected static Instant getDataTimeOfEpochMillis(String timestampField, List<JsonObject> data, int index) {
        String finalTimeStr = data.get(index).get(timestampField).getAsString();
        return Instant.ofEpochMilli(Long.parseLong(finalTimeStr));
    }

    protected static Instant getDataTimeofISOFormat(String timestampField, List<JsonObject> data, int index) {
        String finalTimeStr = data.get(index).get(timestampField).getAsString();

        try {
            // Attempt to parse as an ISO 8601 formatted string (e.g., "2019-11-01T00:00:00Z")
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(finalTimeStr, DateTimeFormatter.ISO_DATE_TIME);
            return zonedDateTime.toInstant();
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException("Invalid timestamp format: " + finalTimeStr, ex);
        }
    }

    protected List<JsonObject> getTasks(String detectorId, int size, ConditionChecker checker, RestClient client)
        throws InterruptedException {
        Request request = new Request("POST", "/_plugins/_anomaly_detection/detectors/tasks/_search");

        String jsonTemplate = "{\n"
            + "  \"size\": %d,\n"
            + "  \"query\": {\n"
            + "    \"bool\": {\n"
            + "      \"filter\": [\n"
            + "        {\n"
            + "          \"term\": {\n"
            + "            \"detector_id\": \"%s\"\n"
            + "          }\n"
            + "        }\n"
            + "      ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

        // try to get size + 10 results if there are that many
        String formattedJson = String.format(Locale.ROOT, jsonTemplate, size + 10, detectorId);

        request.setJsonEntity(formattedJson);

        // wait until results are available
        // max wait for 60_000 milliseconds
        int maxWaitCycles = 30;
        do {
            try {
                JsonArray hits = getHits(client, request);
                if (hits != null && checker.checkCondition(hits, size)) {
                    List<JsonObject> res = new ArrayList<>();
                    for (int i = 0; i < hits.size(); i++) {
                        JsonObject source = hits.get(i).getAsJsonObject().get("_source").getAsJsonObject();
                        res.add(source);
                    }

                    return res;
                } else {
                    LOG.info("wait for result, previous result: {}, size: {}", hits, hits.size());
                }
                Thread.sleep(2_000 * size);
            } catch (Exception e) {
                LOG.warn("Exception while waiting for result", e);
                Thread.sleep(2_000 * size);
            }
        } while (maxWaitCycles-- >= 0);

        // leave some debug information before returning empty
        try {
            String matchAll = "{\n" + "  \"size\": 1000,\n" + "  \"query\": {\n" + "    \"match_all\": {}\n" + "  }\n" + "}";
            request.setJsonEntity(matchAll);
            JsonArray hits = getHits(client, request);
            LOG.info("Query: {}", formattedJson);
            LOG.info("match all result: {}", hits);
        } catch (Exception e) {
            LOG.warn("Exception while waiting for match all result", e);
        }

        return new ArrayList<>();
    }

    protected static boolean getLatest(List<JsonObject> data, int index) {
        return data.get(index).get("is_latest").getAsBoolean();
    }
}
