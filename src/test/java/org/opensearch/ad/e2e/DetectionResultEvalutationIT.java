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

import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonMessages;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class DetectionResultEvalutationIT extends AbstractSyntheticDataTest {
    protected static final Logger LOG = (Logger) LogManager.getLogger(DetectionResultEvalutationIT.class);

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
        assertEquals(CommonMessages.INTERVAL_REC + recDetectorIntervalMinutes, messageMap.get("detection_interval").get("message"));
    }

    public void testValidationWindowDelayRecommendation() throws Exception {
        RestClient client = client();
        long recDetectorIntervalMillisForDataSet = 180000;
        // this would be equivalent to the window delay in this data test
        List<JsonObject> data = createData(2000, recDetectorIntervalMillisForDataSet);
        indexTrainData("validation", data, 2000, client);
        long detectorInterval = 4;
        long expectedWindowDelayMillis = Instant.now().toEpochMilli() - data.get(0).get("timestamp").getAsLong();
        // we always round up for window delay recommendation to reduce chance of missed data.
        long expectedWindowDelayMinutes = (long) Math.ceil(expectedWindowDelayMillis / 60000.0);
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
        assertEquals(
            String.format(Locale.ROOT, CommonMessages.WINDOW_DELAY_REC, expectedWindowDelayMinutes, expectedWindowDelayMinutes),
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
        // a WarningFailureException on access system indices .opendistro_security will fail the test if this is not false.
        setWarningHandler(request, false);
        client.performRequest(request);
        Thread.sleep(1_000);
        data.stream().limit(trainTestSplit).forEach(r -> {
            try {
                Request req = new Request("POST", String.format(Locale.ROOT, "/%s/_doc/", datasetName));
                req.setJsonEntity(r.toString());
                client.performRequest(req);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        Thread.sleep(3_000);
    }

    public void testRestartHCADDetector() throws Exception {
        // TODO: this test case will run for a much longer time and timeout with security enabled
        if (!isHttps()) {
            try {
                disableResourceNotFoundFaultTolerence();
                verifyRestart("synthetic", 1, 8);
            } catch (Throwable throwable) {
                LOG.info("Retry restart test case", throwable);
                cleanUpCluster();
                wipeAllODFEIndices();
                fail();
            }
        }
    }

    private void verifyRestart(String datasetName, int intervalMinutes, int shingleSize) throws Exception {
        RestClient client = client();

        String dataFileName = String.format(Locale.ROOT, "data/%s.data", datasetName);

        List<JsonObject> data = getData(dataFileName);

        String categoricalField = "host";
        String tsField = "timestamp";

        Clock clock = Clock.systemUTC();
        long currentMilli = clock.millis();
        int trainTestSplit = 1500;

        // e.g., 2019-11-01T00:03:00Z
        String pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern, Locale.ROOT);
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

        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);

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

        String mapping = String
            .format(
                Locale.ROOT,
                "{ \"mappings\": { \"properties\": { \"timestamp\": { \"type\": \"date\"},"
                    + " \"Feature1\": { \"type\": \"double\" }, \"Feature2\": { \"type\": \"double\" },"
                    + "\"%s\": { \"type\": \"keyword\"} } } }",
                categoricalField
            );
        bulkIndexTrainData(datasetName, data, trainTestSplit, client, mapping);

        String detector = String
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
                categoricalField,
                0
            );
        String detectorId = createDetector(client, detector);
        // cannot stop without actually starting detector because ad complains no ad job index
        startDetector(detectorId, client);
        profileDetectorInitProgress(detectorId, client);
        // it would be long if we wait for the job actually run the work periodically; speed it up by using simulateHCADStartDetector
        waitForInitDetector(detectorId, client);
        String initProgress = profileDetectorInitProgress(detectorId, client);
        assertEquals("init progress is " + initProgress, "100%", initProgress);
        stopDetector(detectorId, client);
        // restart detector
        startDetector(detectorId, client);
        waitForInitDetector(detectorId, client);
        initProgress = profileDetectorInitProgress(detectorId, client);
        assertEquals("init progress is " + initProgress, "100%", initProgress);
    }

    private void stopDetector(String detectorId, RestClient client) throws Exception {
        Request request = new Request("POST", String.format(Locale.ROOT, "/_plugins/_anomaly_detection/detectors/%s/_stop", detectorId));

        Map<String, Object> response = entityAsMap(client.performRequest(request));
        String responseDetectorId = (String) response.get("_id");
        assertEquals(detectorId, responseDetectorId);
    }
}
