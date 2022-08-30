/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.rest;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomBoolean;
import static org.opensearch.test.OpenSearchTestCase.randomAlphaOfLength;
import static org.opensearch.test.OpenSearchTestCase.randomDoubleBetween;
import static org.opensearch.test.OpenSearchTestCase.randomInt;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;
import static org.opensearch.test.OpenSearchTestCase.randomLong;
import static org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.ToDoubleFunction;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.mock.model.MockSimpleLog;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentParser;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

//TODO: remove duplicate code in HistoricalAnalysisRestTestCase
public class ADRestTestUtils {
    protected static final Logger LOG = (Logger) LogManager.getLogger(ADRestTestUtils.class);

    public enum DetectorType {
        SINGLE_ENTITY_DETECTOR,
        SINGLE_CATEGORY_HC_DETECTOR,
        MULTI_CATEGORY_HC_DETECTOR
    }

    public static Response ingestSimpleMockLog(
        RestClient client,
        String indexName,
        int startDays,
        int totalDocsPerCategory,
        long intervalInMinutes,
        ToDoubleFunction<Integer> valueFunc,
        int ipSize,
        int categorySize,
        boolean createIndex
    ) throws IOException {
        if (createIndex) {
            TestHelpers
                .makeRequest(
                    client,
                    "PUT",
                    indexName,
                    null,
                    TestHelpers.toHttpEntity(MockSimpleLog.INDEX_MAPPING),
                    ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "OpenSearch"))
                );
        }

        StringBuilder bulkRequestBuilder = new StringBuilder();
        Instant startTime = Instant.now().minus(startDays, ChronoUnit.DAYS);
        for (int i = 0; i < totalDocsPerCategory; i++) {
            for (int m = 0; m < ipSize; m++) {
                String ip = "192.168.1." + m;
                for (int n = 0; n < categorySize; n++) {
                    String category = "category" + n;
                    String docId = randomAlphaOfLength(10);
                    bulkRequestBuilder.append("{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_id\" : \"" + docId + "\" } }\n");
                    MockSimpleLog simpleLog1 = new MockSimpleLog(
                        startTime,
                        valueFunc.applyAsDouble(i),
                        ip,
                        category,
                        randomBoolean(),
                        randomAlphaOfLength(5)
                    );
                    bulkRequestBuilder.append(TestHelpers.toJsonString(simpleLog1));
                    bulkRequestBuilder.append("\n");
                }
            }
            startTime = startTime.plus(intervalInMinutes, ChronoUnit.MINUTES);
        }
        Response bulkResponse = TestHelpers
            .makeRequest(
                client,
                "POST",
                "_bulk?refresh=true",
                null,
                TestHelpers.toHttpEntity(bulkRequestBuilder.toString()),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
            );
        return bulkResponse;
    }

    public static Response ingestTestDataForHistoricalAnalysis(
        RestClient client,
        String indexName,
        int detectionIntervalInMinutes,
        boolean createIndex,
        int startDays,
        int totalDocsPerCategory,
        int categoryFieldSize
    ) throws IOException {
        return ingestSimpleMockLog(client, indexName, startDays, totalDocsPerCategory, detectionIntervalInMinutes, (i) -> {
            if (i % 500 == 0) {
                return randomDoubleBetween(100, 1000, true);
            } else {
                return randomDoubleBetween(1, 10, true);
            }
        }, categoryFieldSize, categoryFieldSize, createIndex);
    }

    @SuppressWarnings("unchecked")
    public static int getDocCountOfIndex(RestClient client, String indexName) throws IOException {
        Response searchResponse = TestHelpers
            .makeRequest(
                client,
                "GET",
                indexName + "/_search",
                null,
                TestHelpers.toHttpEntity("{\"track_total_hits\": true}"),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "OpenSearch"))
            );

        Map<String, Object> responseMap = entityAsMap(searchResponse);
        Object total = ((Map<String, Object>) responseMap.get("hits")).get("total");
        return (int) ((Map<String, Object>) total).get("value");
    }

    public static Response createAnomalyDetector(
        RestClient client,
        String indexName,
        String timeField,
        int detectionIntervalInMinutes,
        int windowDelayIntervalInMinutes,
        String valueField,
        String aggregationMethod,
        String filterQuery,
        List<String> categoryFields
    ) throws Exception {
        return createAnomalyDetector(
            client,
            indexName,
            timeField,
            detectionIntervalInMinutes,
            windowDelayIntervalInMinutes,
            valueField,
            aggregationMethod,
            filterQuery,
            categoryFields,
            false
        );
    }

    public static Response createAnomalyDetector(
        RestClient client,
        String indexName,
        String timeField,
        int detectionIntervalInMinutes,
        int windowDelayIntervalInMinutes,
        String valueField,
        String aggregationMethod,
        String filterQuery,
        List<String> categoryFields,
        boolean historical
    ) throws Exception {
        Instant now = Instant.now();
        AnomalyDetector detector = new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            // TODO: check why throw duplicate detector name error with randomAlphaOfLength(20) in twoThirdsUpgradedClusterTask
            randomAlphaOfLength(20) + now.toEpochMilli(),
            randomAlphaOfLength(30),
            timeField,
            ImmutableList.of(indexName),
            ImmutableList.of(TestHelpers.randomFeature(randomAlphaOfLength(5), valueField, aggregationMethod, true)),
            filterQuery == null ? TestHelpers.randomQuery("{\"match_all\":{\"boost\":1}}") : TestHelpers.randomQuery(filterQuery),
            new IntervalTimeConfiguration(detectionIntervalInMinutes, ChronoUnit.MINUTES),
            new IntervalTimeConfiguration(windowDelayIntervalInMinutes, ChronoUnit.MINUTES),
            randomIntBetween(1, 20),
            null,
            randomInt(),
            now,
            categoryFields,
            TestHelpers.randomUser(),
            null
        );

        if (historical) {
            detector.setDetectionDateRange(new DetectionDateRange(now.minus(30, ChronoUnit.DAYS), now));
        }

        return TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI,
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(detector),
                null
            );
    }

    @SuppressWarnings("unchecked")
    public static List<ADTask> searchLatestAdTaskOfDetector(RestClient client, String detectorId, String taskType) throws IOException {
        List<ADTask> adTasks = new ArrayList<>();
        Response searchAdTaskResponse = TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/tasks/_search",
                ImmutableMap.of(),
                TestHelpers
                    .toHttpEntity(
                        "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"detector_id\":\""
                            + detectorId
                            + "\"}},{\"term\":{\"is_latest\":\"true\"}},{\"terms\":{\"task_type\":[\""
                            + taskType
                            + "\"]}}]}},\"sort\":[{\"execution_start_time\":{\"order\":\"desc\"}}],\"size\":1000}"
                    ),
                null
            );
        Map<String, Object> responseMap = entityAsMap(searchAdTaskResponse);
        Map<String, Object> hits = (Map<String, Object>) responseMap.get("hits");
        Object totalHits = hits.get("total");
        Integer totalTasks = (Integer) ((Map<String, Object>) totalHits).get("value");

        if (totalTasks == 0) {
            return adTasks;
        }
        List<Object> adTaskResponses = (List<Object>) hits.get("hits");
        for (Object adTaskResponse : adTaskResponses) {
            String id = (String) ((Map<String, Object>) adTaskResponse).get("_id");
            Map<String, Object> source = (Map<String, Object>) ((Map<String, Object>) adTaskResponse).get("_source");
            String state = (String) source.get(ADTask.STATE_FIELD);
            String parsedDetectorId = (String) source.get(ADTask.DETECTOR_ID_FIELD);
            Double taskProgress = (Double) source.get(ADTask.TASK_PROGRESS_FIELD);
            Double initProgress = (Double) source.get(ADTask.INIT_PROGRESS_FIELD);
            String parsedTaskType = (String) source.get(ADTask.TASK_TYPE_FIELD);
            String coordinatingNode = (String) source.get(ADTask.COORDINATING_NODE_FIELD);
            ADTask adTask = ADTask
                .builder()
                .taskId(id)
                .state(state)
                .detectorId(parsedDetectorId)
                .taskProgress(taskProgress.floatValue())
                .initProgress(initProgress.floatValue())
                .taskType(parsedTaskType)
                .coordinatingNode(coordinatingNode)
                .build();
            adTasks.add(adTask);
        }
        return adTasks;
    }

    @SuppressWarnings("unchecked")
    public static int countADResultOfDetector(RestClient client, String detectorId, String taskId) throws IOException {
        String taskFilter = "TASK_FILTER";
        String query = "{\"query\":{\"bool\":{\"filter\":[{\"term\":{\"detector_id\":\""
            + detectorId
            + "\"}}"
            + taskFilter
            + "]}},\"track_total_hits\":true,\"size\":0}";
        if (taskId != null) {
            query = query.replace(taskFilter, ",{\"term\":{\"task_id\":\"" + taskId + "\"}}");
        } else {
            query = query.replace(taskFilter, "");
        }
        Response searchAdTaskResponse = TestHelpers
            .makeRequest(
                client,
                "GET",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/results/_search",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(query),

                null
            );
        Map<String, Object> responseMap = entityAsMap(searchAdTaskResponse);
        Map<String, Object> hits = (Map<String, Object>) ((Map<String, Object>) responseMap.get("hits")).get("total");
        return (int) hits.get("value");
    }

    @SuppressWarnings("unchecked")
    public static int countDetectors(RestClient client, String detectorType) throws IOException {
        String detectorTypeFilter = "DETECTOR_TYPE_FILTER";
        String query = "{\"query\":{\"bool\":{\"filter\":[{\"exists\":{\"field\":\"name\"}}"
            + detectorTypeFilter
            + "]}},\"track_total_hits\":true,\"size\":0}";
        if (detectorType != null) {
            query = query.replace(detectorTypeFilter, ",{\"term\":{\"detector_type\":\"" + detectorType + "\"}}");
        } else {
            query = query.replace(detectorTypeFilter, "");
        }
        Response searchAdTaskResponse = TestHelpers
            .makeRequest(
                client,
                "GET",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/_search",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(query),

                null
            );
        Map<String, Object> responseMap = entityAsMap(searchAdTaskResponse);
        Map<String, Object> hits = (Map<String, Object>) ((Map<String, Object>) responseMap.get("hits")).get("total");
        return (int) hits.get("value");
    }

    @SuppressWarnings("unchecked")
    // public static Map<String, Object> getDetectorWithJobAndTask(RestClient client, String detectorId) throws IOException {
    // Map<String, Object> results = new HashMap<>();
    // Response searchAdTaskResponse = TestHelpers
    // .makeRequest(
    // client,
    // "GET",
    // TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + detectorId + "?job=true&task=true",
    // ImmutableMap.of(),
    // (HttpEntity) null,
    // null
    // );
    // Map<String, Object> responseMap = entityAsMap(searchAdTaskResponse);
    //
    // Map<String, Object> jobMap = (Map<String, Object>) responseMap.get(ANOMALY_DETECTOR_JOB);
    // if (jobMap != null) {
    // String jobName = (String) jobMap.get(AnomalyDetectorJob.NAME_FIELD);
    // boolean enabled = (boolean) jobMap.get(AnomalyDetectorJob.IS_ENABLED_FIELD);
    // long enabledTime = (long) jobMap.get(AnomalyDetectorJob.ENABLED_TIME_FIELD);
    // long lastUpdateTime = (long) jobMap.get(AnomalyDetectorJob.LAST_UPDATE_TIME_FIELD);
    //
    // AnomalyDetectorJob job = new AnomalyDetectorJob(
    // jobName,
    // null,
    // null,
    // enabled,
    // Instant.ofEpochMilli(enabledTime),
    // null,
    // Instant.ofEpochMilli(lastUpdateTime),
    // null,
    // null,
    // null
    // );
    // results.put(ANOMALY_DETECTOR_JOB, job);
    // }
    //
    // Map<String, Object> historicalTaskMap = (Map<String, Object>) responseMap.get(HISTORICAL_ANALYSIS_TASK);
    // if (historicalTaskMap != null) {
    // ADTask historicalAdTask = parseAdTask(historicalTaskMap);
    // results.put(HISTORICAL_ANALYSIS_TASK, historicalAdTask);
    // }
    //
    // Map<String, Object> realtimeTaskMap = (Map<String, Object>) responseMap.get(REALTIME_TASK);
    // if (realtimeTaskMap != null) {
    // ADTask realtimeAdTask = parseAdTask(realtimeTaskMap);
    // results.put(REALTIME_TASK, realtimeAdTask);
    // }
    //
    // return results;
    // }

    private static ADTask parseAdTask(Map<String, Object> taskMap) {
        String id = (String) taskMap.get(ADTask.TASK_ID_FIELD);
        String state = (String) taskMap.get(ADTask.STATE_FIELD);
        String parsedDetectorId = (String) taskMap.get(ADTask.DETECTOR_ID_FIELD);
        Double taskProgress = (Double) taskMap.get(ADTask.TASK_PROGRESS_FIELD);
        Double initProgress = (Double) taskMap.get(ADTask.INIT_PROGRESS_FIELD);
        String parsedTaskType = (String) taskMap.get(ADTask.TASK_TYPE_FIELD);
        String coordinatingNode = (String) taskMap.get(ADTask.COORDINATING_NODE_FIELD);
        return ADTask
            .builder()
            .taskId(id)
            .state(state)
            .detectorId(parsedDetectorId)
            .taskProgress(taskProgress.floatValue())
            .initProgress(initProgress.floatValue())
            .taskType(parsedTaskType)
            .coordinatingNode(coordinatingNode)
            .build();
    }

    /**
     * Start anomaly detector directly.
     * For AD versions on or before 1.0, this function will start realtime job for
     * realtime detector, and start historical analysis for historical detector.
     *
     * For AD version on or after 1.1, this function will start realtime job only.
     * @param client REST client
     * @param detectorId detector id
     * @return job id for realtime job or task id for historical analysis
     * @throws IOException exception may throw in entityAsMap
     */
    @SuppressWarnings("unchecked")
    public static String startAnomalyDetectorDirectly(RestClient client, String detectorId) throws IOException {
        Response response = TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + detectorId + "/_start",
                ImmutableMap.of(),
                (HttpEntity) null,
                null
            );
        Map<String, Object> startDetectorResponseMap = entityAsMap(response);
        // For AD on or before 1.0, if the detector is historical detector, then it will be task id
        String jobOrTaskId = (String) startDetectorResponseMap.get("_id");
        return jobOrTaskId;
    }

    /**
     * Start historical analysis.
     * For AD versions on or before 1.0, should pass historical detector id to
     * this function.
     * For AD version on or after 1.1, can pass any detector id to this function.
     *
     * @param client REST client
     * @param detectorId detector id
     * @return task id of historical analysis
     * @throws IOException exception may throw in toHttpEntity and entityAsMap
     */
    @SuppressWarnings("unchecked")
    public static String startHistoricalAnalysis(RestClient client, String detectorId) throws IOException {
        Instant now = Instant.now();
        DetectionDateRange dateRange = new DetectionDateRange(now.minus(30, ChronoUnit.DAYS), now);
        Response response = TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + detectorId + "/_start",
                ImmutableMap.of(),
                // Start historical detector directly on new node will start realtime job.
                // Need to pass detection date range in http body if need to start historical analysis.
                TestHelpers.toHttpEntity(TestHelpers.toJsonString(dateRange)),
                null
            );
        Map<String, Object> startDetectorResponseMap = entityAsMap(response);
        String taskId = (String) startDetectorResponseMap.get("_id");
        return taskId;
    }

    public static ADTaskProfile waitUntilTaskDone(RestClient client, String detectorId) throws InterruptedException {
        return waitUntilTaskReachState(client, detectorId, TestHelpers.HISTORICAL_ANALYSIS_DONE_STATS);
    }

    public static ADTaskProfile waitUntilTaskReachState(RestClient client, String detectorId, Set<String> targetStates)
        throws InterruptedException {
        int i = 0;
        int retryTimes = 200;
        ADTaskProfile adTaskProfile = null;
        while ((adTaskProfile == null || !targetStates.contains(adTaskProfile.getAdTask().getState())) && i < retryTimes) {
            try {
                adTaskProfile = getADTaskProfile(client, detectorId);
            } catch (Exception e) {
                LOG.error("failed to get ADTaskProfile", e);
            } finally {
                Thread.sleep(1000);
            }
            i++;
        }
        // assertNotNull(adTaskProfile);
        return adTaskProfile;
    }

    public static ADTaskProfile getADTaskProfile(RestClient client, String detectorId) throws IOException {
        Response profileResponse = TestHelpers
            .makeRequest(
                client,
                "GET",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + detectorId + "/_profile?_all",
                ImmutableMap.of(),
                "",
                null
            );
        return parseADTaskProfile(profileResponse);
    }

    public static ADTaskProfile parseADTaskProfile(Response profileResponse) throws IOException {
        String profileResult = EntityUtils.toString(profileResponse.getEntity());
        XContentParser parser = TestHelpers.parser(profileResult);
        ADTaskProfile adTaskProfile = null;
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();
            if ("ad_task".equals(fieldName)) {
                adTaskProfile = ADTaskProfile.parse(parser);
            } else {
                parser.skipChildren();
            }
        }
        return adTaskProfile;
    }

    public static Response stopRealtimeJob(RestClient client, String detectorId) throws IOException {
        return stopDetector(client, detectorId, false);
    }

    public static Response stopHistoricalAnalysis(RestClient client, String detectorId) throws IOException {
        return stopDetector(client, detectorId, true);
    }

    public static Response stopDetector(RestClient client, String detectorId, boolean historicalAnalysis) throws IOException {
        String param = historicalAnalysis ? "?historical" : "";
        Response response = TestHelpers
            .makeRequest(
                client,
                "POST",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + detectorId + "/_stop" + param,
                ImmutableMap.of(),
                "",
                null
            );
        return response;
    }

    public static Response deleteDetector(RestClient client, String detectorId) throws IOException {
        Response response = TestHelpers
            .makeRequest(
                client,
                "DELETE",
                TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + detectorId,
                ImmutableMap.of(),
                "",
                null
            );
        return response;
    }
}
