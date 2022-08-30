/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad.bwc;

import static org.opensearch.ad.rest.ADRestTestUtils.countADResultOfDetector;
import static org.opensearch.ad.rest.ADRestTestUtils.deleteDetector;
import static org.opensearch.ad.rest.ADRestTestUtils.searchLatestAdTaskOfDetector;
import static org.opensearch.ad.rest.ADRestTestUtils.startHistoricalAnalysis;
import static org.opensearch.ad.rest.ADRestTestUtils.waitUntilTaskDone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.rest.OpenSearchRestTestCase;

public class ADBackwardsCompatibilityIT extends OpenSearchRestTestCase {

    private static final ClusterType CLUSTER_TYPE = ClusterType.parse(System.getProperty("tests.rest.bwcsuite"));
    private static final String CLUSTER_NAME = System.getProperty("tests.clustername");
    private static final String MIXED_CLUSTER_TEST_ROUND = System.getProperty("tests.rest.bwcsuite_round");
    private String dataIndexName = "test_data_for_ad_plugin";
    private int detectionIntervalInMinutes = 1;
    private int windowDelayIntervalInMinutes = 1;
    private String aggregationMethod = "sum";
    private int totalDocsPerCategory = 10_000;
    private int categoryFieldSize = 2;
    private List<String> runningRealtimeDetectors;
    private List<String> historicalDetectors;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.runningRealtimeDetectors = new ArrayList<>();
        this.historicalDetectors = new ArrayList<>();
    }

    @Override
    protected final boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected final boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected final Settings restClientSettings() {
        return Settings
            .builder()
            .put(super.restClientSettings())
            // increase the timeout here to 90 seconds to handle long waits for a green
            // cluster health. the waits for green need to be longer than a minute to
            // account for delayed shards
            .put(OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
            .build();
    }

    private enum ClusterType {
        OLD,
        MIXED,
        UPGRADED;

        public static ClusterType parse(String value) {
            switch (value) {
                case "old_cluster":
                    return OLD;
                case "mixed_cluster":
                    return MIXED;
                case "upgraded_cluster":
                    return UPGRADED;
                default:
                    throw new AssertionError("unknown cluster type: " + value);
            }
        }
    }

    @SuppressWarnings("unchecked")
    // public void testBackwardsCompatibility() throws Exception {
    // String uri = getUri();
    // Map<String, Map<String, Object>> responseMap = (Map<String, Map<String, Object>>) getAsMap(uri).get("nodes");
    // for (Map<String, Object> response : responseMap.values()) {
    // List<Map<String, Object>> plugins = (List<Map<String, Object>>) response.get("plugins");
    // Set<Object> pluginNames = plugins.stream().map(map -> map.get("name")).collect(Collectors.toSet());
    // switch (CLUSTER_TYPE) {
    // case OLD:
    // // Ingest test data
    // ingestTestDataForHistoricalAnalysis(
    // client(),
    // dataIndexName,
    // detectionIntervalInMinutes,
    // true,
    // 10,
    // totalDocsPerCategory,
    // categoryFieldSize
    // );
    // assertEquals(totalDocsPerCategory * categoryFieldSize * 2, getDocCountOfIndex(client(), dataIndexName));
    // Assert.assertTrue(pluginNames.contains("opendistro-anomaly-detection"));
    // Assert.assertTrue(pluginNames.contains("opendistro-job-scheduler"));
    //
    // // Create single entity detector and start realtime job
    // createRealtimeAnomalyDetectorsAndStart(SINGLE_ENTITY_DETECTOR);
    //
    // // Create single category HC detector and start realtime job
    // createRealtimeAnomalyDetectorsAndStart(SINGLE_CATEGORY_HC_DETECTOR);
    //
    // // Create single entity historical detector and start historical analysis
    // createHistoricalAnomalyDetectorsAndStart();
    //
    // // Verify cluster has correct number of detectors now
    // verifyAnomalyDetectorCount(TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI, 3);
    // // Verify cluster has correct number of detectors with search detector API
    // assertEquals(3, countDetectors(client(), null));
    //
    // // Verify all realtime detectors are running realtime job
    // verifyAllRealtimeJobsRunning();
    // break;
    // case MIXED:
    // // TODO: We have no way to specify whether send request to old node or new node now.
    // // Add more test later when it's possible to specify request node.
    // Assert.assertTrue(pluginNames.contains("opensearch-anomaly-detection"));
    // Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
    //
    // // Create single entity detector and start realtime job
    // createRealtimeAnomalyDetectorsAndStart(SINGLE_ENTITY_DETECTOR);
    //
    // // Create single category HC detector and start realtime job
    // createRealtimeAnomalyDetectorsAndStart(SINGLE_CATEGORY_HC_DETECTOR);
    //
    // int mixedClusterTestRound = getMixedClusterTestRound();
    // int numberOfDetector = 3 + 2 * mixedClusterTestRound;
    // // Verify cluster has correct number of detectors now
    // verifyAnomalyDetectorCount(TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI, numberOfDetector);
    // // Verify cluster has correct number of detectors with search detector API
    // assertEquals(numberOfDetector, countDetectors(client(), null));
    //
    // // Verify all realtime detectors are running realtime job
    // verifyAllRealtimeJobsRunning();
    // break;
    // case UPGRADED:
    // // This branch is for testing full upgraded cluster. That means all nodes in cluster are running
    // // latest AD version.
    // Assert.assertTrue(pluginNames.contains("opensearch-anomaly-detection"));
    // Assert.assertTrue(pluginNames.contains("opensearch-job-scheduler"));
    //
    // Map<ADRestTestUtils.DetectorType, String> detectors = new HashMap<>();
    //
    // // Create single entity detector and start realtime job
    // List<String> singleEntityDetectorResults = createRealtimeAnomalyDetectorsAndStart(SINGLE_ENTITY_DETECTOR);
    // detectors.put(SINGLE_ENTITY_DETECTOR, singleEntityDetectorResults.get(0));
    // // Start historical analysis for single entity detector
    // startHistoricalAnalysisOnNewNode(singleEntityDetectorResults.get(0), ADTaskType.HISTORICAL_SINGLE_ENTITY.name());
    //
    // // Create single category HC detector and start realtime job
    // List<String> singleCategoryHCResults = createRealtimeAnomalyDetectorsAndStart(SINGLE_CATEGORY_HC_DETECTOR);
    // detectors.put(SINGLE_CATEGORY_HC_DETECTOR, singleEntityDetectorResults.get(0));
    // // Start historical analysis for single category HC detector
    // startHistoricalAnalysisOnNewNode(singleCategoryHCResults.get(0), ADTaskType.HISTORICAL_HC_DETECTOR.name());
    //
    // // Create multi category HC detector and start realtime job
    // List<String> multiCategoryHCResults = createRealtimeAnomalyDetectorsAndStart(MULTI_CATEGORY_HC_DETECTOR);
    // detectors.put(MULTI_CATEGORY_HC_DETECTOR, singleEntityDetectorResults.get(0));
    // // Start historical analysis for multi category HC detector
    // startHistoricalAnalysisOnNewNode(multiCategoryHCResults.get(0), ADTaskType.HISTORICAL_HC_DETECTOR.name());
    //
    // // Verify cluster has correct number of detectors now
    // verifyAnomalyDetectorCount(TestHelpers.AD_BASE_DETECTORS_URI, 6);
    // // Verify cluster has correct number of detectors with search detector API
    // assertEquals(6, countDetectors(client(), null));
    //
    // // Start realtime job for historical detector created on old cluster and check realtime job running.
    // startRealtimeJobForHistoricalDetectorOnNewNode();
    //
    // // Verify all realtime detectors are running realtime job
    // verifyAllRealtimeJobsRunning();
    // // Verify realtime and historical task exists for all running realtime detector
    // verifyAdTasks();
    //
    // // Stop and delete detector
    // stopAndDeleteDetectors();
    // break;
    // }
    // break;
    // }
    // }

    private int getMixedClusterTestRound() {
        int mixedClusterTestRound = 0;
        switch (MIXED_CLUSTER_TEST_ROUND) {
            case "first":
                mixedClusterTestRound = 1;
                break;
            case "second":
                mixedClusterTestRound = 2;
                break;
            case "third":
                mixedClusterTestRound = 3;
                break;
            default:
                break;
        }
        return mixedClusterTestRound;
    }

    // private void verifyAdTasks() throws InterruptedException, IOException {
    // boolean realtimeTaskMissing = false;
    // int i = 0;
    // int maxRetryTimes = 10;
    // do {
    // i++;
    // for (String detectorId : runningRealtimeDetectors) {
    // Map<String, Object> jobAndTask = getDetectorWithJobAndTask(client(), detectorId);
    // AnomalyDetectorJob job = (AnomalyDetectorJob) jobAndTask.get(ANOMALY_DETECTOR_JOB);
    // ADTask historicalTask = (ADTask) jobAndTask.get(HISTORICAL_ANALYSIS_TASK);
    // ADTask realtimeTask = (ADTask) jobAndTask.get(REALTIME_TASK);
    // assertTrue(job.isEnabled());
    // assertNotNull(historicalTask);
    // if (realtimeTask == null) {
    // realtimeTaskMissing = true;
    // }
    // if (i >= maxRetryTimes) {
    // assertNotNull(realtimeTask);
    // assertFalse(realtimeTask.isDone());
    // }
    // }
    // if (realtimeTaskMissing) {
    // Thread.sleep(10_000);// sleep 10 seconds to wait for realtime job to backfill realtime task
    // }
    // } while (realtimeTaskMissing && i < maxRetryTimes);
    // }

    // private void stopAndDeleteDetectors() throws Exception {
    // for (String detectorId : runningRealtimeDetectors) {
    // deleteRunningDetector(detectorId);
    // Response stopRealtimeJobResponse = stopRealtimeJob(client(), detectorId);
    // assertEquals(RestStatus.OK, TestHelpers.restStatus(stopRealtimeJobResponse));
    // try {
    // Response stopHistoricalAnalysisResponse = stopHistoricalAnalysis(client(), detectorId);
    // assertEquals(RestStatus.OK, TestHelpers.restStatus(stopHistoricalAnalysisResponse));
    // } catch (Exception e) {
    // if (!ExceptionUtil.getErrorMessage(e).contains("No running task found")) {
    // throw e;
    // }
    // }
    // Map<String, Object> jobAndTask = getDetectorWithJobAndTask(client(), detectorId);
    // AnomalyDetectorJob job = (AnomalyDetectorJob) jobAndTask.get(ANOMALY_DETECTOR_JOB);
    // ADTask historicalAdTask = (ADTask) jobAndTask.get(HISTORICAL_ANALYSIS_TASK);
    // if (!job.isEnabled() && historicalAdTask.isDone()) {
    // Response deleteDetectorResponse = deleteDetector(client(), detectorId);
    // assertEquals(RestStatus.OK, TestHelpers.restStatus(deleteDetectorResponse));
    // }
    // }
    // }

    // This function can only run on new AD version(>=1.1).
    // TODO: execute this function on new node in mixed cluster when we have way to send request to specific node.
    private void startHistoricalAnalysisOnNewNode(String detectorId, String taskType) throws IOException, InterruptedException {
        String taskId = startHistoricalAnalysis(client(), detectorId);
        deleteRunningDetector(detectorId);
        waitUntilTaskDone(client(), detectorId);
        List<ADTask> adTasks = searchLatestAdTaskOfDetector(client(), detectorId, taskType);
        assertEquals(1, adTasks.size());
        assertEquals(taskId, adTasks.get(0).getTaskId());
        int adResultCount = countADResultOfDetector(client(), detectorId, taskId);
        assertTrue(adResultCount > 0);
    }

    // This function can only run on new AD version(>=1.1).
    // TODO: execute this function on new node in mixed cluster when we have way to send request to specific node.
    // private void startRealtimeJobForHistoricalDetectorOnNewNode() throws IOException {
    // for (String detectorId : historicalDetectors) {
    // String jobId = startAnomalyDetectorDirectly(client(), detectorId);
    // assertEquals(detectorId, jobId);
    // Map<String, Object> jobAndTask = getDetectorWithJobAndTask(client(), detectorId);
    // AnomalyDetectorJob detectorJob = (AnomalyDetectorJob) jobAndTask.get(ANOMALY_DETECTOR_JOB);
    // assertTrue(detectorJob.isEnabled());
    // runningRealtimeDetectors.add(detectorId);
    // }
    // }

    // private void verifyAllRealtimeJobsRunning() throws IOException {
    // for (String detectorId : runningRealtimeDetectors) {
    // Map<String, Object> jobAndTask = getDetectorWithJobAndTask(client(), detectorId);
    // AnomalyDetectorJob detectorJob = (AnomalyDetectorJob) jobAndTask.get(ANOMALY_DETECTOR_JOB);
    // assertTrue(detectorJob.isEnabled());
    // }
    // }

    private String getUri() {
        switch (CLUSTER_TYPE) {
            case OLD:
                return "_nodes/" + CLUSTER_NAME + "-0/plugins";
            case MIXED:
                String round = System.getProperty("tests.rest.bwcsuite_round");
                if (round.equals("second")) {
                    return "_nodes/" + CLUSTER_NAME + "-1/plugins";
                } else if (round.equals("third")) {
                    return "_nodes/" + CLUSTER_NAME + "-2/plugins";
                } else {
                    return "_nodes/" + CLUSTER_NAME + "-0/plugins";
                }
            case UPGRADED:
                return "_nodes/plugins";
            default:
                throw new AssertionError("unknown cluster type: " + CLUSTER_TYPE);
        }
    }

    // private List<String> createRealtimeAnomalyDetectorsAndStart(ADRestTestUtils.DetectorType detectorType) throws Exception {
    // switch (detectorType) {
    // case SINGLE_ENTITY_DETECTOR:
    // // Create single flow detector
    // Response singleFlowDetectorResponse = createAnomalyDetector(
    // client(),
    // dataIndexName,
    // MockSimpleLog.TIME_FIELD,
    // detectionIntervalInMinutes,
    // windowDelayIntervalInMinutes,
    // MockSimpleLog.VALUE_FIELD,
    // aggregationMethod,
    // null,
    // null
    // );
    // return startAnomalyDetector(singleFlowDetectorResponse, false);
    // case SINGLE_CATEGORY_HC_DETECTOR:
    // // Create single category HC detector
    // Response singleCategoryHCDetectorResponse = createAnomalyDetector(
    // client(),
    // dataIndexName,
    // MockSimpleLog.TIME_FIELD,
    // detectionIntervalInMinutes,
    // windowDelayIntervalInMinutes,
    // MockSimpleLog.VALUE_FIELD,
    // aggregationMethod,
    // null,
    // ImmutableList.of(MockSimpleLog.CATEGORY_FIELD)
    // );
    // return startAnomalyDetector(singleCategoryHCDetectorResponse, false);
    // case MULTI_CATEGORY_HC_DETECTOR:
    // // Create multi-category HC detector
    // Response multiCategoryHCDetectorResponse = createAnomalyDetector(
    // client(),
    // dataIndexName,
    // MockSimpleLog.TIME_FIELD,
    // detectionIntervalInMinutes,
    // windowDelayIntervalInMinutes,
    // MockSimpleLog.VALUE_FIELD,
    // aggregationMethod,
    // null,
    // ImmutableList.of(MockSimpleLog.IP_FIELD, MockSimpleLog.CATEGORY_FIELD)
    // );
    // return startAnomalyDetector(multiCategoryHCDetectorResponse, false);
    // default:
    // return null;
    // }
    // }

    @SuppressWarnings("unchecked")
    // private void createHistoricalAnomalyDetectorsAndStart() throws Exception {
    // // only support single entity for historical detector
    // Response historicalSingleFlowDetectorResponse = createAnomalyDetector(
    // client(),
    // dataIndexName,
    // MockSimpleLog.TIME_FIELD,
    // detectionIntervalInMinutes,
    // windowDelayIntervalInMinutes,
    // MockSimpleLog.VALUE_FIELD,
    // aggregationMethod,
    // null,
    // null,
    // true
    // );
    // List<String> historicalDetectorStartResult = startAnomalyDetector(historicalSingleFlowDetectorResponse, true);
    // String detectorId = historicalDetectorStartResult.get(0);
    // String taskId = historicalDetectorStartResult.get(1);
    // deleteRunningDetector(detectorId);
    // waitUntilTaskDone(client(), detectorId);
    // List<ADTask> adTasks = searchLatestAdTaskOfDetector(client(), detectorId, ADTaskType.HISTORICAL.name());
    // assertEquals(1, adTasks.size());
    // assertEquals(taskId, adTasks.get(0).getTaskId());
    // int adResultCount = countADResultOfDetector(client(), detectorId, taskId);
    // assertTrue(adResultCount > 0);
    // }

    private void deleteRunningDetector(String detectorId) {
        try {
            deleteDetector(client(), detectorId);
        } catch (Exception e) {
            assertTrue(ExceptionUtil.getErrorMessage(e).contains("running"));
        }
    }

    // private List<String> startAnomalyDetector(Response response, boolean historicalDetector) throws IOException {
    // // verify that the detector is created
    // assertEquals("Create anomaly detector failed", RestStatus.CREATED, TestHelpers.restStatus(response));
    // Map<String, Object> responseMap = entityAsMap(response);
    // String detectorId = (String) responseMap.get("_id");
    // int version = (int) responseMap.get("_version");
    // assertNotEquals("response is missing Id", AnomalyDetector.NO_ID, detectorId);
    // assertTrue("incorrect version", version > 0);
    //
    // Response startDetectorResponse = TestHelpers
    // .makeRequest(
    // client(),
    // "POST",
    // TestHelpers.LEGACY_OPENDISTRO_AD_BASE_DETECTORS_URI + "/" + detectorId + "/_start",
    // ImmutableMap.of(),
    // (HttpEntity) null,
    // null
    // );
    // Map<String, Object> startDetectorResponseMap = responseAsMap(startDetectorResponse);
    // String taskOrJobId = (String) startDetectorResponseMap.get("_id");
    // assertNotNull(taskOrJobId);
    //
    // if (!historicalDetector) {
    // Map<String, Object> jobAndTask = getDetectorWithJobAndTask(client(), detectorId);
    // AnomalyDetectorJob job = (AnomalyDetectorJob) jobAndTask.get(ANOMALY_DETECTOR_JOB);
    // assertTrue(job.isEnabled());
    // runningRealtimeDetectors.add(detectorId);
    // } else {
    // historicalDetectors.add(detectorId);
    // }
    // return ImmutableList.of(detectorId, taskOrJobId);
    // }

    private void verifyAnomalyDetectorCount(String uri, long expectedCount) throws Exception {
        Response response = TestHelpers.makeRequest(client(), "GET", uri + "/" + RestHandlerUtils.COUNT, null, "", null);
        Map<String, Object> responseMap = entityAsMap(response);
        Integer count = (Integer) responseMap.get("count");
        assertEquals(expectedCount, (long) count);
    }

}
