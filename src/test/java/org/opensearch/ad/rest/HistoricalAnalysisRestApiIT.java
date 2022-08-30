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

package org.opensearch.ad.rest;
/*


@Ignore
public class HistoricalAnalysisRestApiIT extends HistoricalAnalysisRestTestCase {

    @Before
    @Override
    public void setUp() throws Exception {
        super.categoryFieldDocCount = 3;
        super.setUp();
        updateClusterSettings(MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS.getKey(), 2);
        updateClusterSettings(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 5);
        updateClusterSettings(MAX_BATCH_TASK_PER_NODE.getKey(), 10);
    }

    public void testHistoricalAnalysisForSingleEntityDetector() throws Exception {
        List<String> startHistoricalAnalysisResult = startHistoricalAnalysis(0);
        String detectorId = startHistoricalAnalysisResult.get(0);
        String taskId = startHistoricalAnalysisResult.get(1);
        checkIfTaskCanFinishCorrectly(detectorId, taskId, HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS);
    }

    public void testHistoricalAnalysisForSingleEntityDetectorWithCustomResultIndex() throws Exception {
        String resultIndex = CommonName.CUSTOM_RESULT_INDEX_PREFIX + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        List<String> startHistoricalAnalysisResult = startHistoricalAnalysis(0, resultIndex);
        String detectorId = startHistoricalAnalysisResult.get(0);
        String taskId = startHistoricalAnalysisResult.get(1);
        checkIfTaskCanFinishCorrectly(detectorId, taskId, HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS);
        Response searchResponse = searchTaskResult(resultIndex, taskId);
        assertEquals("Search anomaly result failed", RestStatus.OK, TestHelpers.restStatus(searchResponse));
    }

    public void testHistoricalAnalysisForSingleCategoryHC() throws Exception {
        List<String> startHistoricalAnalysisResult = startHistoricalAnalysis(1);
        String detectorId = startHistoricalAnalysisResult.get(0);
        String taskId = startHistoricalAnalysisResult.get(1);
        checkIfTaskCanFinishCorrectly(detectorId, taskId, HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS);
    }

    public void testHistoricalAnalysisForMultiCategoryHC() throws Exception {
        List<String> startHistoricalAnalysisResult = startHistoricalAnalysis(2);
        String detectorId = startHistoricalAnalysisResult.get(0);
        String taskId = startHistoricalAnalysisResult.get(1);
        checkIfTaskCanFinishCorrectly(detectorId, taskId, HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS);
    }

    private void checkIfTaskCanFinishCorrectly(String detectorId, String taskId, Set<String> states) throws InterruptedException {
        List<Object> results = waitUntilTaskDone(detectorId);
        ADTaskProfile endTaskProfile = (ADTaskProfile) results.get(0);
        Integer retryCount = (Integer) results.get(1);
        ADTask stoppedAdTask = endTaskProfile.getAdTask();
        assertEquals(taskId, stoppedAdTask.getTaskId());
        if (retryCount < MAX_RETRY_TIMES) {
            // It's possible that historical analysis still running after max retry times
            assertTrue(states.contains(stoppedAdTask.getState()));
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> startHistoricalAnalysis(int categoryFieldSize) throws Exception {
        return startHistoricalAnalysis(categoryFieldSize, null);
    }

    @SuppressWarnings("unchecked")
    private List<String> startHistoricalAnalysis(int categoryFieldSize, String resultIndex) throws Exception {
        AnomalyDetector detector = createAnomalyDetector(categoryFieldSize, resultIndex);
        String detectorId = detector.getDetectorId();

        // start historical detector
        String taskId = startHistoricalAnalysis(detectorId);

        // get task profile
        ADTaskProfile adTaskProfile = waitUntilGetTaskProfile(detectorId);
        if (categoryFieldSize > 0) {
            if (!ADTaskState.RUNNING.name().equals(adTaskProfile.getAdTask().getState())) {
                adTaskProfile = (ADTaskProfile) waitUntilTaskReachState(detectorId, ImmutableSet.of(ADTaskState.RUNNING.name())).get(0);
            }
            assertEquals((int) Math.pow(categoryFieldDocCount, categoryFieldSize), adTaskProfile.getTotalEntitiesCount().intValue());
            assertTrue(adTaskProfile.getPendingEntitiesCount() > 0);
            assertTrue(adTaskProfile.getRunningEntitiesCount() > 0);
        }
        ADTask adTask = adTaskProfile.getAdTask();
        assertEquals(taskId, adTask.getTaskId());
        assertTrue(TestHelpers.HISTORICAL_ANALYSIS_RUNNING_STATS.contains(adTask.getState()));

        // get task stats
        Response statsResponse = TestHelpers.makeRequest(client(), "GET", AD_BASE_STATS_URI, ImmutableMap.of(), "", null);
        String statsResult = EntityUtils.toString(statsResponse.getEntity());
        Map<String, Object> stringObjectMap = TestHelpers.parseStatsResult(statsResult);
        String detectorCountState = categoryFieldSize > 0 ? MULTI_ENTITY_DETECTOR_COUNT.getName() : SINGLE_ENTITY_DETECTOR_COUNT.getName();
        assertTrue((long) stringObjectMap.get(detectorCountState) > 0);
        Map<String, Object> nodes = (Map<String, Object>) stringObjectMap.get("nodes");
        long totalBatchTaskExecution = 0;
        for (String key : nodes.keySet()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodes.get(key);
            totalBatchTaskExecution += (long) nodeStats.get(AD_TOTAL_BATCH_TASK_EXECUTION_COUNT.getName());
        }
        assertTrue(totalBatchTaskExecution > 0);

        // get detector with AD task
        ToXContentObject[] result = getHistoricalAnomalyDetector(detectorId, true, client());
        AnomalyDetector parsedDetector = (AnomalyDetector) result[0];
        AnomalyDetectorJob parsedJob = (AnomalyDetectorJob) result[1];
        ADTask parsedADTask = (ADTask) result[2];
        assertNull(parsedJob);
        assertNotNull(parsedDetector);
        assertNotNull(parsedADTask);
        assertEquals(taskId, parsedADTask.getTaskId());

        return ImmutableList.of(detectorId, taskId);
    }

    @SuppressWarnings("unchecked")
    public void testStopHistoricalAnalysis() throws Exception {
        // create historical detector
        AnomalyDetector detector = createAnomalyDetector();
        String detectorId = detector.getDetectorId();

        // start historical detector
        String taskId = startHistoricalAnalysis(detectorId);

        waitUntilGetTaskProfile(detectorId);

        // stop historical detector
        Response stopDetectorResponse = stopAnomalyDetector(detectorId, client(), false);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(stopDetectorResponse));

        // get task profile
        checkIfTaskCanFinishCorrectly(detectorId, taskId, ImmutableSet.of(ADTaskState.STOPPED.name()));
        updateClusterSettings(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1);

        waitUntilTaskDone(detectorId);

        // get AD stats
        Response statsResponse = TestHelpers.makeRequest(client(), "GET", AD_BASE_STATS_URI, ImmutableMap.of(), "", null);
        String statsResult = EntityUtils.toString(statsResponse.getEntity());
        Map<String, Object> stringObjectMap = TestHelpers.parseStatsResult(statsResult);
        assertTrue((long) stringObjectMap.get("single_entity_detector_count") > 0);
        Map<String, Object> nodes = (Map<String, Object>) stringObjectMap.get("nodes");
        long cancelledTaskCount = 0;
        for (String key : nodes.keySet()) {
            Map<String, Object> nodeStats = (Map<String, Object>) nodes.get(key);
            cancelledTaskCount += (long) nodeStats.get("ad_canceled_batch_task_count");
        }
        assertTrue(cancelledTaskCount >= 1);
    }

    public void testUpdateHistoricalAnalysis() throws IOException, IllegalAccessException {
        // create historical detector
        AnomalyDetector detector = createAnomalyDetector();
        String detectorId = detector.getDetectorId();

        // update historical detector
        AnomalyDetector newDetector = randomAnomalyDetector(detector);
        Response updateResponse = TestHelpers
            .makeRequest(
                client(),
                "PUT",
                TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "?refresh=true",
                ImmutableMap.of(),
                TestHelpers.toHttpEntity(newDetector),
                null
            );
        Map<String, Object> responseBody = entityAsMap(updateResponse);
        assertEquals(detector.getDetectorId(), responseBody.get("_id"));
        assertEquals((detector.getVersion().intValue() + 1), (int) responseBody.get("_version"));

        // get historical detector
        AnomalyDetector updatedDetector = getAnomalyDetector(detector.getDetectorId(), client());
        assertNotEquals(updatedDetector.getLastUpdateTime(), detector.getLastUpdateTime());
        assertEquals(newDetector.getName(), updatedDetector.getName());
        assertEquals(newDetector.getDescription(), updatedDetector.getDescription());
    }

    public void testUpdateRunningHistoricalAnalysis() throws Exception {
        // create historical detector
        AnomalyDetector detector = createAnomalyDetector();
        String detectorId = detector.getDetectorId();

        // start historical detector
        startHistoricalAnalysis(detectorId);

        // update historical detector
        AnomalyDetector newDetector = randomAnomalyDetector(detector);
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Detector is running",
                () -> TestHelpers
                    .makeRequest(
                        client(),
                        "PUT",
                        TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId + "?refresh=true",
                        ImmutableMap.of(),
                        TestHelpers.toHttpEntity(newDetector),
                        null
                    )
            );

        waitUntilTaskDone(detectorId);
    }

    // TODO: fix delete
    public void testDeleteHistoricalAnalysis() throws IOException, IllegalAccessException {
        // create historical detector
        AnomalyDetector detector = createAnomalyDetector();
        String detectorId = detector.getDetectorId();

        // delete detector
        Response response = TestHelpers
            .makeRequest(client(), "DELETE", TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId, ImmutableMap.of(), "", null);
        assertEquals(RestStatus.OK, TestHelpers.restStatus(response));
    }

    // TODO: fix flaky test
    @Ignore
    public void testDeleteRunningHistoricalDetector() throws Exception {
        // create historical detector
        AnomalyDetector detector = createAnomalyDetector();
        String detectorId = detector.getDetectorId();

        // start historical detector
        startHistoricalAnalysis(detectorId);

        // delete detector
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Detector is running",
                () -> TestHelpers
                    .makeRequest(client(), "DELETE", TestHelpers.AD_BASE_DETECTORS_URI + "/" + detectorId, ImmutableMap.of(), "", null)
            );

        waitUntilTaskDone(detectorId);
    }

    public void testSearchTasks() throws IOException, InterruptedException, IllegalAccessException {
        // create historical detector
        AnomalyDetector detector = createAnomalyDetector();
        String detectorId = detector.getDetectorId();

        // start historical detector
        String taskId = startHistoricalAnalysis(detectorId);

        waitUntilTaskDone(detectorId);

        String query = String.format(Locale.ROOT, "{\"query\":{\"term\":{\"detector_id\":{\"value\":\"%s\"}}}}", detectorId);
        Response response = TestHelpers
            .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI + "/tasks/_search", ImmutableMap.of(), query, null);
        String searchResult = EntityUtils.toString(response.getEntity());
        assertTrue(searchResult.contains(taskId));
        assertTrue(searchResult.contains(detector.getDetectorId()));
    }

    private AnomalyDetector randomAnomalyDetector(AnomalyDetector detector) {
        return new AnomalyDetector(
            detector.getDetectorId(),
            null,
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            detector.getTimeField(),
            detector.getIndices(),
            detector.getFeatureAttributes(),
            detector.getFilterQuery(),
            detector.getDetectionInterval(),
            detector.getWindowDelay(),
            detector.getShingleSize(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime(),
            detector.getCategoryField(),
            detector.getUser(),
            detector.getResultIndex()
        );
    }

}
*/
