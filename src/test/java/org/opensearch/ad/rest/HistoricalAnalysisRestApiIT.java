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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MODEL_MAX_SIZE_PERCENTAGE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS;
import static org.opensearch.timeseries.TestHelpers.AD_BASE_STATS_URI;
import static org.opensearch.timeseries.TestHelpers.HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS;
import static org.opensearch.timeseries.stats.StatNames.AD_TOTAL_BATCH_TASK_EXECUTION_COUNT;
import static org.opensearch.timeseries.stats.StatNames.HC_DETECTOR_COUNT;
import static org.opensearch.timeseries.stats.StatNames.SINGLE_STREAM_DETECTOR_COUNT;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.opensearch.ad.HistoricalAnalysisRestTestCase;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.timeseries.TaskProfile;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class HistoricalAnalysisRestApiIT extends HistoricalAnalysisRestTestCase {

    @Before
    @Override
    public void setUp() throws Exception {
        super.categoryFieldDocCount = 3;
        super.setUp();
        updateClusterSettings(MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS.getKey(), 2);
        updateClusterSettings(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 5);
        updateClusterSettings(MAX_BATCH_TASK_PER_NODE.getKey(), 10);
        // increase the AD memory percentage. Since enabling jacoco coverage instrumentation,
        // the memory is not enough to finish HistoricalAnalysisRestApiIT.
        updateClusterSettings(AD_MODEL_MAX_SIZE_PERCENTAGE.getKey(), 0.5);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        updateClusterSettings(AD_MODEL_MAX_SIZE_PERCENTAGE.getKey(), 0.1);
        super.tearDown();
    }

    public void testHistoricalAnalysisForSingleEntityDetector() throws Exception {
        List<String> startHistoricalAnalysisResult = startHistoricalAnalysis(0);
        String detectorId = startHistoricalAnalysisResult.get(0);
        String taskId = startHistoricalAnalysisResult.get(1);
        checkIfTaskCanFinishCorrectly(detectorId, taskId, HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS);
    }

    public void testHistoricalAnalysisForSingleEntityDetectorWithCustomResultIndex() throws Exception {
        String resultIndex = ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
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
        List<Object> results = waitUntilTaskReachState(detectorId, states);
        TaskProfile<ADTask> endTaskProfile = (TaskProfile<ADTask>) results.get(0);
        Integer retryCount = (Integer) results.get(1);
        ADTask stoppedAdTask = endTaskProfile.getTask();
        assertEquals(taskId, stoppedAdTask.getTaskId());
        if (retryCount < MAX_RETRY_TIMES) {
            // It's possible that historical analysis still running after max retry times
            assertTrue(
                "expect: " + stoppedAdTask.getState() + ", but got " + stoppedAdTask.getState(),
                states.contains(stoppedAdTask.getState())
            );
        }
    }

    private List<String> startHistoricalAnalysis(int categoryFieldSize) throws Exception {
        return startHistoricalAnalysis(categoryFieldSize, null);
    }

    @SuppressWarnings("unchecked")
    private List<String> startHistoricalAnalysis(int categoryFieldSize, String resultIndex) throws Exception {
        AnomalyDetector detector = createAnomalyDetector(categoryFieldSize, resultIndex);
        String detectorId = detector.getId();

        // start historical detector
        String taskId = startHistoricalAnalysis(detectorId);

        // get task profile
        ADTaskProfile adTaskProfile = waitUntilGetTaskProfile(detectorId);
        if (categoryFieldSize > 0) {
            if (!TaskState.RUNNING.name().equals(adTaskProfile.getTask().getState())) {
                adTaskProfile = (ADTaskProfile) waitUntilTaskReachState(detectorId, ImmutableSet.of(TaskState.RUNNING.name())).get(0);
            }
            if (adTaskProfile == null
                || (int) Math.pow(categoryFieldDocCount, categoryFieldSize) != adTaskProfile.getTotalEntitiesCount().intValue()) {
                adTaskProfile = (ADTaskProfile) waitUntilTaskReachNumberOfEntities(detectorId, categoryFieldDocCount).get(0);
            }
            assertEquals((int) Math.pow(categoryFieldDocCount, categoryFieldSize), adTaskProfile.getTotalEntitiesCount().intValue());
            assertTrue(adTaskProfile.getPendingEntitiesCount() > 0);
            assertTrue(adTaskProfile.getRunningEntitiesCount() > 0);
        }
        ADTask adTask = adTaskProfile.getTask();
        assertEquals(taskId, adTask.getTaskId());
        assertTrue(TestHelpers.HISTORICAL_ANALYSIS_RUNNING_STATS.contains(adTask.getState()));

        // get task stats
        Response statsResponse = TestHelpers.makeRequest(client(), "GET", AD_BASE_STATS_URI, ImmutableMap.of(), "", null);
        String statsResult = EntityUtils.toString(statsResponse.getEntity());
        Map<String, Object> stringObjectMap = TestHelpers.parseStatsResult(statsResult);
        String detectorCountState = categoryFieldSize > 0 ? HC_DETECTOR_COUNT.getName() : SINGLE_STREAM_DETECTOR_COUNT.getName();
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
        Job parsedJob = (Job) result[1];
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
        String detectorId = detector.getId();

        // start historical detector
        String taskId = startHistoricalAnalysis(detectorId);

        waitUntilGetTaskProfile(detectorId);

        // stop historical detector
        try {
            Response stopDetectorResponse = stopAnomalyDetector(detectorId, client(), false);
            assertEquals(RestStatus.OK, TestHelpers.restStatus(stopDetectorResponse));
        } catch (Exception e) {
            // it is possible the tasks has already stopped
            assertTrue("expected No running task found but actual is " + e.getMessage(), e.getMessage().contains("No running task found"));
        }

        // get task profile
        checkIfTaskCanFinishCorrectly(detectorId, taskId, ImmutableSet.of(TaskState.STOPPED.name()));
        updateClusterSettings(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1);

        waitUntilTaskDone(detectorId);

        // get AD stats
        Response statsResponse = TestHelpers.makeRequest(client(), "GET", AD_BASE_STATS_URI, ImmutableMap.of(), "", null);
        String statsResult = EntityUtils.toString(statsResponse.getEntity());
        Map<String, Object> stringObjectMap = TestHelpers.parseStatsResult(statsResult);
        assertTrue((long) stringObjectMap.get("single_stream_detector_count") > 0);
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
        String detectorId = detector.getId();

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
        assertEquals(detector.getId(), responseBody.get("_id"));
        assertEquals((detector.getVersion().intValue() + 1), (int) responseBody.get("_version"));

        // get historical detector
        AnomalyDetector updatedDetector = getConfig(detector.getId(), client());
        assertNotEquals(updatedDetector.getLastUpdateTime(), detector.getLastUpdateTime());
        assertEquals(newDetector.getName(), updatedDetector.getName());
        assertEquals(newDetector.getDescription(), updatedDetector.getDescription());
    }

    public void testUpdateRunningHistoricalAnalysis() throws Exception {
        // create historical detector
        AnomalyDetector detector = createAnomalyDetector();
        String detectorId = detector.getId();

        // start historical detector
        startHistoricalAnalysis(detectorId);

        // update historical detector
        AnomalyDetector newDetector = randomAnomalyDetector(detector);
        TestHelpers
            .assertFailWith(
                ResponseException.class,
                "Historical is running",
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
        String detectorId = detector.getId();

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
        String detectorId = detector.getId();

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

    public void testSearchTasks() throws IOException, InterruptedException, IllegalAccessException, ParseException {
        // create historical detector
        AnomalyDetector detector = createAnomalyDetector();
        String detectorId = detector.getId();

        // start historical detector
        String taskId = startHistoricalAnalysis(detectorId);

        waitUntilTaskDone(detectorId);

        String query = String.format(Locale.ROOT, "{\"query\":{\"term\":{\"detector_id\":{\"value\":\"%s\"}}}}", detectorId);
        Response response = TestHelpers
            .makeRequest(client(), "POST", TestHelpers.AD_BASE_DETECTORS_URI + "/tasks/_search", ImmutableMap.of(), query, null);
        String searchResult = EntityUtils.toString(response.getEntity());
        assertTrue(searchResult.contains(taskId));
        assertTrue(searchResult.contains(detector.getId()));
    }

    private AnomalyDetector randomAnomalyDetector(AnomalyDetector detector) {
        return new AnomalyDetector(
            detector.getId(),
            null,
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            detector.getTimeField(),
            detector.getIndices(),
            detector.getFeatureAttributes(),
            detector.getFilterQuery(),
            detector.getInterval(),
            detector.getWindowDelay(),
            detector.getShingleSize(),
            detector.getUiMetadata(),
            detector.getSchemaVersion(),
            detector.getLastUpdateTime(),
            detector.getCategoryFields(),
            detector.getUser(),
            detector.getCustomResultIndexOrAlias(),
            detector.getImputationOption(),
            randomIntBetween(1, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            detector.getCustomResultIndexMinSize(),
            detector.getCustomResultIndexMinAge(),
            detector.getCustomResultIndexTTL(),
            detector.getFlattenResultIndexMapping(),
            detector.getLastBreakingUIChangeTime()
        );
    }

}
