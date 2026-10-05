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
import static org.opensearch.ad.task.ADTaskManager.AD_TASK_LEAD_NODE_MODEL_ID;
import static org.opensearch.timeseries.TestHelpers.AD_BASE_STATS_URI;
import static org.opensearch.timeseries.TestHelpers.HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS;
import static org.opensearch.timeseries.stats.StatNames.AD_TOTAL_BATCH_TASK_EXECUTION_COUNT;
import static org.opensearch.timeseries.stats.StatNames.HC_DETECTOR_COUNT;
import static org.opensearch.timeseries.stats.StatNames.SINGLE_STREAM_DETECTOR_COUNT;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.opensearch.ad.HistoricalAnalysisRestTestCase;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.mock.model.MockSimpleLog;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.Action;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Condition;
import org.opensearch.ad.model.Rule;
import org.opensearch.ad.model.ThresholdType;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.timeseries.TaskProfile;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class HistoricalAnalysisRestApiIT extends HistoricalAnalysisRestTestCase {

    private static final int HASH_RING_VIRTUAL_NODE_COUNT = 100;
    private static final int MAX_DETECTOR_CREATION_ATTEMPTS = 100;

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

    /**
     * Unlike the ordinary historical-analysis tests, this regression deliberately assigns more task slots than there are
     * entities. A slot is the coordinator's reserved concurrency quota for the detector. A lane is the execution chain that
     * runs one entity and, from its completion callback, polls the next pending entity.
     *
     * With three nodes and ten batch-task slots per node, this test sets the per-detector limit to ten and creates nine
     * entities (three IP values times three category values). Slot allocation happens before top-entity discovery, so the
     * detector initially receives ten slots. After the first entity reaches a terminal result, eight entities remain:
     *
     * taskLaneLimit = min(unfinishedEntities=8, clusterSlots=30, perDetectorLimit=10) = 8
     * scaleDelta = taskLaneLimit(8) - assignedTaskSlots(10) = -2
     *
     * The absolute-threshold rule intentionally has a null operator and value because those fields do not apply to
     * ACTUAL_IS_OVER_EXPECTED. Before nullable operator transport was supported, serializing the detector for remote entity
     * dispatch threw a NullPointerException. After serialization succeeds, the other eight entity lanes become active. The
     * first completion scales the assigned slots from ten to eight and stops only its own now-unneeded lane because all eight
     * remaining slots are occupied.
     *
     * The other multi-category historical test uses the default per-detector limit of two. After its first entity finishes,
     * taskLaneLimit and assignedTaskSlots both remain two, so scaleDelta is zero while pending work remains. It therefore does
     * not exercise this scale-down condition. This REST test verifies nullable-rule transport and the end-to-end terminal
     * state during scale-down. The focused ADTaskManagerTests force the original stranded state—negative scaleDelta, pending
     * entities, and no active lanes—and verify that the current lane continues.
     */
    public void testScaleDownWithNullableRuleCompletes() throws Exception {
        Map<String, HttpHost> nodeHosts = getNodeHosts();
        assumeTrue("This regression requires at least three OpenSearch nodes", nodeHosts.size() >= 3);
        updateClusterSettings(MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS.getKey(), 10);
        updateClusterSettings(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1);
        try {
            String indexName = "test_hcad_scale_down_" + randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
            ingestSimpleMockLog(indexName, 1, 50, 1, i -> i.doubleValue(), 3, 3);

            String leadNodeId = getOwningNodeId(AD_TASK_LEAD_NODE_MODEL_ID, nodeHosts.keySet());
            String detectorId = createDetectorOwnedByNode(indexName, leadNodeId, nodeHosts.keySet());

            Instant endTime = Instant.now().truncatedTo(ChronoUnit.SECONDS);
            DateRange dateRange = new DateRange(endTime.minus(2, ChronoUnit.DAYS), endTime);
            String taskId;
            try (RestClient leadNodeClient = buildClient(restClientSettings(), new HttpHost[] { nodeHosts.get(leadNodeId) })) {
                Response startResponse = startAnomalyDetector(detectorId, dateRange, leadNodeClient);
                taskId = (String) responseAsMap(startResponse).get("_id");
            }
            assertNotNull(taskId);

            ADTaskProfile completedProfile = waitForHistoricalTaskCompletion(detectorId);
            assertEquals(taskId, completedProfile.getTask().getTaskId());
            assertEquals(1.0f, completedProfile.getTask().getTaskProgress(), 0.0f);
            assertTrue(completedProfile.getPendingEntitiesCount() == null || completedProfile.getPendingEntitiesCount().intValue() == 0);
            assertTrue(completedProfile.getRunningEntitiesCount() == null || completedProfile.getRunningEntitiesCount().intValue() == 0);
            assertTrue(HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(completedProfile.getTask().getState()));
        } finally {
            updateClusterSettings(MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS.getKey(), 2);
            updateClusterSettings(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 5);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, HttpHost> getNodeHosts() throws Exception {
        Response nodesResponse = TestHelpers.makeRequest(client(), "GET", "/_nodes/http", ImmutableMap.of(), "", null);
        Map<String, Object> nodes = (Map<String, Object>) entityAsMap(nodesResponse).get("nodes");
        Map<String, HttpHost> nodeHosts = new TreeMap<>();
        for (Map.Entry<String, Object> nodeEntry : nodes.entrySet()) {
            Map<String, Object> node = (Map<String, Object>) nodeEntry.getValue();
            Map<String, Object> http = (Map<String, Object>) node.get("http");
            nodeHosts.put(nodeEntry.getKey(), HttpHost.create(getProtocol() + "://" + http.get("publish_address")));
        }
        return nodeHosts;
    }

    private String createDetectorOwnedByNode(String indexName, String targetNodeId, Collection<String> nodeIds) throws Exception {
        for (int attempt = 0; attempt < MAX_DETECTOR_CREATION_ATTEMPTS; attempt++) {
            AnomalyDetector detector = createDetectorWithNullableAbsoluteThresholdRule(indexName);
            Response createResponse = TestHelpers
                .makeRequest(
                    client(),
                    "POST",
                    TestHelpers.AD_BASE_DETECTORS_URI,
                    ImmutableMap.of(),
                    TestHelpers.toHttpEntity(detector),
                    null
                );
            assertEquals(RestStatus.CREATED, TestHelpers.restStatus(createResponse));
            String detectorId = (String) entityAsMap(createResponse).get("_id");
            if (targetNodeId.equals(getOwningNodeId(detectorId, nodeIds))) {
                return detectorId;
            }
        }
        fail("Could not create a detector owned by AD task lead node " + targetNodeId);
        return null;
    }

    private AnomalyDetector createDetectorWithNullableAbsoluteThresholdRule(String indexName) throws IOException {
        AggregationBuilder aggregation = TestHelpers
            .parseAggregation("{\"max_value\":{\"max\":{\"field\":\"" + MockSimpleLog.VALUE_FIELD + "\"}}}");
        Feature feature = new Feature(randomAlphaOfLength(5), "max_value", true, aggregation);
        Condition condition = new Condition(feature.getName(), ThresholdType.ACTUAL_IS_OVER_EXPECTED, null, null);
        Rule rule = new Rule(Action.IGNORE_ANOMALY, ImmutableList.of(condition));
        return TestHelpers.AnomalyDetectorBuilder
            .newInstance(1)
            .setName("scale-down-regression-" + randomAlphaOfLength(8))
            .setDescription("Exercises nullable rule transport during task-slot scale-down")
            .setTimeField(MockSimpleLog.TIME_FIELD)
            .setIndices(ImmutableList.of(indexName))
            .setFeatureAttributes(ImmutableList.of(feature))
            .setFilterQuery(QueryBuilders.matchAllQuery())
            .setDetectionInterval(new IntervalTimeConfiguration(1, ChronoUnit.MINUTES))
            .setWindowDelay(new IntervalTimeConfiguration(0, ChronoUnit.MINUTES))
            .setShingleSize(8)
            .setSchemaVersion(0)
            .setCategoryFields(ImmutableList.of(MockSimpleLog.IP_FIELD, MockSimpleLog.CATEGORY_FIELD))
            .setUser(null)
            .setRules(ImmutableList.of(rule))
            .build();
    }

    private String getOwningNodeId(String modelId, Collection<String> nodeIds) {
        TreeMap<Integer, String> hashRing = new TreeMap<>();
        for (String nodeId : nodeIds) {
            for (int i = 0; i < HASH_RING_VIRTUAL_NODE_COUNT; i++) {
                hashRing.put(Murmur3HashFunction.hash(nodeId + i), nodeId);
            }
        }
        Map.Entry<Integer, String> owningEntry = hashRing.higherEntry(Murmur3HashFunction.hash(modelId));
        return (owningEntry == null ? hashRing.firstEntry() : owningEntry).getValue();
    }

    private ADTaskProfile waitForHistoricalTaskCompletion(String detectorId) throws Exception {
        long timeout = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(60);
        ADTaskProfile profile = null;
        do {
            try {
                profile = getADTaskProfile(detectorId);
                if (profile != null && HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(profile.getTask().getState())) {
                    return profile;
                }
            } catch (Exception e) {
                logger.debug("Historical task profile is not available yet for detector {}", detectorId, e);
            }
            Thread.sleep(200);
        } while (System.nanoTime() < timeout);

        assertNotNull("Historical task profile was never created for detector " + detectorId, profile);
        fail(
            "Historical task did not complete. state="
                + profile.getTask().getState()
                + ", progress="
                + profile.getTask().getTaskProgress()
                + ", pending="
                + profile.getPendingEntitiesCount()
                + ", running="
                + profile.getRunningEntitiesCount()
                + ", slots="
                + profile.getDetectorTaskSlots()
        );
        return null;
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
            randomIntBetween(2, 10000),
            randomInt(TimeSeriesSettings.MAX_SHINGLE_SIZE / 2),
            randomIntBetween(1, 1000),
            null,
            detector.getCustomResultIndexMinSize(),
            detector.getCustomResultIndexMinAge(),
            detector.getCustomResultIndexTTL(),
            detector.getFlattenResultIndexMapping(),
            detector.getLastBreakingUIChangeTime(),
            detector.getFrequency(),
            detector.getAutoCreated(),
            null,
            null
        );
    }

}
