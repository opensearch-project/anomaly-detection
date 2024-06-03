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

package org.opensearch.timeseries.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.opensearch.timeseries.TestHelpers.HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS;
import static org.opensearch.timeseries.constant.CommonMessages.CONFIG_IS_RUNNING;
import static org.opensearch.timeseries.util.RestHandlerUtils.PROFILE;
import static org.opensearch.timeseries.util.RestHandlerUtils.START_JOB;
import static org.opensearch.timeseries.util.RestHandlerUtils.STOP_JOB;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.HistoricalAnalysisIntegTestCase;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.mock.model.MockSimpleLog;
import org.opensearch.ad.mock.transport.MockAnomalyDetectorJobAction;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.transport.AnomalyDetectorJobAction;
import org.opensearch.ad.transport.GetAnomalyDetectorAction;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.ad.transport.StatsAnomalyDetectorAction;
import org.opensearch.client.Client;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.timeseries.TaskProfile;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.stats.StatNames;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class AnomalyDetectorJobTransportActionTests extends HistoricalAnalysisIntegTestCase {
    private Instant startTime;
    private Instant endTime;
    private String type = "error";
    private int maxOldAdTaskDocsPerDetector = 2;
    private DateRange dateRange;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        endTime = Instant.now();
        dateRange = new DateRange(startTime, endTime);
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type, 2000);
        createDetectorIndex();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(MAX_BATCH_TASK_PER_NODE.getKey(), 1)
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), maxOldAdTaskDocsPerDetector)
            .build();
    }

    public void testDetectorIndexNotFound() {
        deleteDetectorIndex();
        String detectorId = randomAlphaOfLength(5);
        JobRequest request = startDetectorJobRequest(detectorId, dateRange);
        IndexNotFoundException exception = expectThrows(
            IndexNotFoundException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(3000)
        );
        assertTrue(exception.getMessage().contains("no such index [.opendistro-anomaly-detectors]"));
    }

    public void testDetectorNotFound() {
        String detectorId = randomAlphaOfLength(5);
        JobRequest request = startDetectorJobRequest(detectorId, dateRange);
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000)
        );
        assertTrue(exception.getMessage().contains(CommonMessages.FAIL_TO_FIND_CONFIG_MSG));
    }

    public void testValidHistoricalAnalysis() throws IOException, InterruptedException {
        ADTask adTask = startHistoricalAnalysis(startTime, endTime);
        Thread.sleep(10000);
        ADTask finishedTask = getADTask(adTask.getTaskId());
        assertTrue(HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(finishedTask.getState()));
    }

    public void testStartHistoricalAnalysisWithUser() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        JobRequest request = new JobRequest(detectorId, dateRange, true, START_JOB);
        Client nodeClient = getDataNodeClient();
        if (nodeClient != null) {
            JobResponse response = nodeClient.execute(MockAnomalyDetectorJobAction.INSTANCE, request).actionGet(100000);
            ADTask adTask = getADTask(response.getId());
            assertNotNull(adTask.getStartedBy());
            assertNotNull(adTask.getUser());
        }
    }

    public void testStartHistoricalAnalysisForSingleCategoryHCWithUser() throws IOException, InterruptedException {
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type + "1", DEFAULT_IP, 2000, false);
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type + "2", DEFAULT_IP, 2000, false);
        AnomalyDetector detector = TestHelpers
            .randomDetector(
                ImmutableList.of(maxValueFeature()),
                testIndex,
                detectionIntervalInMinutes,
                MockSimpleLog.TIME_FIELD,
                ImmutableList.of(categoryField)
            );
        String detectorId = createDetector(detector);
        JobRequest request = new JobRequest(detectorId, dateRange, true, START_JOB);
        Client nodeClient = getDataNodeClient();

        if (nodeClient != null) {
            JobResponse response = nodeClient.execute(MockAnomalyDetectorJobAction.INSTANCE, request).actionGet(100000);
            waitUntil(() -> {
                try {
                    ADTask task = getADTask(response.getId());
                    return HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(task.getState());
                } catch (IOException e) {
                    return false;
                }
            }, 60, TimeUnit.SECONDS);
            ADTask adTask = getADTask(response.getId());
            assertEquals(ADTaskType.HISTORICAL_HC_DETECTOR.toString(), adTask.getTaskType());
            assertTrue(HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(adTask.getState()));
            assertEquals(categoryField, adTask.getDetector().getCategoryFields().get(0));

            if (TaskState.FINISHED.name().equals(adTask.getState())) {
                List<ADTask> adTasks = searchADTasks(detectorId, true, 100);
                assertEquals(4, adTasks.size());
                List<ADTask> entityTasks = adTasks
                    .stream()
                    .filter(task -> ADTaskType.HISTORICAL_HC_ENTITY.name().equals(task.getTaskType()))
                    .collect(Collectors.toList());
                assertEquals(3, entityTasks.size());
            }
        }
    }

    public void testStartHistoricalAnalysisForMultiCategoryHCWithUser() throws IOException, InterruptedException {
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type + "1", DEFAULT_IP, 2000, false);
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type + "2", DEFAULT_IP, 2000, false);
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type + "3", "127.0.0.2", 2000, false);
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type + "4", "127.0.0.2", 2000, false);

        AnomalyDetector detector = TestHelpers
            .randomDetector(
                ImmutableList.of(maxValueFeature()),
                testIndex,
                detectionIntervalInMinutes,
                MockSimpleLog.TIME_FIELD,
                ImmutableList.of(categoryField, ipField)
            );
        String detectorId = createDetector(detector);
        JobRequest request = new JobRequest(detectorId, dateRange, true, START_JOB);
        Client nodeClient = getDataNodeClient();

        if (nodeClient != null) {
            JobResponse response = nodeClient.execute(MockAnomalyDetectorJobAction.INSTANCE, request).actionGet(100_000);
            String taskId = response.getId();

            waitUntil(() -> {
                try {
                    ADTask task = getADTask(taskId);
                    return !TestHelpers.HISTORICAL_ANALYSIS_RUNNING_STATS.contains(task.getState());
                } catch (IOException e) {
                    return false;
                }
            }, 90, TimeUnit.SECONDS);
            ADTask adTask = getADTask(taskId);
            assertEquals(ADTaskType.HISTORICAL_HC_DETECTOR.toString(), adTask.getTaskType());
            // Task may fail if memory circuit breaker triggered
            assertTrue(HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(adTask.getState()));
            assertEquals(categoryField, adTask.getDetector().getCategoryFields().get(0));
            assertEquals(ipField, adTask.getDetector().getCategoryFields().get(1));

            if (TaskState.FINISHED.name().equals(adTask.getState())) {
                List<ADTask> adTasks = searchADTasks(detectorId, taskId, true, 100);
                assertEquals(5, adTasks.size());
                List<ADTask> entityTasks = adTasks
                    .stream()
                    .filter(task -> ADTaskType.HISTORICAL_HC_ENTITY.name().equals(task.getTaskType()))
                    .collect(Collectors.toList());
                assertEquals(5, entityTasks.size());
            }
        }
    }

    public void testRunMultipleTasksForHistoricalAnalysis() throws IOException, InterruptedException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        JobRequest request = startDetectorJobRequest(detectorId, dateRange);
        JobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        assertNotNull(response.getId());
        OpenSearchStatusException exception = null;
        // Add retry to solve the flaky test
        for (int i = 0; i < 10; i++) {
            exception = expectThrows(
                OpenSearchStatusException.class,
                () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000)
            );
            if (exception.getMessage().contains(CONFIG_IS_RUNNING)) {
                break;
            } else {
                logger.error("Unexpected error happened when rerun detector", exception);
            }
            Thread.sleep(1000);
        }
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains(CONFIG_IS_RUNNING));
        assertEquals(CONFIG_IS_RUNNING, exception.getMessage());
        Thread.sleep(20000);
        List<ADTask> adTasks = searchADTasks(detectorId, null, 100);
        assertEquals(1, adTasks.size());
        assertTrue(HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(adTasks.get(0).getState()));
    }

    public void testRaceConditionByStartingMultipleTasks() throws IOException, InterruptedException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        JobRequest request = new JobRequest(detectorId, dateRange, true, START_JOB);
        client().execute(AnomalyDetectorJobAction.INSTANCE, request);
        client().execute(AnomalyDetectorJobAction.INSTANCE, request);

        Thread.sleep(5000);
        List<ADTask> adTasks = searchADTasks(detectorId, null, 100);

        assertEquals(1, adTasks.size());
        assertTrue(adTasks.get(0).isLatest());
        assertNotEquals(TaskState.FAILED.name(), adTasks.get(0).getState());
    }

    // TODO: fix this flaky test case
    @Ignore
    public void testCleanOldTaskDocs() throws InterruptedException, IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);

        createDetectionStateIndex();
        List<TaskState> states = ImmutableList.of(TaskState.FAILED, TaskState.FINISHED, TaskState.STOPPED);
        for (TaskState state : states) {
            ADTask task = randomADTask(randomAlphaOfLength(5), detector, detectorId, dateRange, state);
            createADTask(task);
        }
        long count = countDocs(ADCommonName.DETECTION_STATE_INDEX);
        assertEquals(states.size(), count);

        JobRequest request = new JobRequest(detectorId, dateRange, true, START_JOB);

        AtomicReference<JobResponse> response = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        Thread.sleep(2000);
        client().execute(AnomalyDetectorJobAction.INSTANCE, request, ActionListener.wrap(r -> {
            latch.countDown();
            response.set(r);
        }, e -> { latch.countDown(); }));
        latch.await();
        Thread.sleep(10000);
        count = countDetectorDocs(detectorId);
        // we have one latest task, so total count should add 1
        assertEquals(maxOldAdTaskDocsPerDetector + 1, count);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        // delete index will clear search context, this can avoid in-flight contexts error
        deleteIndexIfExists(CommonName.CONFIG_INDEX);
        deleteIndexIfExists(ADCommonName.DETECTION_STATE_INDEX);
    }

    public void testStartRealtimeDetector() throws IOException {
        List<String> realtimeResult = startRealtimeDetector();
        String detectorId = realtimeResult.get(0);
        String jobId = realtimeResult.get(1);
        GetResponse jobDoc = getDoc(CommonName.JOB_INDEX, detectorId);
        Job job = toADJob(jobDoc);
        assertTrue(job.isEnabled());
        assertEquals(detectorId, job.getName());

        List<ADTask> adTasks = searchADTasks(detectorId, true, 10);
        assertEquals(1, adTasks.size());
        assertEquals(ADTaskType.REALTIME_SINGLE_ENTITY.name(), adTasks.get(0).getTaskType());
        assertNotEquals(jobId, adTasks.get(0).getTaskId());
    }

    private List<String> startRealtimeDetector() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        JobRequest request = startDetectorJobRequest(detectorId, null);
        JobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        String jobId = response.getId();
        assertEquals(detectorId, jobId);
        return ImmutableList.of(detectorId, jobId);
    }

    public void testRealtimeDetectorWithoutFeature() throws IOException {
        AnomalyDetector detector = TestHelpers.randomDetector(ImmutableList.of(), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start job as no features configured");
    }

    public void testHistoricalDetectorWithoutFeature() throws IOException {
        AnomalyDetector detector = TestHelpers.randomDetector(ImmutableList.of(), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start job as no features configured");
    }

    public void testRealtimeDetectorWithoutEnabledFeature() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(TestHelpers.randomFeature(false)), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start job as no enabled features configured");
    }

    public void testHistoricalDetectorWithoutEnabledFeature() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(TestHelpers.randomFeature(false)), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start job as no enabled features configured");
    }

    private void testInvalidDetector(AnomalyDetector detector, String error) throws IOException {
        String detectorId = createDetector(detector);
        JobRequest request = startDetectorJobRequest(detectorId, dateRange);
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000)
        );
        assertEquals(error, exception.getMessage());
    }

    private JobRequest startDetectorJobRequest(String detectorId, DateRange dateRange) {
        return new JobRequest(detectorId, dateRange, false, START_JOB);
    }

    private JobRequest stopDetectorJobRequest(String detectorId, boolean historical) {
        return new JobRequest(detectorId, null, historical, STOP_JOB);
    }

    public void testStopRealtimeDetector() throws IOException {
        List<String> realtimeResult = startRealtimeDetector();
        String detectorId = realtimeResult.get(0);
        String jobId = realtimeResult.get(1);

        JobRequest request = stopDetectorJobRequest(detectorId, false);
        client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        GetResponse doc = getDoc(CommonName.JOB_INDEX, detectorId);
        Job job = toADJob(doc);
        assertFalse(job.isEnabled());
        assertEquals(detectorId, job.getName());

        List<ADTask> adTasks = searchADTasks(detectorId, true, 10);
        assertEquals(1, adTasks.size());
        assertEquals(ADTaskType.REALTIME_SINGLE_ENTITY.name(), adTasks.get(0).getTaskType());
        assertNotEquals(jobId, adTasks.get(0).getTaskId());
        assertEquals(TaskState.STOPPED.name(), adTasks.get(0).getState());
    }

    public void testStopHistoricalDetector() throws IOException, InterruptedException {
        updateTransientSettings(ImmutableMap.of(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 5));
        ADTask adTask = startHistoricalAnalysis(startTime, endTime);
        assertEquals(TaskState.INIT.name(), adTask.getState());
        assertNull(adTask.getStartedBy());
        assertNull(adTask.getUser());
        waitUntil(() -> {
            try {
                ADTask task = getADTask(adTask.getTaskId());
                boolean taskRunning = TestHelpers.HISTORICAL_ANALYSIS_RUNNING_STATS.contains(task.getState());
                if (taskRunning) {
                    // It's possible that the task not started on worker node yet. Recancel it to make sure
                    // task cancelled.
                    JobRequest request = stopDetectorJobRequest(adTask.getConfigId(), true);
                    client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
                }
                return !taskRunning;
            } catch (Exception e) {
                return false;
            }
        }, 20, TimeUnit.SECONDS);
        ADTask stoppedTask = getADTask(adTask.getTaskId());
        assertEquals(TaskState.STOPPED.name(), stoppedTask.getState());
        assertEquals(0, getExecutingADTask());
    }

    public void testProfileHistoricalDetector() throws IOException, InterruptedException {
        ADTask adTask = startHistoricalAnalysis(startTime, endTime);
        GetConfigRequest request = taskProfileRequest(adTask.getConfigId());
        GetAnomalyDetectorResponse response = client().execute(GetAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        assertTrue(response.getDetectorProfile().getTaskProfile() != null);

        ADTask finishedTask = getADTask(adTask.getTaskId());
        int i = 0;
        while (TestHelpers.HISTORICAL_ANALYSIS_RUNNING_STATS.contains(finishedTask.getState()) && i < 10) {
            finishedTask = getADTask(adTask.getTaskId());
            Thread.sleep(2000);
            i++;
        }
        assertTrue(HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(finishedTask.getState()));

        response = client().execute(GetAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        assertNull(response.getDetectorProfile().getTaskProfile().getNodeId());
        ADTask profileAdTask = response.getDetectorProfile().getTaskProfile().getTask();
        assertEquals(finishedTask.getTaskId(), profileAdTask.getTaskId());
        assertEquals(finishedTask.getConfigId(), profileAdTask.getConfigId());
        assertEquals(finishedTask.getDetector(), profileAdTask.getDetector());
        assertEquals(finishedTask.getState(), profileAdTask.getState());
    }

    public void testProfileWithMultipleRunningTask() throws IOException {
        ADTask adTask1 = startHistoricalAnalysis(startTime, endTime);
        ADTask adTask2 = startHistoricalAnalysis(startTime, endTime);

        GetConfigRequest request1 = taskProfileRequest(adTask1.getConfigId());
        GetConfigRequest request2 = taskProfileRequest(adTask2.getConfigId());
        GetAnomalyDetectorResponse response1 = client().execute(GetAnomalyDetectorAction.INSTANCE, request1).actionGet(10000);
        GetAnomalyDetectorResponse response2 = client().execute(GetAnomalyDetectorAction.INSTANCE, request2).actionGet(10000);
        TaskProfile<ADTask> taskProfile1 = response1.getDetectorProfile().getTaskProfile();
        TaskProfile<ADTask> taskProfile2 = response2.getDetectorProfile().getTaskProfile();
        assertNotNull(taskProfile1.getNodeId());
        assertNotNull(taskProfile2.getNodeId());
        assertNotEquals(taskProfile1.getNodeId(), taskProfile2.getNodeId());
    }

    private GetConfigRequest taskProfileRequest(String detectorId) throws IOException {
        return new GetConfigRequest(detectorId, Versions.MATCH_ANY, false, false, "", PROFILE, true, null);
    }

    private long getExecutingADTask() {
        StatsRequest adStatsRequest = new StatsRequest(getDataNodesArray());
        Set<String> validStats = ImmutableSet.of(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName());
        adStatsRequest.addAll(validStats);
        StatsTimeSeriesResponse statsResponse = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5000);
        AtomicLong totalExecutingTask = new AtomicLong(0);
        statsResponse.getAdStatsResponse().getStatsNodesResponse().getNodes().forEach(node -> {
            totalExecutingTask.getAndAdd((Long) node.getStatsMap().get(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName()));
        });
        return totalExecutingTask.get();
    }
}
