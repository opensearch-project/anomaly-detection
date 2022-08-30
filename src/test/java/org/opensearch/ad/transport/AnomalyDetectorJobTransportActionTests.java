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

/*
package org.opensearch.ad.transport;


@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
@Ignore
public class AnomalyDetectorJobTransportActionTests extends HistoricalAnalysisIntegTestCase {
    private Instant startTime;
    private Instant endTime;
    private String type = "error";
    private int maxOldAdTaskDocsPerDetector = 2;
    private DetectionDateRange dateRange;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        endTime = Instant.now();
        dateRange = new DetectionDateRange(startTime, endTime);
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
        AnomalyDetectorJobRequest request = startDetectorJobRequest(detectorId, dateRange);
        IndexNotFoundException exception = expectThrows(
            IndexNotFoundException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(3000)
        );
        assertTrue(exception.getMessage().contains("no such index [.opendistro-anomaly-detectors]"));
    }

    public void testDetectorNotFound() {
        String detectorId = randomAlphaOfLength(5);
        AnomalyDetectorJobRequest request = startDetectorJobRequest(detectorId, dateRange);
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000)
        );
        assertTrue(exception.getMessage().contains(FAIL_TO_FIND_DETECTOR_MSG));
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
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            dateRange,
            true,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        Client nodeClient = getDataNodeClient();
        if (nodeClient != null) {
            AnomalyDetectorJobResponse response = nodeClient.execute(MockAnomalyDetectorJobAction.INSTANCE, request).actionGet(100000);
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
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            dateRange,
            true,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        Client nodeClient = getDataNodeClient();

        if (nodeClient != null) {
            AnomalyDetectorJobResponse response = nodeClient.execute(MockAnomalyDetectorJobAction.INSTANCE, request).actionGet(100000);
            waitUntil(() -> {
                try {
                    ADTask task = getADTask(response.getId());
                    return !TestHelpers.HISTORICAL_ANALYSIS_RUNNING_STATS.contains(task.getState());
                } catch (IOException e) {
                    return false;
                }
            }, 20, TimeUnit.SECONDS);
            ADTask adTask = getADTask(response.getId());
            assertEquals(ADTaskType.HISTORICAL_HC_DETECTOR.toString(), adTask.getTaskType());
            assertTrue(HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(adTask.getState()));
            assertEquals(categoryField, adTask.getDetector().getCategoryField().get(0));

            if (ADTaskState.FINISHED.name().equals(adTask.getState())) {
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
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            dateRange,
            true,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        Client nodeClient = getDataNodeClient();

        if (nodeClient != null) {
            AnomalyDetectorJobResponse response = nodeClient.execute(MockAnomalyDetectorJobAction.INSTANCE, request).actionGet(100_000);
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
            assertEquals(categoryField, adTask.getDetector().getCategoryField().get(0));
            assertEquals(ipField, adTask.getDetector().getCategoryField().get(1));

            if (ADTaskState.FINISHED.name().equals(adTask.getState())) {
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
        AnomalyDetectorJobRequest request = startDetectorJobRequest(detectorId, dateRange);
        AnomalyDetectorJobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        assertNotNull(response.getId());
        OpenSearchStatusException exception = null;
        // Add retry to solve the flaky test
        for (int i = 0; i < 10; i++) {
            exception = expectThrows(
                OpenSearchStatusException.class,
                () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000)
            );
            if (exception.getMessage().contains(DETECTOR_IS_RUNNING)) {
                break;
            } else {
                logger.error("Unexpected error happened when rerun detector", exception);
            }
            Thread.sleep(1000);
        }
        assertNotNull(exception);
        assertTrue(exception.getMessage().contains(DETECTOR_IS_RUNNING));
        assertEquals(DETECTOR_IS_RUNNING, exception.getMessage());
        Thread.sleep(20000);
        List<ADTask> adTasks = searchADTasks(detectorId, null, 100);
        assertEquals(1, adTasks.size());
        assertTrue(HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(adTasks.get(0).getState()));
    }

    public void testRaceConditionByStartingMultipleTasks() throws IOException, InterruptedException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            dateRange,
            true,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        client().execute(AnomalyDetectorJobAction.INSTANCE, request);
        client().execute(AnomalyDetectorJobAction.INSTANCE, request);

        Thread.sleep(5000);
        List<ADTask> adTasks = searchADTasks(detectorId, null, 100);

        assertEquals(1, adTasks.size());
        assertTrue(adTasks.get(0).getLatest());
        assertNotEquals(ADTaskState.FAILED.name(), adTasks.get(0).getState());
    }

    // TODO: fix this flaky test case
    @Ignore
    public void testCleanOldTaskDocs() throws InterruptedException, IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);

        createDetectionStateIndex();
        List<ADTaskState> states = ImmutableList.of(ADTaskState.FAILED, ADTaskState.FINISHED, ADTaskState.STOPPED);
        for (ADTaskState state : states) {
            ADTask task = randomADTask(randomAlphaOfLength(5), detector, detectorId, dateRange, state);
            createADTask(task);
        }
        long count = countDocs(CommonName.DETECTION_STATE_INDEX);
        assertEquals(states.size(), count);

        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            dateRange,
            true,
            randomLong(),
            randomLong(),
            START_JOB
        );

        AtomicReference<AnomalyDetectorJobResponse> response = new AtomicReference<>();
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
        deleteIndexIfExists(AnomalyDetector.ANOMALY_DETECTORS_INDEX);
        deleteIndexIfExists(CommonName.DETECTION_STATE_INDEX);
    }

//    public void testStartRealtimeDetector() throws IOException {
//        List<String> realtimeResult = startRealtimeDetector();
//        String detectorId = realtimeResult.get(0);
//        String jobId = realtimeResult.get(1);
//        GetResponse jobDoc = getDoc(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, detectorId);
//        AnomalyDetectorJob job = toADJob(jobDoc);
//        assertTrue(job.isEnabled());
//        assertEquals(detectorId, job.getName());
//
//        List<ADTask> adTasks = searchADTasks(detectorId, true, 10);
//        assertEquals(1, adTasks.size());
//        assertEquals(ADTaskType.REALTIME_SINGLE_ENTITY.name(), adTasks.get(0).getTaskType());
//        assertNotEquals(jobId, adTasks.get(0).getTaskId());
//    }

    private List<String> startRealtimeDetector() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = startDetectorJobRequest(detectorId, null);
        AnomalyDetectorJobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        String jobId = response.getId();
        assertEquals(detectorId, jobId);
        return ImmutableList.of(detectorId, jobId);
    }

    public void testRealtimeDetectorWithoutFeature() throws IOException {
        AnomalyDetector detector = TestHelpers.randomDetector(ImmutableList.of(), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start detector job as no features configured");
    }

    public void testHistoricalDetectorWithoutFeature() throws IOException {
        AnomalyDetector detector = TestHelpers.randomDetector(ImmutableList.of(), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start detector job as no features configured");
    }

    public void testRealtimeDetectorWithoutEnabledFeature() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(TestHelpers.randomFeature(false)), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start detector job as no enabled features configured");
    }

    public void testHistoricalDetectorWithoutEnabledFeature() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(TestHelpers.randomFeature(false)), testIndex, detectionIntervalInMinutes, timeField);
        testInvalidDetector(detector, "Can't start detector job as no enabled features configured");
    }

    private void testInvalidDetector(AnomalyDetector detector, String error) throws IOException {
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = startDetectorJobRequest(detectorId, dateRange);
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000)
        );
        assertEquals(error, exception.getMessage());
    }

    private AnomalyDetectorJobRequest startDetectorJobRequest(String detectorId, DetectionDateRange dateRange) {
        return new AnomalyDetectorJobRequest(detectorId, dateRange, false, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, START_JOB);
    }

    private AnomalyDetectorJobRequest stopDetectorJobRequest(String detectorId, boolean historical) {
        return new AnomalyDetectorJobRequest(detectorId, null, historical, UNASSIGNED_SEQ_NO, UNASSIGNED_PRIMARY_TERM, STOP_JOB);
    }

//    public void testStopRealtimeDetector() throws IOException {
//        List<String> realtimeResult = startRealtimeDetector();
//        String detectorId = realtimeResult.get(0);
//        String jobId = realtimeResult.get(1);
//
//        AnomalyDetectorJobRequest request = stopDetectorJobRequest(detectorId, false);
//        client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
//        GetResponse doc = getDoc(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, detectorId);
//        AnomalyDetectorJob job = toADJob(doc);
//        assertFalse(job.isEnabled());
//        assertEquals(detectorId, job.getName());
//
//        List<ADTask> adTasks = searchADTasks(detectorId, true, 10);
//        assertEquals(1, adTasks.size());
//        assertEquals(ADTaskType.REALTIME_SINGLE_ENTITY.name(), adTasks.get(0).getTaskType());
//        assertNotEquals(jobId, adTasks.get(0).getTaskId());
//        assertEquals(ADTaskState.STOPPED.name(), adTasks.get(0).getState());
//    }

    public void testStopHistoricalDetector() throws IOException, InterruptedException {
        updateTransientSettings(ImmutableMap.of(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 5));
        ADTask adTask = startHistoricalAnalysis(startTime, endTime);
        assertEquals(ADTaskState.INIT.name(), adTask.getState());
        assertNull(adTask.getStartedBy());
        assertNull(adTask.getUser());
        waitUntil(() -> {
            try {
                ADTask task = getADTask(adTask.getTaskId());
                boolean taskRunning = TestHelpers.HISTORICAL_ANALYSIS_RUNNING_STATS.contains(task.getState());
                if (taskRunning) {
                    // It's possible that the task not started on worker node yet. Recancel it to make sure
                    // task cancelled.
                    AnomalyDetectorJobRequest request = stopDetectorJobRequest(adTask.getDetectorId(), true);
                    client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
                }
                return !taskRunning;
            } catch (Exception e) {
                return false;
            }
        }, 20, TimeUnit.SECONDS);
        ADTask stoppedTask = getADTask(adTask.getTaskId());
        assertEquals(ADTaskState.STOPPED.name(), stoppedTask.getState());
        assertEquals(0, getExecutingADTask());
    }

    public void testProfileHistoricalDetector() throws IOException, InterruptedException {
        ADTask adTask = startHistoricalAnalysis(startTime, endTime);
        GetAnomalyDetectorRequest request = taskProfileRequest(adTask.getDetectorId());
        GetAnomalyDetectorResponse response = client().execute(GetAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        assertTrue(response.getDetectorProfile().getAdTaskProfile() != null);

        ADTask finishedTask = getADTask(adTask.getTaskId());
        int i = 0;
        while (TestHelpers.HISTORICAL_ANALYSIS_RUNNING_STATS.contains(finishedTask.getState()) && i < 10) {
            finishedTask = getADTask(adTask.getTaskId());
            Thread.sleep(2000);
            i++;
        }
        assertTrue(HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(finishedTask.getState()));

        response = client().execute(GetAnomalyDetectorAction.INSTANCE, request).actionGet(10000);
        assertNull(response.getDetectorProfile().getAdTaskProfile().getNodeId());
        ADTask profileAdTask = response.getDetectorProfile().getAdTaskProfile().getAdTask();
        assertEquals(finishedTask.getTaskId(), profileAdTask.getTaskId());
        assertEquals(finishedTask.getDetectorId(), profileAdTask.getDetectorId());
        assertEquals(finishedTask.getDetector(), profileAdTask.getDetector());
        assertEquals(finishedTask.getState(), profileAdTask.getState());
    }

    public void testProfileWithMultipleRunningTask() throws IOException {
        ADTask adTask1 = startHistoricalAnalysis(startTime, endTime);
        ADTask adTask2 = startHistoricalAnalysis(startTime, endTime);

        GetAnomalyDetectorRequest request1 = taskProfileRequest(adTask1.getDetectorId());
        GetAnomalyDetectorRequest request2 = taskProfileRequest(adTask2.getDetectorId());
        GetAnomalyDetectorResponse response1 = client().execute(GetAnomalyDetectorAction.INSTANCE, request1).actionGet(10000);
        GetAnomalyDetectorResponse response2 = client().execute(GetAnomalyDetectorAction.INSTANCE, request2).actionGet(10000);
        ADTaskProfile taskProfile1 = response1.getDetectorProfile().getAdTaskProfile();
        ADTaskProfile taskProfile2 = response2.getDetectorProfile().getAdTaskProfile();
        assertNotNull(taskProfile1.getNodeId());
        assertNotNull(taskProfile2.getNodeId());
        assertNotEquals(taskProfile1.getNodeId(), taskProfile2.getNodeId());
    }

    private GetAnomalyDetectorRequest taskProfileRequest(String detectorId) throws IOException {
        return new GetAnomalyDetectorRequest(detectorId, Versions.MATCH_ANY, false, false, "", PROFILE, true, null);
    }

    private long getExecutingADTask() {
        ADStatsRequest adStatsRequest = new ADStatsRequest(getDataNodesArray());
        Set<String> validStats = ImmutableSet.of(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName());
        adStatsRequest.addAll(validStats);
        StatsAnomalyDetectorResponse statsResponse = client().execute(StatsAnomalyDetectorAction.INSTANCE, adStatsRequest).actionGet(5000);
        AtomicLong totalExecutingTask = new AtomicLong(0);
        statsResponse
            .getAdStatsResponse()
            .getADStatsNodesResponse()
            .getNodes()
            .forEach(
                node -> { totalExecutingTask.getAndAdd((Long) node.getStatsMap().get(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName())); }
            );
        return totalExecutingTask.get();
    }
}
*/
