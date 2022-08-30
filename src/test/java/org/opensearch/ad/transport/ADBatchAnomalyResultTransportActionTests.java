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

/*package org.opensearch.ad.transport;


@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
@Ignore
public class ADBatchAnomalyResultTransportActionTests extends HistoricalAnalysisIntegTestCase {

    private String testIndex;
    private Instant startTime;
    private Instant endTime;
    private String type = "error";
    private int detectionIntervalInMinutes = 1;
    private DetectionDateRange dateRange;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        testIndex = "test_historical_data";
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        endTime = Instant.now();
        dateRange = new DetectionDateRange(endTime, endTime.plus(10, ChronoUnit.DAYS));
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type);
        createDetectionStateIndex();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(MAX_BATCH_TASK_PER_NODE.getKey(), 1)
            .build();
    }

    public void testAnomalyDetectorWithNullDetector() {
        ADTask task = randomCreatedADTask(randomAlphaOfLength(5), null, dateRange);
        ADBatchAnomalyResultRequest request = new ADBatchAnomalyResultRequest(task);
        ActionRequestValidationException exception = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(30_000)
        );
        assertTrue(exception.getMessage().contains("Detector can't be null"));
    }

    public void testHistoricalAnalysisWithFutureDateRange() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(endTime, endTime.plus(10, ChronoUnit.DAYS));
        testInvalidDetectionDateRange(dateRange);
    }

    public void testHistoricalAnalysisWithInvalidHistoricalDateRange() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime.minus(10, ChronoUnit.DAYS), startTime);
        testInvalidDetectionDateRange(dateRange);
    }

    public void testHistoricalAnalysisWithSmallHistoricalDateRange() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, startTime.plus(10, ChronoUnit.MINUTES));
        testInvalidDetectionDateRange(dateRange, "There is not enough data to train model");
    }

    public void testHistoricalAnalysisWithValidDateRange() throws IOException, InterruptedException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(dateRange);
        client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(5000);
        Thread.sleep(20000);
        GetResponse doc = getDoc(CommonName.DETECTION_STATE_INDEX, request.getAdTask().getTaskId());
        assertTrue(HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(doc.getSourceAsMap().get(ADTask.STATE_FIELD)));
    }

    public void testHistoricalAnalysisWithNonExistingIndex() throws IOException {
        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(
            new DetectionDateRange(startTime, endTime),
            randomAlphaOfLength(5)
        );
        client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(10_000);
    }

    public void testHistoricalAnalysisExceedsMaxRunningTaskLimit() throws IOException, InterruptedException {
        updateTransientSettings(ImmutableMap.of(MAX_BATCH_TASK_PER_NODE.getKey(), 1));
        updateTransientSettings(ImmutableMap.of(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 5));
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        int totalDataNodes = getDataNodes().size();
        for (int i = 0; i < totalDataNodes; i++) {
            client().execute(ADBatchAnomalyResultAction.INSTANCE, adBatchAnomalyResultRequest(dateRange)).actionGet(5000);
        }
        waitUntil(() -> countDocs(CommonName.DETECTION_STATE_INDEX) >= totalDataNodes, 10, TimeUnit.SECONDS);

        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(dateRange);
        try {
            client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(5000);
        } catch (Exception e) {
            assertTrue(
                ExceptionUtil
                    .getErrorMessage(e)
                    .contains("All nodes' executing batch tasks exceeds limitation No eligible node to run detector")
            );
        }
    }

    public void testDisableADPlugin() throws IOException {
        updateTransientSettings(ImmutableMap.of(AD_PLUGIN_ENABLED, false));

        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(new DetectionDateRange(startTime, endTime));
        RuntimeException exception = expectThrowsAnyOf(
            ImmutableList.of(NotSerializableExceptionWrapper.class, EndRunException.class),
            () -> client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(10000)
        );
        assertTrue(exception.getMessage().contains("AD plugin is disabled"));
        updateTransientSettings(ImmutableMap.of(AD_PLUGIN_ENABLED, true));
    }

    public void testMultipleTasks() throws IOException, InterruptedException {
        updateTransientSettings(ImmutableMap.of(MAX_BATCH_TASK_PER_NODE.getKey(), 2));

        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        for (int i = 0; i < getDataNodes().size(); i++) {
            client().execute(ADBatchAnomalyResultAction.INSTANCE, adBatchAnomalyResultRequest(dateRange));
        }

        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(
            new DetectionDateRange(startTime, startTime.plus(2000, ChronoUnit.MINUTES))
        );
        client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(5000);
        Thread.sleep(25000);
        GetResponse doc = getDoc(CommonName.DETECTION_STATE_INDEX, request.getAdTask().getTaskId());
        assertTrue(HISTORICAL_ANALYSIS_FINISHED_FAILED_STATS.contains(doc.getSourceAsMap().get(ADTask.STATE_FIELD)));
        updateTransientSettings(ImmutableMap.of(MAX_BATCH_TASK_PER_NODE.getKey(), 1));
    }

    private ADBatchAnomalyResultRequest adBatchAnomalyResultRequest(DetectionDateRange dateRange) throws IOException {
        return adBatchAnomalyResultRequest(dateRange, testIndex);
    }

    private ADBatchAnomalyResultRequest adBatchAnomalyResultRequest(DetectionDateRange dateRange, String indexName) throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), indexName, detectionIntervalInMinutes, timeField);
        ADTask adTask = randomCreatedADTask(randomAlphaOfLength(5), detector, dateRange);
        adTask.setTaskId(createADTask(adTask));
        return new ADBatchAnomalyResultRequest(adTask);
    }

    private void testInvalidDetectionDateRange(DetectionDateRange dateRange) throws IOException, InterruptedException {
        testInvalidDetectionDateRange(dateRange, "There is no data in the detection date range");
    }

    private void testInvalidDetectionDateRange(DetectionDateRange dateRange, String error) throws IOException, InterruptedException {
        ADBatchAnomalyResultRequest request = adBatchAnomalyResultRequest(dateRange);
        client().execute(ADBatchAnomalyResultAction.INSTANCE, request).actionGet(5000);
        Thread.sleep(5000);
        GetResponse doc = getDoc(CommonName.DETECTION_STATE_INDEX, request.getAdTask().getTaskId());
        assertEquals(error, doc.getSourceAsMap().get(ADTask.ERROR_FIELD));
    }
}*/
