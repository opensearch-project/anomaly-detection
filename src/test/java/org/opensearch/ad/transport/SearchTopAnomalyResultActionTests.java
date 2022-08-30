/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

/*
package org.opensearch.ad.transport;


// Only invalid test cases are covered here. This is due to issues with the lang-painless module not
// being installed on test clusters spun up in OpenSearchIntegTestCase classes (which this class extends),
// which is needed for executing the API on ingested data. Valid test cases are covered at the REST layer.
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
@Ignore
public class SearchTopAnomalyResultActionTests extends HistoricalAnalysisIntegTestCase {

    private String testIndex;
    private String detectorId;
    private String taskId;
    private Instant startTime;
    private Instant endTime;
    private ImmutableList<String> categoryFields;
    private String type = "error";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings
            .builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(MAX_BATCH_TASK_PER_NODE.getKey(), 1)
            .build();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        testIndex = "test_data";
        taskId = "test-task-id";
        startTime = Instant.now().minus(10, ChronoUnit.DAYS);
        endTime = Instant.now();
        categoryFields = ImmutableList.of("test-field-1", "test-field-2");
        ingestTestData();
        createSystemIndices();
        createAndIndexDetector();
    }

    private void ingestTestData() {
        ingestTestData(testIndex, startTime, 1, "test", 1);
    }

    private void createSystemIndices() throws IOException {
        createDetectorIndex();
        createADResultIndex();
    }

    private void createAndIndexDetector() throws IOException {
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(testIndex),
                ImmutableList.of(TestHelpers.randomFeature(true)),
                null,
                Instant.now(),
                1,
                false,
                categoryFields
            );
        detectorId = createDetector(detector);

    }

    public void testInstanceAndNameValid() {
        assertNotNull(SearchTopAnomalyResultAction.INSTANCE.name());
        assertEquals(SearchTopAnomalyResultAction.INSTANCE.name(), SearchTopAnomalyResultAction.NAME);
    }

    public void testInvalidOrder() {
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
            false,
            1,
            Arrays.asList(categoryFields.get(0)),
            "invalid-order",
            startTime,
            endTime
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest).actionGet(10_000)
        );
    }

    public void testNegativeSize() {
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
            false,
            -1,
            Arrays.asList(categoryFields.get(0)),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            startTime,
            endTime
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest).actionGet(10_000)
        );
    }

    public void testZeroSize() {
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
            false,
            0,
            Arrays.asList(categoryFields.get(0)),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            startTime,
            endTime
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest).actionGet(10_000)
        );
    }

    public void testTooLargeSize() {
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
            false,
            9999999,
            Arrays.asList(categoryFields.get(0)),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            startTime,
            endTime
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest).actionGet(10_000)
        );
    }

    public void testMissingStartTime() {
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
            false,
            1,
            Arrays.asList(categoryFields.get(0)),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            null,
            endTime
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest).actionGet(10_000)
        );
    }

    public void testMissingEndTime() {
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
            false,
            1,
            Arrays.asList(categoryFields.get(0)),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            startTime,
            null
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest).actionGet(10_000)
        );
    }

    public void testInvalidStartAndEndTimes() {
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
            false,
            1,
            Arrays.asList(categoryFields.get(0)),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            endTime,
            startTime
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest).actionGet(10_000)
        );

        Instant curTimeInMillis = Instant.now();
        SearchTopAnomalyResultRequest searchRequest2 = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
            false,
            1,
            Arrays.asList(categoryFields.get(0)),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            curTimeInMillis,
            curTimeInMillis
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest2).actionGet(10_000)
        );
    }

    public void testNoExistingHistoricalTask() throws IOException {
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            detectorId,
            taskId,
            true,
            1,
            Arrays.asList(categoryFields.get(0)),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            startTime,
            endTime
        );
        expectThrows(Exception.class, () -> client().execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest).actionGet(10_000));
    }

    public void testSearchOnNonHCDetector() throws IOException {
        AnomalyDetector nonHCDetector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(testIndex),
                ImmutableList.of(TestHelpers.randomFeature(true)),
                null,
                Instant.now(),
                1,
                false,
                ImmutableList.of()
            );
        String nonHCDetectorId = createDetector(nonHCDetector);
        SearchTopAnomalyResultRequest searchRequest = new SearchTopAnomalyResultRequest(
            nonHCDetectorId,
            taskId,
            false,
            1,
            Arrays.asList(categoryFields.get(0)),
            SearchTopAnomalyResultTransportAction.OrderType.SEVERITY.getName(),
            startTime,
            endTime
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> client().execute(SearchTopAnomalyResultAction.INSTANCE, searchRequest).actionGet(10_000)
        );
    }
}
*/
