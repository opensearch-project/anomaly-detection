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
package org.opensearch.ad;


public abstract class HistoricalAnalysisIntegTestCase extends ADIntegTestCase {

    protected String testIndex = "test_historical_data";
    protected int detectionIntervalInMinutes = 1;
    protected int DEFAULT_TEST_DATA_DOCS = 3000;
    protected String DEFAULT_IP = "127.0.0.1";

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(MockReindexPlugin.class);
        plugins.addAll(super.getMockPlugins());
        plugins.remove(MockTransportService.TestPlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    public void ingestTestData(String testIndex, Instant startTime, int detectionIntervalInMinutes, String type) {
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type, DEFAULT_IP, DEFAULT_TEST_DATA_DOCS, true);
    }

    public void ingestTestData(String testIndex, Instant startTime, int detectionIntervalInMinutes, String type, int totalDocs) {
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type, DEFAULT_IP, totalDocs, true);
    }

    public void ingestTestData(
        String testIndex,
        Instant startTime,
        int detectionIntervalInMinutes,
        String type,
        String ip,
        int totalDocs,
        boolean createIndexFirst
    ) {
        if (createIndexFirst) {
            createTestDataIndex(testIndex);
        }
        List<Map<String, ?>> docs = new ArrayList<>();
        Instant currentInterval = Instant.from(startTime);

        for (int i = 0; i < totalDocs; i++) {
            currentInterval = currentInterval.plus(detectionIntervalInMinutes, ChronoUnit.MINUTES);
            double value = i % 500 == 0 ? randomDoubleBetween(1000, 2000, true) : randomDoubleBetween(10, 100, true);
            Map<String, Object> doc = new HashMap<>();
            doc.put(timeField, currentInterval.toEpochMilli());
            doc.put("value", value);
            doc.put("ip", ip);
            doc.put("type", type);
            doc.put("is_error", randomBoolean());
            doc.put("message", randomAlphaOfLength(5));
            docs.add(doc);
        }
        BulkResponse bulkResponse = bulkIndexDocs(testIndex, docs, 30_000);
        assertEquals(RestStatus.OK, bulkResponse.status());
        assertFalse(bulkResponse.hasFailures());
        long count = countDocs(testIndex);
        if (createIndexFirst) {
            assertEquals(totalDocs, count);
        }
    }

    public Feature maxValueFeature() throws IOException {
        AggregationBuilder aggregationBuilder = TestHelpers.parseAggregation("{\"test\":{\"max\":{\"field\":\"" + valueField + "\"}}}");
        return new Feature(randomAlphaOfLength(5), randomAlphaOfLength(10), true, aggregationBuilder);
    }

    public AnomalyDetector randomDetector(List<Feature> features) throws IOException {
        return TestHelpers.randomDetector(features, testIndex, detectionIntervalInMinutes, timeField);
    }

    public ADTask randomCreatedADTask(String taskId, AnomalyDetector detector, DetectionDateRange detectionDateRange) {
        String detectorId = detector == null ? null : detector.getDetectorId();
        return randomCreatedADTask(taskId, detector, detectorId, detectionDateRange);
    }

    public ADTask randomCreatedADTask(String taskId, AnomalyDetector detector, String detectorId, DetectionDateRange detectionDateRange) {
        return randomADTask(taskId, detector, detectorId, detectionDateRange, ADTaskState.CREATED);
    }

    public ADTask randomADTask(
        String taskId,
        AnomalyDetector detector,
        String detectorId,
        DetectionDateRange detectionDateRange,
        ADTaskState state
    ) {
        ADTask.Builder builder = ADTask
            .builder()
            .taskId(taskId)
            .taskType(ADTaskType.HISTORICAL_SINGLE_ENTITY.name())
            .detectorId(detectorId)
            .detectionDateRange(detectionDateRange)
            .detector(detector)
            .state(state.name())
            .taskProgress(0.0f)
            .initProgress(0.0f)
            .isLatest(true)
            .startedBy(randomAlphaOfLength(5))
            .executionStartTime(Instant.now().minus(randomLongBetween(10, 100), ChronoUnit.MINUTES));
        if (ADTaskState.FINISHED == state) {
            setPropertyForNotRunningTask(builder);
        } else if (ADTaskState.FAILED == state) {
            setPropertyForNotRunningTask(builder);
            builder.error(randomAlphaOfLength(5));
        } else if (ADTaskState.STOPPED == state) {
            setPropertyForNotRunningTask(builder);
            builder.error(randomAlphaOfLength(5));
            builder.stoppedBy(randomAlphaOfLength(5));
        }
        return builder.build();
    }

    private ADTask.Builder setPropertyForNotRunningTask(ADTask.Builder builder) {
        builder.executionEndTime(Instant.now().minus(randomLongBetween(1, 5), ChronoUnit.MINUTES));
        builder.isLatest(false);
        return builder;
    }

    public List<ADTask> searchADTasks(String detectorId, Boolean isLatest, int size) throws IOException {
        return searchADTasks(detectorId, null, isLatest, size);
    }

    public List<ADTask> searchADTasks(String detectorId, String parentTaskId, Boolean isLatest, int size) throws IOException {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));
        if (isLatest != null) {
            query.filter(new TermQueryBuilder(IS_LATEST_FIELD, isLatest));
        }
        if (parentTaskId != null) {
            query.filter(new TermQueryBuilder(PARENT_TASK_ID_FIELD, parentTaskId));
        }
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query).sort(EXECUTION_START_TIME_FIELD, SortOrder.DESC).trackTotalHits(true).size(size);
        searchRequest.source(sourceBuilder).indices(CommonName.DETECTION_STATE_INDEX);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        Iterator<SearchHit> iterator = searchResponse.getHits().iterator();

        List<ADTask> adTasks = new ArrayList<>();
        while (iterator.hasNext()) {
            SearchHit next = iterator.next();
            ADTask task = ADTask.parse(TestHelpers.parser(next.getSourceAsString()), next.getId());
            adTasks.add(task);
        }
        return adTasks;
    }

    public ADTask getADTask(String taskId) throws IOException {
        ADTask adTask = toADTask(getDoc(CommonName.DETECTION_STATE_INDEX, taskId));
        adTask.setTaskId(taskId);
        return adTask;
    }

//    public AnomalyDetectorJob getADJob(String detectorId) throws IOException {
//        return toADJob(getDoc(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, detectorId));
//    }

    public ADTask toADTask(GetResponse doc) throws IOException {
        return ADTask.parse(TestHelpers.parser(doc.getSourceAsString()));
    }

//    public AnomalyDetectorJob toADJob(GetResponse doc) throws IOException {
//        return AnomalyDetectorJob.parse(TestHelpers.parser(doc.getSourceAsString()));
//    }

    public ADTask startHistoricalAnalysis(Instant startTime, Instant endTime) throws IOException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
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
        AnomalyDetectorJobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        return getADTask(response.getId());
    }

    public ADTask startHistoricalAnalysis(String detectorId, Instant startTime, Instant endTime) throws IOException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            dateRange,
            true,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        AnomalyDetectorJobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        return getADTask(response.getId());
    }
}
*/
