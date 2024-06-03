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

package org.opensearch.ad;

import static org.opensearch.ad.model.ADTask.DETECTOR_ID_FIELD;
import static org.opensearch.timeseries.util.RestHandlerUtils.START_JOB;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.mock.plugin.MockReindexPlugin;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.transport.AnomalyDetectorJobAction;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.transport.JobRequest;
import org.opensearch.timeseries.transport.JobResponse;

import com.google.common.collect.ImmutableList;

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

    @Override
    public Feature maxValueFeature() throws IOException {
        AggregationBuilder aggregationBuilder = TestHelpers.parseAggregation("{\"test\":{\"max\":{\"field\":\"" + valueField + "\"}}}");
        return new Feature(randomAlphaOfLength(5), randomAlphaOfLength(10), true, aggregationBuilder);
    }

    public AnomalyDetector randomDetector(List<Feature> features) throws IOException {
        return TestHelpers.randomDetector(features, testIndex, detectionIntervalInMinutes, timeField);
    }

    public ADTask randomCreatedADTask(String taskId, AnomalyDetector detector, DateRange detectionDateRange) {
        String detectorId = detector == null ? null : detector.getId();
        return randomCreatedADTask(taskId, detector, detectorId, detectionDateRange);
    }

    public ADTask randomCreatedADTask(String taskId, AnomalyDetector detector, String detectorId, DateRange detectionDateRange) {
        return randomADTask(taskId, detector, detectorId, detectionDateRange, TaskState.CREATED);
    }

    public ADTask randomADTask(String taskId, AnomalyDetector detector, String detectorId, DateRange detectionDateRange, TaskState state) {
        ADTask.Builder builder = ADTask
            .builder()
            .taskId(taskId)
            .taskType(ADTaskType.HISTORICAL_SINGLE_ENTITY.name())
            .configId(detectorId)
            .detectionDateRange(detectionDateRange)
            .detector(detector)
            .state(state.name())
            .taskProgress(0.0f)
            .initProgress(0.0f)
            .isLatest(true)
            .startedBy(randomAlphaOfLength(5))
            .executionStartTime(Instant.now().minus(randomLongBetween(10, 100), ChronoUnit.MINUTES));
        if (TaskState.FINISHED == state) {
            setPropertyForNotRunningTask(builder);
        } else if (TaskState.FAILED == state) {
            setPropertyForNotRunningTask(builder);
            builder.error(randomAlphaOfLength(5));
        } else if (TaskState.STOPPED == state) {
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
            query.filter(new TermQueryBuilder(TimeSeriesTask.IS_LATEST_FIELD, isLatest));
        }
        if (parentTaskId != null) {
            query.filter(new TermQueryBuilder(TimeSeriesTask.PARENT_TASK_ID_FIELD, parentTaskId));
        }
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query).sort(TimeSeriesTask.EXECUTION_START_TIME_FIELD, SortOrder.DESC).trackTotalHits(true).size(size);
        searchRequest.source(sourceBuilder).indices(ADCommonName.DETECTION_STATE_INDEX);
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
        ADTask adTask = toADTask(getDoc(ADCommonName.DETECTION_STATE_INDEX, taskId));
        adTask.setTaskId(taskId);
        return adTask;
    }

    public Job getADJob(String detectorId) throws IOException {
        return toADJob(getDoc(CommonName.JOB_INDEX, detectorId));
    }

    public ADTask toADTask(GetResponse doc) throws IOException {
        return ADTask.parse(TestHelpers.parser(doc.getSourceAsString()));
    }

    public Job toADJob(GetResponse doc) throws IOException {
        return Job.parse(TestHelpers.parser(doc.getSourceAsString()));
    }

    public ADTask startHistoricalAnalysis(Instant startTime, Instant endTime) throws IOException {
        DateRange dateRange = new DateRange(startTime, endTime);
        AnomalyDetector detector = TestHelpers
            .randomDetector(ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        JobRequest request = new JobRequest(detectorId, dateRange, true, START_JOB);
        JobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        return getADTask(response.getId());
    }

    public ADTask startHistoricalAnalysis(String detectorId, Instant startTime, Instant endTime) throws IOException {
        DateRange dateRange = new DateRange(startTime, endTime);
        JobRequest request = new JobRequest(detectorId, dateRange, true, START_JOB);
        JobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        return getADTask(response.getId());
    }
}
