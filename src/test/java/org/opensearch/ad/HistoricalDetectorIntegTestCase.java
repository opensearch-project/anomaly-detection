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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad;

import static org.opensearch.ad.model.ADTask.DETECTOR_ID_FIELD;
import static org.opensearch.ad.model.ADTask.EXECUTION_START_TIME_FIELD;
import static org.opensearch.ad.model.ADTask.IS_LATEST_FIELD;
import static org.opensearch.ad.util.RestHandlerUtils.START_JOB;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_PRIMARY_TERM;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.mock.plugin.MockReindexPlugin;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.transport.AnomalyDetectorJobAction;
import org.opensearch.ad.transport.AnomalyDetectorJobRequest;
import org.opensearch.ad.transport.AnomalyDetectorJobResponse;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.transport.MockTransportService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public abstract class HistoricalDetectorIntegTestCase extends ADIntegTestCase {

    protected String testIndex = "test_historical_data";
    protected int detectionIntervalInMinutes = 1;
    protected int DEFAULT_TEST_DATA_DOCS = 3000;

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(MockReindexPlugin.class);
        plugins.addAll(super.getMockPlugins());
        plugins.remove(MockTransportService.TestPlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    public void ingestTestData(String testIndex, Instant startTime, int detectionIntervalInMinutes, String type) {
        ingestTestData(testIndex, startTime, detectionIntervalInMinutes, type, DEFAULT_TEST_DATA_DOCS);
    }

    public void ingestTestData(String testIndex, Instant startTime, int detectionIntervalInMinutes, String type, int totalDocs) {
        createTestDataIndex(testIndex);
        List<Map<String, ?>> docs = new ArrayList<>();
        Instant currentInterval = Instant.from(startTime);

        for (int i = 0; i < totalDocs; i++) {
            currentInterval = currentInterval.plus(detectionIntervalInMinutes, ChronoUnit.MINUTES);
            double value = i % 500 == 0 ? randomDoubleBetween(1000, 2000, true) : randomDoubleBetween(10, 100, true);
            docs
                .add(
                    ImmutableMap
                        .of(
                            timeField,
                            currentInterval.toEpochMilli(),
                            "value",
                            value,
                            "type",
                            type,
                            "is_error",
                            randomBoolean(),
                            "message",
                            randomAlphaOfLength(5)
                        )
                );
        }
        BulkResponse bulkResponse = bulkIndexDocs(testIndex, docs, 30_000);
        assertEquals(RestStatus.OK, bulkResponse.status());
        assertFalse(bulkResponse.hasFailures());
        long count = countDocs(testIndex);
        assertEquals(totalDocs, count);
    }

    public Feature maxValueFeature() throws IOException {
        AggregationBuilder aggregationBuilder = TestHelpers.parseAggregation("{\"test\":{\"max\":{\"field\":\"" + valueField + "\"}}}");
        return new Feature(randomAlphaOfLength(5), randomAlphaOfLength(10), true, aggregationBuilder);
    }

    public AnomalyDetector randomDetector(DetectionDateRange dateRange, List<Feature> features) throws IOException {
        return TestHelpers.randomDetector(dateRange, features, testIndex, detectionIntervalInMinutes, timeField);
    }

    public ADTask randomCreatedADTask(String taskId, AnomalyDetector detector) {
        String detectorId = detector == null ? null : detector.getDetectorId();
        return randomCreatedADTask(taskId, detector, detectorId);
    }

    public ADTask randomCreatedADTask(String taskId, AnomalyDetector detector, String detectorId) {
        return randomADTask(taskId, detector, detectorId, ADTaskState.CREATED);
    }

    public ADTask randomADTask(String taskId, AnomalyDetector detector, String detectorId, ADTaskState state) {
        ADTask.Builder builder = ADTask
            .builder()
            .taskId(taskId)
            .taskType(ADTaskType.HISTORICAL.name())
            .detectorId(detectorId)
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
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));
        if (isLatest != null) {
            query.filter(new TermQueryBuilder(IS_LATEST_FIELD, false));
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

    public AnomalyDetectorJob getADJob(String detectorId) throws IOException {
        return toADJob(getDoc(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX, detectorId));
    }

    public ADTask toADTask(GetResponse doc) throws IOException {
        return ADTask.parse(TestHelpers.parser(doc.getSourceAsString()));
    }

    public AnomalyDetectorJob toADJob(GetResponse doc) throws IOException {
        return AnomalyDetectorJob.parse(TestHelpers.parser(doc.getSourceAsString()));
    }

    public ADTask startHistoricalDetector(Instant startTime, Instant endTime) throws IOException {
        DetectionDateRange dateRange = new DetectionDateRange(startTime, endTime);
        AnomalyDetector detector = TestHelpers
            .randomDetector(dateRange, ImmutableList.of(maxValueFeature()), testIndex, detectionIntervalInMinutes, timeField);
        String detectorId = createDetector(detector);
        AnomalyDetectorJobRequest request = new AnomalyDetectorJobRequest(
            detectorId,
            UNASSIGNED_SEQ_NO,
            UNASSIGNED_PRIMARY_TERM,
            START_JOB
        );
        AnomalyDetectorJobResponse response = client().execute(AnomalyDetectorJobAction.INSTANCE, request).actionGet(10000);
        return getADTask(response.getId());
    }
}
