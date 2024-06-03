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

package org.opensearch.ad.task;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.constant.ADCommonName.DETECTION_STATE_INDEX;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.DELETE_AD_RESULT_WHEN_DELETE_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.opensearch.timeseries.TestHelpers.randomAdTask;
import static org.opensearch.timeseries.TestHelpers.randomAnomalyDetector;
import static org.opensearch.timeseries.TestHelpers.randomFeature;
import static org.opensearch.timeseries.TestHelpers.randomIntervalSchedule;
import static org.opensearch.timeseries.TestHelpers.randomIntervalTimeConfiguration;
import static org.opensearch.timeseries.TestHelpers.randomUser;
import static org.opensearch.timeseries.model.Entity.createSingleAttributeEntity;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.opensearch.Version;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.ADTaskProfileRunner;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.mock.model.MockSimpleLog;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.ADIndexJobActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.ADTaskProfileNodeResponse;
import org.opensearch.ad.transport.ADTaskProfileResponse;
import org.opensearch.ad.transport.ForwardADTaskRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.DuplicateTaskException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.InternalStatNames;
import org.opensearch.timeseries.task.RealtimeTaskCache;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.transport.StatsNodeResponse;
import org.opensearch.timeseries.transport.StatsNodesResponse;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ADTaskManagerTests extends AbstractTimeSeriesTest {

    private Settings settings;
    private Client client;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private DiscoveryNodeFilterer nodeFilter;
    private ADIndexManagement detectionIndices;
    private ADTaskCacheManager adTaskCacheManager;
    private HashRing hashRing;
    private ThreadContext threadContext;
    private TransportService transportService;
    private ADTaskManager adTaskManager;
    private ThreadPool threadPool;
    private ADIndexJobActionHandler indexAnomalyDetectorJobActionHandler;

    private DateRange detectionDateRange;
    private ActionListener<JobResponse> listener;

    private DiscoveryNode node1;
    private DiscoveryNode node2;

    private int maxRunningEntities;
    private int maxBatchTaskPerNode;

    private String historicalTaskId = "test_historical_task_id";
    private String realtimeTaskId = "test_realtime_task_id";
    private String runningHistoricalHCTaskContent = "{\"_index\":\".opendistro-anomaly-detection-state\",\"_type\":\"_doc\",\"_id\":\""
        + historicalTaskId
        + "\",\"_score\":1,\"_source\":{\"last_update_time\":1630999442827,\"state\":\"RUNNING\",\"detector_id\":"
        + "\"tQQiv3sBr1GKRuDiJ5uI\",\"task_progress\":1,\"init_progress\":1,\"execution_start_time\":1630999393798,"
        + "\"is_latest\":true,\"task_type\":\"HISTORICAL_HC_DETECTOR\",\"coordinating_node\":\"u8aYDPmaS4Ccd08Ed0GNQw\","
        + "\"detector\":{\"name\":\"test-hc1\",\"description\":\"test\",\"time_field\":\"timestamp\","
        + "\"indices\":[\"nab_ec2_cpu_utilization_24ae8d\"],\"filter_query\":{\"match_all\":{\"boost\":1}},"
        + "\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\""
        + ":{\"interval\":1,\"unit\":\"Minutes\"}},\"shingle_size\":8,\"schema_version\":0,\"feature_attributes\""
        + ":[{\"feature_id\":\"tAQiv3sBr1GKRuDiJ5ty\",\"feature_name\":\"F1\",\"feature_enabled\":true,"
        + "\"aggregation_query\":{\"f_1\":{\"sum\":{\"field\":\"value\"}}}}],\"ui_metadata\":{\"features\":"
        + "{\"F1\":{\"featureType\":\"simple_aggs\",\"aggregationBy\":\"sum\",\"aggregationOf\":\"value\"}},"
        + "\"filters\":[]},\"last_update_time\":1630999291783,\"category_field\":[\"type\"],\"detector_type\":"
        + "\"MULTI_ENTITY\"},\"detection_date_range\":{\"start_time\":1628407291580,\"end_time\":1630999291580},"
        + "\"entity\":[{\"name\":\"type\",\"value\":\"error10\"}],\"parent_task_id\":\"a1civ3sBwF58XZxvKrko\","
        + "\"worker_node\":\"DL5uOJV3TjOOAyh5hJXrCA\",\"current_piece\":1630999260000,\"execution_end_time\":1630999442814}}";

    private String taskContent = "{\"_index\":\".opendistro-anomaly-detection-state\",\"_type\":\"_doc\",\"_id\":"
        + "\"-1ojv3sBwF58XZxvtksG\",\"_score\":1,\"_source\":{\"last_update_time\":1630999442827,\"state\":\"FINISHED\""
        + ",\"detector_id\":\"tQQiv3sBr1GKRuDiJ5uI\",\"task_progress\":1,\"init_progress\":1,\"execution_start_time\""
        + ":1630999393798,\"is_latest\":true,\"task_type\":\"HISTORICAL_HC_ENTITY\",\"coordinating_node\":\""
        + "u8aYDPmaS4Ccd08Ed0GNQw\",\"detector\":{\"name\":\"test-hc1\",\"description\":\"test\",\"time_field\":\""
        + "timestamp\",\"indices\":[\"nab_ec2_cpu_utilization_24ae8d\"],\"filter_query\":{\"match_all\":{\"boost\":1}}"
        + ",\"detection_interval\":{\"period\":{\"interval\":1,\"unit\":\"Minutes\"}},\"window_delay\":{\"period\":"
        + "{\"interval\":1,\"unit\":\"Minutes\"}},\"shingle_size\":8,\"schema_version\":0,\"feature_attributes\":"
        + "[{\"feature_id\":\"tAQiv3sBr1GKRuDiJ5ty\",\"feature_name\":\"F1\",\"feature_enabled\":true,\"aggregation_query"
        + "\":{\"f_1\":{\"sum\":{\"field\":\"value\"}}}}],\"ui_metadata\":{\"features\":{\"F1\":{\"featureType\":"
        + "\"simple_aggs\",\"aggregationBy\":\"sum\",\"aggregationOf\":\"value\"}},\"filters\":[]},\"last_update_time"
        + "\":1630999291783,\"category_field\":[\"type\"],\"detector_type\":\"MULTI_ENTITY\"},\"detection_date_range\""
        + ":{\"start_time\":1628407291580,\"end_time\":1630999291580},\"entity\":[{\"name\":\"type\",\"value\":\"error10\"}]"
        + ",\"parent_task_id\":\"a1civ3sBwF58XZxvKrko\",\"worker_node\":\"DL5uOJV3TjOOAyh5hJXrCA\",\"current_piece\""
        + ":1630999260000,\"execution_end_time\":1630999442814}}";
    @Captor
    ArgumentCaptor<TransportResponseHandler<JobResponse>> remoteResponseHandler;

    NodeStateManager nodeStateManager;
    ADTaskProfileRunner taskProfileRunner;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Instant now = Instant.now();
        Instant startTime = now.minus(10, ChronoUnit.DAYS);
        Instant endTime = now.minus(1, ChronoUnit.DAYS);
        detectionDateRange = new DateRange(startTime, endTime);

        settings = Settings
            .builder()
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), 2)
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .build();

        clusterSettings = clusterSetting(
            settings,
            MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
            BATCH_TASK_PIECE_INTERVAL_SECONDS,
            AD_REQUEST_TIMEOUT,
            DELETE_AD_RESULT_WHEN_DELETE_DETECTOR,
            MAX_BATCH_TASK_PER_NODE,
            MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS,
            TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            TimeSeriesSettings.BACKOFF_MINUTES
        );

        maxBatchTaskPerNode = MAX_BATCH_TASK_PER_NODE.get(settings);
        clusterService = spy(new ClusterService(settings, clusterSettings, null));

        client = mock(Client.class);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        detectionIndices = mock(ADIndexManagement.class);
        adTaskCacheManager = mock(ADTaskCacheManager.class);
        hashRing = mock(HashRing.class);
        transportService = mock(TransportService.class);
        threadPool = mock(ThreadPool.class);
        threadContext = new ThreadContext(settings);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        when(client.threadPool()).thenReturn(threadPool);
        nodeStateManager = mock(NodeStateManager.class);
        taskProfileRunner = new ADTaskProfileRunner(hashRing, client);
        adTaskManager = spy(
            new ADTaskManager(
                settings,
                clusterService,
                client,
                TestHelpers.xContentRegistry(),
                detectionIndices,
                nodeFilter,
                hashRing,
                adTaskCacheManager,
                threadPool,
                nodeStateManager,
                taskProfileRunner
            )
        );
        indexAnomalyDetectorJobActionHandler = new ADIndexJobActionHandler(
            client,
            detectionIndices,
            mock(NamedXContentRegistry.class),
            adTaskManager,
            mock(ExecuteADResultResponseRecorder.class),
            nodeStateManager,
            Settings.EMPTY
        );

        listener = spy(new ActionListener<JobResponse>() {
            @Override
            public void onResponse(JobResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {}
        });

        node1 = new DiscoveryNode(
            "nodeName1",
            "node1",
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        node2 = new DiscoveryNode(
            "nodeName2",
            "node2",
            new TransportAddress(TransportAddress.META_ADDRESS, 9300),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        maxRunningEntities = MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS.get(settings).intValue();
    }

    private void setupHashRingWithSameLocalADVersionNodes() {
        doAnswer(invocation -> {
            Consumer<DiscoveryNode[]> function = invocation.getArgument(0);
            function.accept(new DiscoveryNode[] { node1, node2 });
            return null;
        }).when(hashRing).getNodesWithSameLocalVersion(any(), any());
    }

    private void setupTaskSlots(int node1UsedTaskSlots, int node1AssignedTaskSLots, int node2UsedTaskSlots, int node2AssignedTaskSLots) {
        doAnswer(invocation -> {
            ActionListener<StatsNodesResponse> listener = invocation.getArgument(2);
            listener
                .onResponse(
                    new StatsNodesResponse(
                        new ClusterName(randomAlphaOfLength(5)),
                        ImmutableList
                            .of(
                                new StatsNodeResponse(
                                    node1,
                                    ImmutableMap
                                        .of(
                                            InternalStatNames.AD_USED_BATCH_TASK_SLOT_COUNT.getName(),
                                            node1UsedTaskSlots,
                                            InternalStatNames.AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT.getName(),
                                            node1AssignedTaskSLots
                                        )
                                ),
                                new StatsNodeResponse(
                                    node2,
                                    ImmutableMap
                                        .of(
                                            InternalStatNames.AD_USED_BATCH_TASK_SLOT_COUNT.getName(),
                                            node2UsedTaskSlots,
                                            InternalStatNames.AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT.getName(),
                                            node2AssignedTaskSLots
                                        )
                                )
                            ),
                        ImmutableList.of()
                    )
                );
            return null;
        }).when(client).execute(any(), any(), any());
    }

    public void testCheckTaskSlotsWithNoAvailableTaskSlots() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            TaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
        );
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, maxBatchTaskPerNode, maxBatchTaskPerNode, maxBatchTaskPerNode);

        adTaskManager
            .checkTaskSlots(adTask, adTask.getDetector(), detectionDateRange, randomUser(), ADTaskAction.START, transportService, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains("No available task slot"));
    }

    private void setupSearchTopEntities(int entitySize) {
        List<Entity> entities = new ArrayList<>();
        for (int i = 0; i < entitySize; i++) {
            entities.add(createSingleAttributeEntity("category", "value" + i));
        }
    }

    public void testCheckTaskSlotsWithAvailableTaskSlotsForHC() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            TaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
        );
        setupSearchTopEntities(4);
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, maxBatchTaskPerNode, maxBatchTaskPerNode, maxBatchTaskPerNode - 1);

        adTaskManager
            .checkTaskSlots(adTask, adTask.getDetector(), detectionDateRange, randomUser(), ADTaskAction.START, transportService, listener);
        verify(adTaskManager, times(1))
            .startHistoricalAnalysis(eq(adTask.getDetector()), eq(detectionDateRange), any(), eq(1), eq(transportService), any());
    }

    public void testCheckTaskSlotsWithAvailableTaskSlotsForSingleEntityDetector() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            TaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of())
        );
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, 2, 2, 1);

        adTaskManager
            .checkTaskSlots(adTask, adTask.getDetector(), detectionDateRange, randomUser(), ADTaskAction.START, transportService, listener);
        verify(adTaskManager, times(1))
            .startHistoricalAnalysis(eq(adTask.getDetector()), eq(detectionDateRange), any(), eq(1), eq(transportService), any());
    }

    public void testCheckTaskSlotsWithAvailableTaskSlotsAndNoEntity() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            TaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
        );
        setupSearchTopEntities(0);
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, 2, 2, 1);

        adTaskManager
            .checkTaskSlots(adTask, adTask.getDetector(), detectionDateRange, randomUser(), ADTaskAction.START, transportService, listener);
        verify(adTaskManager, times(1)).startHistoricalAnalysis(any(), any(), any(), anyInt(), any(), any());
    }

    public void testCheckTaskSlotsWithAvailableTaskSlotsForScale() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            TaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
        );
        setupSearchTopEntities(4);
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, maxBatchTaskPerNode, maxBatchTaskPerNode, maxBatchTaskPerNode - 1);

        adTaskManager
            .checkTaskSlots(
                adTask,
                adTask.getDetector(),
                detectionDateRange,
                randomUser(),
                ADTaskAction.SCALE_ENTITY_TASK_SLOTS,
                transportService,
                listener
            );
        verify(adTaskManager, times(1)).scaleTaskLaneOnCoordinatingNode(eq(adTask), eq(1), eq(transportService), any());
    }

    public void testDeleteDuplicateTasks() throws IOException {
        ADTask adTask = randomAdTask();
        adTaskManager.handleTaskException(adTask, new DuplicateTaskException("test"));
        verify(client, times(1)).delete(any(), any());
    }

    public void testParseEntityForSingleCategoryHC() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            TaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
        );
        String entityValue = adTaskManager.convertEntityToString(adTask);
        Entity entity = adTaskManager.parseEntityFromString(entityValue, adTask);
        assertEquals(entity, adTask.getEntity());
    }

    public void testParseEntityForMultiCategoryHC() throws IOException {
        ADTask adTask = randomAdTask(
            randomAlphaOfLength(5),
            TaskState.INIT,
            Instant.now(),
            randomAlphaOfLength(5),
            TestHelpers
                .randomAnomalyDetectorUsingCategoryFields(
                    randomAlphaOfLength(5),
                    ImmutableList.of(randomAlphaOfLength(5), randomAlphaOfLength(5))
                )
        );
        String entityValue = adTaskManager.convertEntityToString(adTask);
        Entity entity = adTaskManager.parseEntityFromString(entityValue, adTask);
        assertEquals(entity, adTask.getEntity());
    }

    public void testDetectorTaskSlotScaleUpDelta() {
        String detectorId = randomAlphaOfLength(5);
        DiscoveryNode[] eligibleDataNodes = new DiscoveryNode[] { node1, node2 };

        // Scale down
        when(hashRing.getNodesWithSameLocalVersion()).thenReturn(eligibleDataNodes);
        when(adTaskCacheManager.getUnfinishedEntityCount(detectorId)).thenReturn(maxRunningEntities * 10);
        int taskSlots = maxRunningEntities - 1;
        when(adTaskCacheManager.getDetectorTaskSlots(detectorId)).thenReturn(taskSlots);
        int delta = adTaskManager.detectorTaskSlotScaleDelta(detectorId);
        assertEquals(maxRunningEntities - taskSlots, delta);
    }

    public void testDetectorTaskSlotScaleDownDelta() {
        String detectorId = randomAlphaOfLength(5);
        DiscoveryNode[] eligibleDataNodes = new DiscoveryNode[] { node1, node2 };

        // Scale down
        when(hashRing.getNodesWithSameLocalVersion()).thenReturn(eligibleDataNodes);
        when(adTaskCacheManager.getUnfinishedEntityCount(detectorId)).thenReturn(maxRunningEntities * 10);
        int taskSlots = maxRunningEntities * 5;
        when(adTaskCacheManager.getDetectorTaskSlots(detectorId)).thenReturn(taskSlots);
        int delta = adTaskManager.detectorTaskSlotScaleDelta(detectorId);
        assertEquals(maxRunningEntities - taskSlots, delta);
    }

    @SuppressWarnings("unchecked")
    public void testGetADTaskWithNullResponse() {
        String taskId = randomAlphaOfLength(5);
        ActionListener<Optional<ADTask>> actionListener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(null);
            return null;
        }).when(client).get(any(), any());

        adTaskManager.getADTask(taskId, actionListener);
        verify(actionListener, times(1)).onResponse(eq(Optional.empty()));
    }

    @SuppressWarnings("unchecked")
    public void testGetADTaskWithNotExistTask() {
        String taskId = randomAlphaOfLength(5);
        ActionListener<Optional<ADTask>> actionListener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            GetResponse response = new GetResponse(
                new GetResult(
                    CommonName.JOB_INDEX,
                    taskId,
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    false,
                    null,
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
            listener.onResponse(response);
            return null;
        }).when(client).get(any(), any());

        adTaskManager.getADTask(taskId, actionListener);
        verify(actionListener, times(1)).onResponse(eq(Optional.empty()));
    }

    @SuppressWarnings("unchecked")
    public void testGetADTaskWithIndexNotFoundException() {
        String taskId = randomAlphaOfLength(5);
        ActionListener<Optional<ADTask>> actionListener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new IndexNotFoundException("", ""));
            return null;
        }).when(client).get(any(), any());

        adTaskManager.getADTask(taskId, actionListener);
        verify(actionListener, times(1)).onResponse(eq(Optional.empty()));
    }

    @SuppressWarnings("unchecked")
    public void testGetADTaskWithIndexUnknownException() {
        String taskId = randomAlphaOfLength(5);
        ActionListener<Optional<ADTask>> actionListener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).get(any(), any());

        adTaskManager.getADTask(taskId, actionListener);
        verify(actionListener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testGetADTaskWithExistingTask() {
        String taskId = randomAlphaOfLength(5);
        ActionListener<Optional<ADTask>> actionListener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            ADTask adTask = randomAdTask();
            GetResponse response = new GetResponse(
                new GetResult(
                    CommonName.JOB_INDEX,
                    taskId,
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    true,
                    BytesReference.bytes(adTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
            listener.onResponse(response);
            return null;
        }).when(client).get(any(), any());

        adTaskManager.getADTask(taskId, actionListener);
        verify(actionListener, times(1)).onResponse(any());
    }

    @SuppressWarnings("unchecked")
    public void testUpdateLatestRealtimeTaskOnCoordinatingNode() {
        String detectorId = randomAlphaOfLength(5);
        String state = TaskState.RUNNING.name();
        Long rcfTotalUpdates = randomLongBetween(200, 1000);
        Long detectorIntervalInMinutes = 1L;
        String error = randomAlphaOfLength(5);
        ActionListener<UpdateResponse> actionListener = mock(ActionListener.class);
        doReturn(node1).when(clusterService).localNode();
        when(adTaskCacheManager.isRealtimeTaskChangeNeeded(anyString(), anyString(), anyFloat(), anyString())).thenReturn(true);
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> listener = invocation.getArgument(3);
            listener.onResponse(new UpdateResponse(ShardId.fromString("[test][1]"), "1", 0L, 1L, 1L, DocWriteResponse.Result.UPDATED));
            return null;
        }).when(adTaskManager).updateLatestTask(anyString(), any(), anyMap(), any());
        adTaskManager
            .updateLatestRealtimeTaskOnCoordinatingNode(
                detectorId,
                state,
                rcfTotalUpdates,
                detectorIntervalInMinutes,
                error,
                actionListener
            );
        verify(actionListener, times(1)).onResponse(any());
    }

    public void testGetLocalADTaskProfilesByDetectorId() {
        doReturn(node1).when(clusterService).localNode();
        when(adTaskCacheManager.isHCTaskRunning(anyString())).thenReturn(true);
        when(adTaskCacheManager.isHCTaskCoordinatingNode(anyString())).thenReturn(true);
        List<String> tasksOfDetector = ImmutableList.of(randomAlphaOfLength(5));
        when(adTaskCacheManager.getTasksOfDetector(anyString())).thenReturn(tasksOfDetector);
        Deque<Map.Entry<Long, Optional<double[]>>> shingle = new LinkedBlockingDeque<>();
        ThresholdedRandomCutForest trcf = mock(ThresholdedRandomCutForest.class);
        when(adTaskCacheManager.getTRcfModel(anyString())).thenReturn(trcf);
        RandomCutForest rcf = mock(RandomCutForest.class);
        when(trcf.getForest()).thenReturn(rcf);
        when(rcf.getTotalUpdates()).thenReturn(randomLongBetween(100, 1000));
        when(adTaskCacheManager.isThresholdModelTrained(anyString())).thenReturn(true);
        when(adTaskCacheManager.getThresholdModelTrainingDataSize(anyString())).thenReturn(randomIntBetween(100, 1000));
        when(adTaskCacheManager.getModelSize(anyString())).thenReturn(randomLongBetween(100, 1000));
        Entity entity = createSingleAttributeEntity(randomAlphaOfLength(5), randomAlphaOfLength(5));
        when(adTaskCacheManager.getEntity(anyString())).thenReturn(entity);
        String detectorId = randomAlphaOfLength(5);

        ExecutorService executeService = mock(ExecutorService.class);
        when(threadPool.executor(anyString())).thenReturn(executeService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executeService).execute(any());

        ADTaskProfile taskProfile = adTaskManager.getLocalADTaskProfilesByDetectorId(detectorId);
        assertEquals(1, taskProfile.getEntityTaskProfiles().size());
        verify(adTaskCacheManager, times(1)).cleanExpiredHCBatchTaskRunStates();
    }

    @SuppressWarnings("unchecked")
    public void testRemoveStaleRunningEntity() throws IOException {
        ActionListener<JobResponse> actionListener = mock(ActionListener.class);
        ADTask adTask = randomAdTask();
        String entity = randomAlphaOfLength(5);
        ExecutorService executeService = mock(ExecutorService.class);
        when(threadPool.executor(anyString())).thenReturn(executeService);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executeService).execute(any());
        when(adTaskCacheManager.removeRunningEntity(anyString(), anyString())).thenReturn(true);
        when(adTaskCacheManager.getPendingEntityCount(anyString())).thenReturn(randomIntBetween(1, 10));
        adTaskManager.removeStaleRunningEntity(adTask, entity, transportService, actionListener);
        verify(adTaskManager, times(1)).runNextEntityForHCADHistorical(any(), any(), any());

        when(adTaskCacheManager.removeRunningEntity(anyString(), anyString())).thenReturn(false);
        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(false);
        adTaskManager.removeStaleRunningEntity(adTask, entity, transportService, actionListener);
        verify(adTaskManager, times(1)).setHCDetectorTaskDone(any(), any(), any());

        when(adTaskCacheManager.hasEntity(anyString())).thenReturn(true);
        adTaskManager.removeStaleRunningEntity(adTask, entity, transportService, actionListener);
        verify(adTaskManager, times(1)).setHCDetectorTaskDone(any(), any(), any());
    }

    public void testResetLatestFlagAsFalse() throws IOException {
        List<ADTask> adTasks = new ArrayList<>();
        adTaskManager.resetLatestFlagAsFalse(adTasks);
        verify(client, never()).execute(any(), any(), any());

        ADTask adTask = randomAdTask();
        adTasks.add(adTask);
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(2);
            BulkItemResponse[] responses = new BulkItemResponse[1];
            ShardId shardId = new ShardId(new Index("index_name", "uuid"), 0);
            responses[0] = new BulkItemResponse(
                0,
                randomFrom(DocWriteRequest.OpType.values()),
                new IndexResponse(shardId, "id", 1, 1, 1, true)
            );
            listener.onResponse(new BulkResponse(responses, 1));
            return null;
        }).when(client).execute(any(), any(), any());
        adTaskManager.resetLatestFlagAsFalse(adTasks);
        verify(client, times(1)).execute(any(), any(), any());
    }

    public void testCleanADResultOfDeletedDetectorWithNoDeletedDetector() {
        when(adTaskCacheManager.pollDeletedConfig()).thenReturn(null);
        adTaskManager.cleanResultOfDeletedConfig();
        verify(client, never()).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
    }

    public void testCleanADResultOfDeletedDetectorWithException() {
        String detectorId = randomAlphaOfLength(5);
        when(adTaskCacheManager.pollDeletedConfig()).thenReturn(detectorId);

        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException("test"));
            return null;
        }).doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> listener = invocation.getArgument(2);
            BulkByScrollResponse deleteByQueryResponse = mock(BulkByScrollResponse.class);
            listener.onResponse(deleteByQueryResponse);
            return null;
        }).when(client).execute(any(), any(), any());

        settings = Settings
            .builder()
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), 2)
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .put(DELETE_AD_RESULT_WHEN_DELETE_DETECTOR.getKey(), true)
            .build();

        clusterSettings = clusterSetting(
            settings,
            MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
            BATCH_TASK_PIECE_INTERVAL_SECONDS,
            AD_REQUEST_TIMEOUT,
            DELETE_AD_RESULT_WHEN_DELETE_DETECTOR,
            MAX_BATCH_TASK_PER_NODE,
            MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS
        );

        clusterService = spy(new ClusterService(settings, clusterSettings, null));

        ADTaskManager adTaskManager = spy(
            new ADTaskManager(
                settings,
                clusterService,
                client,
                TestHelpers.xContentRegistry(),
                detectionIndices,
                nodeFilter,
                hashRing,
                adTaskCacheManager,
                threadPool,
                nodeStateManager,
                taskProfileRunner
            )
        );
        adTaskManager.cleanResultOfDeletedConfig();
        verify(client, times(1)).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        verify(adTaskCacheManager, times(1)).addDeletedConfig(eq(detectorId));

        adTaskManager.cleanResultOfDeletedConfig();
        verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        verify(adTaskCacheManager, times(1)).addDeletedConfig(eq(detectorId));
    }

    public void testMaintainRunningHistoricalTasksWithOwningNodeIsNotLocalNode() {
        // Test no owning node
        when(hashRing.getOwningNodeWithHighestVersion(anyString())).thenReturn(Optional.empty());
        adTaskManager.maintainRunningHistoricalTasks(transportService, 10);
        verify(client, never()).search(any(), any());

        // Test owning node is not local node
        when(hashRing.getOwningNodeWithHighestVersion(anyString())).thenReturn(Optional.of(node2));
        doReturn(node1).when(clusterService).localNode();
        adTaskManager.maintainRunningHistoricalTasks(transportService, 10);
        verify(client, never()).search(any(), any());
    }

    public void testMaintainRunningHistoricalTasksWithNoRunningTask() {
        when(hashRing.getOwningNodeWithHighestVersion(anyString())).thenReturn(Optional.of(node1));
        doReturn(node1).when(clusterService).localNode();

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
            InternalSearchResponse response = new InternalSearchResponse(
                    searchHits,
                    InternalAggregations.EMPTY,
                    null,
                    null,
                    false,
                    null,
                    1
                    );
            SearchResponse searchResponse = new SearchResponse(
                    response,
                    null,
                    1,
                    1,
                    0,
                    100,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                    );
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
        adTaskManager.maintainRunningHistoricalTasks(transportService, 10);
        verify(client, times(1)).search(any(), any());
    }

    public void testMaintainRunningHistoricalTasksWithRunningTask() {
        when(hashRing.getOwningNodeWithHighestVersion(anyString())).thenReturn(Optional.of(node1));
        doReturn(node1).when(clusterService).localNode();
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(threadPool).schedule(any(), any(), anyString());

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchHit task = SearchHit.fromXContent(TestHelpers.parser(runningHistoricalHCTaskContent));
            SearchHits searchHits = new SearchHits(new SearchHit[] { task }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), Float.NaN);
            InternalSearchResponse response = new InternalSearchResponse(
                    searchHits,
                    InternalAggregations.EMPTY,
                    null,
                    null,
                    false,
                    null,
                    1
                    );
            SearchResponse searchResponse = new SearchResponse(
                    response,
                    null,
                    1,
                    1,
                    0,
                    100,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                    );
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
        adTaskManager.maintainRunningHistoricalTasks(transportService, 10);
        verify(client, times(1)).search(any(), any());
    }

    public void testMaintainRunningRealtimeTasksWithNoRealtimeTask() {
        when(adTaskCacheManager.getConfigIdsInRealtimeTaskCache()).thenReturn(null);
        adTaskManager.maintainRunningRealtimeTasks();
        verify(adTaskCacheManager, never()).removeRealtimeTaskCache(anyString());

        when(adTaskCacheManager.getConfigIdsInRealtimeTaskCache()).thenReturn(new String[0]);
        adTaskManager.maintainRunningRealtimeTasks();
        verify(adTaskCacheManager, never()).removeRealtimeTaskCache(anyString());
    }

    public void testMaintainRunningRealtimeTasks() {
        String detectorId1 = randomAlphaOfLength(5);
        String detectorId2 = randomAlphaOfLength(5);
        String detectorId3 = randomAlphaOfLength(5);
        when(adTaskCacheManager.getConfigIdsInRealtimeTaskCache()).thenReturn(new String[] { detectorId1, detectorId2, detectorId3 });
        when(adTaskCacheManager.getRealtimeTaskCache(detectorId1)).thenReturn(null);

        RealtimeTaskCache cacheOfDetector2 = mock(RealtimeTaskCache.class);
        when(cacheOfDetector2.expired()).thenReturn(false);
        when(adTaskCacheManager.getRealtimeTaskCache(detectorId2)).thenReturn(cacheOfDetector2);

        RealtimeTaskCache cacheOfDetector3 = mock(RealtimeTaskCache.class);
        when(cacheOfDetector3.expired()).thenReturn(true);
        when(adTaskCacheManager.getRealtimeTaskCache(detectorId3)).thenReturn(cacheOfDetector3);

        adTaskManager.maintainRunningRealtimeTasks();
        verify(adTaskCacheManager, times(1)).removeRealtimeTaskCache(anyString());
    }

    @SuppressWarnings("unchecked")
    public void testStartHistoricalAnalysisWithNoOwningNode() throws IOException {
        AnomalyDetector detector = TestHelpers.randomAnomalyDetector(ImmutableList.of());
        DateRange detectionDateRange = TestHelpers.randomDetectionDateRange();
        User user = null;
        int availableTaskSlots = randomIntBetween(1, 10);
        ActionListener<JobResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            Consumer<Optional<DiscoveryNode>> function = invocation.getArgument(1);
            function.accept(Optional.empty());
            return null;
        }).when(hashRing).buildAndGetOwningNodeWithSameLocalVersion(anyString(), any(), any());
        adTaskManager.startHistoricalAnalysis(detector, detectionDateRange, user, availableTaskSlots, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testGetAndExecuteOnLatestADTasksWithRunningRealtimeTaskWithTaskStopped() throws IOException {
        String detectorId = randomAlphaOfLength(5);
        Consumer<List<ADTask>> function = mock(Consumer.class);
        AnomalyDetector detector = TestHelpers
            .randomDetector(
                ImmutableList.of(randomFeature(true)),
                randomAlphaOfLength(5),
                randomIntBetween(1, 10),
                MockSimpleLog.TIME_FIELD,
                ImmutableList.of(randomAlphaOfLength(5))
            );
        ADTask adTask = ADTask
            .builder()
            .taskId(randomAlphaOfLength(5))
            .taskType(ADTaskType.HISTORICAL_HC_DETECTOR.name())
            .configId(randomAlphaOfLength(5))
            .detector(detector)
            .entity(null)
            .state(TaskState.RUNNING.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .coordinatingNode(node1.getId())
            .build();
        ADTaskProfile profile = new ADTaskProfile(
            adTask,
            randomLong(),
            randomBoolean(),
            randomInt(),
            randomLong(),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomAlphaOfLength(5),
            randomInt(),
            randomBoolean(),
            randomInt(),
            randomInt(),
            randomInt(),
            ImmutableList.of(randomAlphaOfLength(5)),
            Instant.now().toEpochMilli()
        );
        setupGetAndExecuteOnLatestADTasks(profile);
        adTaskManager
            .getAndExecuteOnLatestTasks(
                detectorId,
                null,
                null,
                ADTaskType.ALL_DETECTOR_TASK_TYPES,
                function,
                transportService,
                true,
                10,
                listener
            );
        verify(client, times(2)).update(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testGetAndExecuteOnLatestADTasksWithRunningHistoricalTask() throws IOException {
        String detectorId = randomAlphaOfLength(5);
        Consumer<List<ADTask>> function = mock(Consumer.class);
        AnomalyDetector detector = TestHelpers
            .randomDetector(
                ImmutableList.of(randomFeature(true)),
                randomAlphaOfLength(5),
                randomIntBetween(1, 10),
                MockSimpleLog.TIME_FIELD,
                ImmutableList.of(randomAlphaOfLength(5))
            );
        ADTask adTask = ADTask
            .builder()
            .taskId(historicalTaskId)
            .taskType(ADTaskType.HISTORICAL_HC_DETECTOR.name())
            .configId(randomAlphaOfLength(5))
            .detector(detector)
            .entity(null)
            .state(TaskState.RUNNING.name())
            .taskProgress(0.5f)
            .initProgress(1.0f)
            .currentPiece(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(randomIntBetween(1, 100), ChronoUnit.MINUTES))
            .executionStartTime(Instant.now().truncatedTo(ChronoUnit.SECONDS).minus(100, ChronoUnit.MINUTES))
            .isLatest(true)
            .error(randomAlphaOfLength(5))
            .checkpointId(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .startedBy(randomAlphaOfLength(5))
            .lastUpdateTime(Instant.now().truncatedTo(ChronoUnit.SECONDS))
            .coordinatingNode(node1.getId())
            .build();
        ADTaskProfile profile = new ADTaskProfile(
            adTask,
            randomLong(),
            randomBoolean(),
            randomInt(),
            randomLong(),
            randomAlphaOfLength(5),
            historicalTaskId,
            randomAlphaOfLength(5),
            randomInt(),
            randomBoolean(),
            randomInt(),
            randomInt(),
            2,
            ImmutableList.of(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            Instant.now().toEpochMilli()
        );
        setupGetAndExecuteOnLatestADTasks(profile);
        adTaskManager
            .getAndExecuteOnLatestTasks(
                detectorId,
                null,
                null,
                ADTaskType.ALL_DETECTOR_TASK_TYPES,
                function,
                transportService,
                true,
                10,
                listener
            );
        verify(client, times(2)).update(any(), any());
    }

    @SuppressWarnings("unchecked")
    private void setupGetAndExecuteOnLatestADTasks(ADTaskProfile adTaskProfile) {
        String runningRealtimeHCTaskContent = runningHistoricalHCTaskContent
            .replace(ADTaskType.HISTORICAL_HC_DETECTOR.name(), ADTaskType.REALTIME_HC_DETECTOR.name())
            .replace(historicalTaskId, realtimeTaskId);

        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchHit historicalTask = SearchHit.fromXContent(TestHelpers.parser(runningHistoricalHCTaskContent));
            SearchHit realtimeTask = SearchHit.fromXContent(TestHelpers.parser(runningRealtimeHCTaskContent));
            SearchHits searchHits = new SearchHits(
                new SearchHit[] { historicalTask, realtimeTask },
                new TotalHits(2, TotalHits.Relation.EQUAL_TO),
                Float.NaN
            );
            InternalSearchResponse response = new InternalSearchResponse(
                searchHits,
                InternalAggregations.EMPTY,
                null,
                null,
                false,
                null,
                1
            );
            SearchResponse searchResponse = new SearchResponse(
                response,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());
        String detectorId = randomAlphaOfLength(5);
        Consumer<List<ADTask>> function = mock(Consumer.class);
        ActionListener<JobResponse> listener = mock(ActionListener.class);

        doAnswer(invocation -> {
            Consumer<DiscoveryNode[]> getNodeFunction = invocation.getArgument(0);
            getNodeFunction.accept(new DiscoveryNode[] { node1, node2 });
            return null;
        }).when(hashRing).getAllEligibleDataNodesWithKnownVersion(any(), any());

        doAnswer(invocation -> {
            ActionListener<ADTaskProfileResponse> taskProfileResponseListener = invocation.getArgument(2);
            AnomalyDetector detector = TestHelpers
                .randomDetector(
                    ImmutableList.of(randomFeature(true)),
                    randomAlphaOfLength(5),
                    randomIntBetween(1, 10),
                    MockSimpleLog.TIME_FIELD,
                    ImmutableList.of(randomAlphaOfLength(5))
                );
            ADTaskProfileNodeResponse nodeResponse = new ADTaskProfileNodeResponse(node1, adTaskProfile, Version.CURRENT);
            ImmutableList<ADTaskProfileNodeResponse> nodes = ImmutableList.of(nodeResponse);
            ADTaskProfileResponse taskProfileResponse = new ADTaskProfileResponse(new ClusterName("test"), nodes, ImmutableList.of());
            taskProfileResponseListener.onResponse(taskProfileResponse);
            return null;
        }).doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> updateResponselistener = invocation.getArgument(2);
            BulkByScrollResponse response = mock(BulkByScrollResponse.class);
            when(response.getBulkFailures()).thenReturn(null);
            updateResponselistener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());
        when(nodeFilter.getEligibleDataNodes()).thenReturn(new DiscoveryNode[] { node1, node2 });

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> updateResponselistener = invocation.getArgument(1);
            UpdateResponse response = new UpdateResponse(ShardId.fromString("[test][1]"), "1", 0L, 1L, 1L, DocWriteResponse.Result.UPDATED);
            updateResponselistener.onResponse(response);
            return null;
        }).when(client).update(any(), any());

        doAnswer(invocation -> {
            ActionListener<GetResponse> getResponselistener = invocation.getArgument(1);
            GetResponse response = new GetResponse(
                new GetResult(
                    CommonName.JOB_INDEX,
                    detectorId,
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    true,
                    BytesReference
                        .bytes(
                            new Job(
                                detectorId,
                                randomIntervalSchedule(),
                                randomIntervalTimeConfiguration(),
                                false,
                                Instant.now().minusSeconds(60),
                                Instant.now(),
                                Instant.now(),
                                60L,
                                TestHelpers.randomUser(),
                                null,
                                AnalysisType.AD
                            ).toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)
                        ),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
            getResponselistener.onResponse(response);
            return null;
        }).when(client).get(any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testCreateADTaskDirectlyWithException() throws IOException {
        ADTask adTask = randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR);
        Consumer<IndexResponse> function = mock(Consumer.class);
        ActionListener<IndexResponse> listener = mock(ActionListener.class);
        doThrow(new RuntimeException("test")).when(client).index(any(), any());

        adTaskManager.createTaskDirectly(adTask, function, listener);
        verify(listener, times(1)).onFailure(any());

        doAnswer(invocation -> {
            ActionListener<IndexResponse> actionListener = invocation.getArgument(1);
            actionListener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).index(any(), any());
        adTaskManager.createTaskDirectly(adTask, function, listener);
        verify(listener, times(2)).onFailure(any());
    }

    public void testCleanChildTasksAndADResultsOfDeletedTaskWithNoDeletedDetectorTask() {
        when(adTaskCacheManager.hasDeletedTask()).thenReturn(false);
        adTaskManager.cleanChildTasksAndResultsOfDeletedTask();
        verify(client, never()).execute(any(), any(), any());
    }

    public void testCleanChildTasksAndADResultsOfDeletedTaskWithNullTask() {
        when(adTaskCacheManager.hasDeletedTask()).thenReturn(true);
        when(adTaskCacheManager.pollDeletedTask()).thenReturn(null);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).execute(any(), any(), any());

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(threadPool).schedule(any(), any(), any());

        adTaskManager.cleanChildTasksAndResultsOfDeletedTask();
        verify(client, never()).execute(any(), any(), any());
    }

    public void testCleanChildTasksAndADResultsOfDeletedTaskWithFailToDeleteADResult() {
        when(adTaskCacheManager.hasDeletedTask()).thenReturn(true);
        when(adTaskCacheManager.pollDeletedTask()).thenReturn(randomAlphaOfLength(5));
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).execute(any(), any(), any());

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(threadPool).schedule(any(), any(), any());

        adTaskManager.cleanChildTasksAndResultsOfDeletedTask();
        verify(client, times(1)).execute(any(), any(), any());
    }

    public void testCleanChildTasksAndADResultsOfDeletedTask() {
        when(adTaskCacheManager.hasDeletedTask()).thenReturn(true);
        when(adTaskCacheManager.pollDeletedTask()).thenReturn(randomAlphaOfLength(5)).thenReturn(null);
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            BulkByScrollResponse response = mock(BulkByScrollResponse.class);
            actionListener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(threadPool).schedule(any(), any(), any());

        adTaskManager.cleanChildTasksAndResultsOfDeletedTask();
        verify(client, times(2)).execute(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testDeleteADTasks() {
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            BulkByScrollResponse response = mock(BulkByScrollResponse.class);
            when(response.getBulkFailures()).thenReturn(null);
            actionListener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        String detectorId = randomAlphaOfLength(5);
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);
        adTaskManager.deleteTasks(detectorId, function, listener);
        verify(function, times(1)).execute();
    }

    @SuppressWarnings("unchecked")
    public void testDeleteADTasksWithBulkFailures() {
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            BulkByScrollResponse response = mock(BulkByScrollResponse.class);
            List<BulkItemResponse.Failure> failures = ImmutableList
                .of(
                    new BulkItemResponse.Failure(
                        DETECTION_STATE_INDEX,
                        randomAlphaOfLength(5),
                        new VersionConflictEngineException(new ShardId(DETECTION_STATE_INDEX, "", 1), "id", "test")
                    )
                );
            when(response.getBulkFailures()).thenReturn(failures);
            actionListener.onResponse(response);
            return null;
        }).when(client).execute(any(), any(), any());

        String detectorId = randomAlphaOfLength(5);
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);
        adTaskManager.deleteTasks(detectorId, function, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testDeleteADTasksWithException() {
        doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new IndexNotFoundException(DETECTION_STATE_INDEX));
            return null;
        }).doAnswer(invocation -> {
            ActionListener<BulkByScrollResponse> actionListener = invocation.getArgument(2);
            actionListener.onFailure(new RuntimeException("test"));
            return null;
        }).when(client).execute(any(), any(), any());

        String detectorId = randomAlphaOfLength(5);
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<DeleteResponse> listener = mock(ActionListener.class);

        adTaskManager.deleteTasks(detectorId, function, listener);
        verify(function, times(1)).execute();
        verify(listener, never()).onFailure(any());

        adTaskManager.deleteTasks(detectorId, function, listener);
        verify(function, times(1)).execute();
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testScaleUpTaskSlots() throws IOException {
        ADTask adTask = randomAdTask(ADTaskType.HISTORICAL_HC_ENTITY);
        ActionListener<JobResponse> listener = mock(ActionListener.class);
        when(adTaskCacheManager.getAvailableNewEntityTaskLanes(anyString())).thenReturn(0);
        doReturn(2).when(adTaskManager).detectorTaskSlotScaleDelta(anyString());
        when(adTaskCacheManager.getLastScaleEntityTaskLaneTime(anyString())).thenReturn(null);

        assertEquals(0, adTaskManager.scaleTaskSlots(adTask, transportService, listener));

        when(adTaskCacheManager.getLastScaleEntityTaskLaneTime(anyString())).thenReturn(Instant.now());
        assertEquals(2, adTaskManager.scaleTaskSlots(adTask, transportService, listener));

        when(adTaskCacheManager.getLastScaleEntityTaskLaneTime(anyString())).thenReturn(Instant.now().minus(10, ChronoUnit.DAYS));
        assertEquals(2, adTaskManager.scaleTaskSlots(adTask, transportService, listener));
        verify(adTaskCacheManager, times(1)).refreshLastScaleEntityTaskLaneTime(anyString());
        verify(adTaskManager, times(1)).forwardScaleTaskSlotRequestToLeadNode(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void testForwardRequestToLeadNodeWithNotExistingNode() throws IOException {
        ADTask adTask = randomAdTask(ADTaskType.HISTORICAL_HC_ENTITY);
        ForwardADTaskRequest forwardADTaskRequest = new ForwardADTaskRequest(adTask, ADTaskAction.APPLY_FOR_TASK_SLOTS);
        ActionListener<JobResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            Consumer<Optional<DiscoveryNode>> function = invocation.getArgument(1);
            function.accept(Optional.empty());
            return null;
        }).when(hashRing).buildAndGetOwningNodeWithSameLocalVersion(any(), any(), any());

        adTaskManager.forwardRequestToLeadNode(forwardADTaskRequest, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testScaleTaskLaneOnCoordinatingNode() {
        ADTask adTask = mock(ADTask.class);
        try {
            // bring up real transport service as mockito cannot mock final method
            // and transportService.sendRequest is called. A lot of null pointer
            // exception will be thrown if we use mocked transport service.
            setUpThreadPool(ADTaskManagerTests.class.getSimpleName());
            setupTestNodes(AnomalyDetectorSettings.AD_MAX_ENTITIES_PER_QUERY, AnomalyDetectorSettings.AD_PAGE_SIZE);
            when(adTask.getCoordinatingNode()).thenReturn(testNodes[1].getNodeId());
            when(nodeFilter.getEligibleDataNodes())
                .thenReturn(new DiscoveryNode[] { testNodes[0].discoveryNode(), testNodes[1].discoveryNode() });
            ActionListener<JobResponse> listener = mock(ActionListener.class);

            adTaskManager.scaleTaskLaneOnCoordinatingNode(adTask, 2, testNodes[1].transportService, listener);
        } finally {
            tearDownTestNodes();
            tearDownThreadPool();
        }
    }

    @SuppressWarnings("unchecked")
    public void testStopDetectorWithNonExistingDetector() {
        String detectorId = randomAlphaOfLength(5);
        boolean historical = true;
        ActionListener<JobResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            Consumer<Optional<AnomalyDetector>> function = invocation.getArgument(2);
            function.accept(Optional.empty());
            return null;
        }).when(nodeStateManager).getConfig(anyString(), eq(AnalysisType.AD), any(Consumer.class), any());
        indexAnomalyDetectorJobActionHandler.stopConfig(detectorId, historical, null, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testStopDetectorWithNonExistingTask() {
        String detectorId = randomAlphaOfLength(5);
        boolean historical = true;
        ActionListener<JobResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            Consumer<Optional<AnomalyDetector>> function = invocation.getArgument(2);
            AnomalyDetector detector = randomAnomalyDetector(ImmutableList.of(randomFeature(true)));
            function.accept(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getConfig(anyString(), eq(AnalysisType.AD), any(Consumer.class), any());

        doAnswer(invocation -> {
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            actionListener.onResponse(null);
            return null;
        }).when(client).search(any(), any());

        indexAnomalyDetectorJobActionHandler.stopConfig(detectorId, historical, null, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testStopDetectorWithTaskDone() {
        String detectorId = randomAlphaOfLength(5);
        boolean historical = true;
        ActionListener<JobResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            Consumer<Optional<AnomalyDetector>> function = invocation.getArgument(2);
            AnomalyDetector detector = randomAnomalyDetector(ImmutableList.of(randomFeature(true)));
            function.accept(Optional.of(detector));
            return null;
        }).when(nodeStateManager).getConfig(anyString(), eq(AnalysisType.AD), any(Consumer.class), any());

        doAnswer(invocation -> {
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            SearchHit task = SearchHit.fromXContent(TestHelpers.parser(taskContent));
            SearchHits searchHits = new SearchHits(new SearchHit[] { task }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), Float.NaN);
            InternalSearchResponse response = new InternalSearchResponse(
                searchHits,
                InternalAggregations.EMPTY,
                null,
                null,
                false,
                null,
                1
            );
            SearchResponse searchResponse = new SearchResponse(
                response,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            actionListener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());

        indexAnomalyDetectorJobActionHandler.stopConfig(detectorId, historical, null, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testGetDetectorWithWrongContent() {
        String detectorId = randomAlphaOfLength(5);
        Consumer<Optional<? extends Config>> function = mock(Consumer.class);
        ActionListener<GetResponse> listener = mock(ActionListener.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> responseListener = invocation.getArgument(1);
            GetResponse response = new GetResponse(
                new GetResult(
                    CommonName.CONFIG_INDEX,
                    detectorId,
                    UNASSIGNED_SEQ_NO,
                    0,
                    -1,
                    true,
                    BytesReference
                        .bytes(
                            new MockSimpleLog(Instant.now(), 1.0, "127.0.0.1", "category", true, "test")
                                .toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)
                        ),
                    Collections.emptyMap(),
                    Collections.emptyMap()
                )
            );
            responseListener.onResponse(response);
            return null;
        }).when(client).get(any(), any());
        NodeStateManager nodeStateManager = new NodeStateManager(
            client,
            xContentRegistry(),
            Settings.EMPTY,
            mock(ClientUtil.class),
            mock(Clock.class),
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            clusterService,
            TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            TimeSeriesSettings.BACKOFF_MINUTES
        );
        nodeStateManager.getConfig(detectorId, AnalysisType.AD, function, listener);
        verify(listener, times(1)).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testDeleteTaskDocs() {
        doAnswer(invocation -> {
            ActionListener<SearchResponse> actionListener = invocation.getArgument(1);
            SearchHit task = SearchHit.fromXContent(TestHelpers.parser(taskContent));
            SearchHits searchHits = new SearchHits(new SearchHit[] { task }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), Float.NaN);
            InternalSearchResponse response = new InternalSearchResponse(
                searchHits,
                InternalAggregations.EMPTY,
                null,
                null,
                false,
                null,
                1
            );
            SearchResponse searchResponse = new SearchResponse(
                response,
                null,
                1,
                1,
                0,
                100,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY
            );
            actionListener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());

        doAnswer(invocation -> {
            ActionListener<BulkResponse> responseListener = invocation.getArgument(2);
            BulkItemResponse[] responses = new BulkItemResponse[1];
            ShardId shardId = new ShardId(new Index("index_name", "uuid"), 0);
            responses[0] = new BulkItemResponse(
                0,
                randomFrom(DocWriteRequest.OpType.values()),
                new IndexResponse(shardId, "id", 1, 1, 1, true)
            );
            responseListener.onResponse(new BulkResponse(responses, 1));
            return null;
        }).when(client).execute(any(), any(), any());

        String detectorId = randomAlphaOfLength(5);
        SearchRequest searchRequest = mock(SearchRequest.class);
        ExecutorFunction function = mock(ExecutorFunction.class);
        ActionListener<SearchResponse> listener = mock(ActionListener.class);
        adTaskManager.deleteTaskDocs(detectorId, searchRequest, function, listener);
        verify(adTaskCacheManager, times(1)).addDeletedTask(anyString());
        verify(function, times(1)).execute();
    }
}
