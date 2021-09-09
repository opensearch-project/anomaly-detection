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

package org.opensearch.ad.task;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.TestHelpers.randomDetector;
import static org.opensearch.ad.TestHelpers.randomFeature;
import static org.opensearch.ad.TestHelpers.randomUser;
import static org.opensearch.ad.constant.CommonErrorMessages.CREATE_INDEX_NOT_ACKNOWLEDGED;
import static org.opensearch.ad.constant.CommonErrorMessages.NO_ENTITY_FOUND;
import static org.opensearch.ad.constant.CommonName.ANOMALY_RESULT_INDEX_ALIAS;
import static org.opensearch.ad.constant.CommonName.DETECTION_STATE_INDEX;
import static org.opensearch.ad.model.Entity.createSingleAttributeEntity;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.DELETE_AD_RESULT_WHEN_DELETE_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

import org.apache.lucene.search.TotalHits;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.cluster.ADDataMigrator;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.DuplicateTaskException;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import org.opensearch.ad.stats.InternalStatNames;
import org.opensearch.ad.transport.ADStatsNodeResponse;
import org.opensearch.ad.transport.ADStatsNodesResponse;
import org.opensearch.ad.transport.AnomalyDetectorJobResponse;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.index.Index;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ADTaskManagerTests extends ADUnitTestCase {

    private Settings settings;
    private Client client;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private DiscoveryNodeFilterer nodeFilter;
    private AnomalyDetectionIndices detectionIndices;
    private ADTaskCacheManager adTaskCacheManager;
    private HashRing hashRing;
    private TransportService transportService;
    private ADTaskManager adTaskManager;
    private ThreadPool threadPool;
    private ADDataMigrator dataMigrator;
    private SearchFeatureDao searchFeatureDao;
    private IndexAnomalyDetectorJobActionHandler indexAnomalyDetectorJobActionHandler;

    private DetectionDateRange detectionDateRange;
    private ActionListener<AnomalyDetectorJobResponse> listener;

    private DiscoveryNode node1;
    private DiscoveryNode node2;

    private int maxRunningEntities;
    @Captor
    ArgumentCaptor<TransportResponseHandler<AnomalyDetectorJobResponse>> remoteResponseHandler;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Instant now = Instant.now();
        Instant startTime = now.minus(10, ChronoUnit.DAYS);
        Instant endTime = now.minus(1, ChronoUnit.DAYS);
        detectionDateRange = new DetectionDateRange(startTime, endTime);

        settings = Settings
            .builder()
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), 2)
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .build();

        clusterSettings = clusterSetting(
            settings,
            MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
            BATCH_TASK_PIECE_INTERVAL_SECONDS,
            REQUEST_TIMEOUT,
            DELETE_AD_RESULT_WHEN_DELETE_DETECTOR,
            MAX_BATCH_TASK_PER_NODE,
            MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS
        );

        clusterService = spy(new ClusterService(settings, clusterSettings, null));

        client = mock(Client.class);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        detectionIndices = mock(AnomalyDetectionIndices.class);
        adTaskCacheManager = mock(ADTaskCacheManager.class);
        hashRing = mock(HashRing.class);
        transportService = mock(TransportService.class);
        threadPool = mock(ThreadPool.class);
        dataMigrator = mock(ADDataMigrator.class);
        searchFeatureDao = mock(SearchFeatureDao.class);
        indexAnomalyDetectorJobActionHandler = mock(IndexAnomalyDetectorJobActionHandler.class);
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
                dataMigrator,
                searchFeatureDao
            )
        );

        listener = spy(new ActionListener<AnomalyDetectorJobResponse>() {
            @Override
            public void onResponse(AnomalyDetectorJobResponse bulkItemResponses) {}

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

    private void setupGetDetector(AnomalyDetector detector) {
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener
                .onResponse(
                    new GetResponse(
                        new GetResult(
                            AnomalyDetector.ANOMALY_DETECTORS_INDEX,
                            MapperService.SINGLE_MAPPING_NAME,
                            detector.getDetectorId(),
                            UNASSIGNED_SEQ_NO,
                            0,
                            -1,
                            true,
                            BytesReference.bytes(detector.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS)),
                            Collections.emptyMap(),
                            Collections.emptyMap()
                        )
                    )
                );
            return null;
        }).when(client).get(any(), any());
    }

    private void setupHashRingWithSameLocalADVersionNodes() {
        doAnswer(invocation -> {
            Consumer<DiscoveryNode[]> function = invocation.getArgument(0);
            function.accept(new DiscoveryNode[] { node1, node2 });
            return null;
        }).when(hashRing).getNodesWithSameLocalAdVersion(any(), any());
    }

    private void setupHashRingWithOwningNode() {
        doAnswer(invocation -> {
            Consumer<Optional<DiscoveryNode>> function = invocation.getArgument(1);
            function.accept(Optional.of(node1));
            return null;
        }).when(hashRing).buildAndGetOwningNodeWithSameLocalAdVersion(any(), any(), any());
    }

    public void testCreateTaskIndexNotAcknowledged() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(false, false, ANOMALY_RESULT_INDEX_ALIAS));
            return null;
        }).when(detectionIndices).initDetectionStateIndex(any());
        doReturn(false).when(detectionIndices).doesDetectorStateIndexExist();
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector);

        adTaskManager.startDetector(detector, detectionDateRange, randomUser(), transportService, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        String error = String.format(Locale.ROOT, CREATE_INDEX_NOT_ACKNOWLEDGED, DETECTION_STATE_INDEX);
        assertEquals(error, exceptionCaptor.getValue().getMessage());
    }

    public void testCreateTaskIndexWithResourceAlreadyExistsException() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException("index created"));
            return null;
        }).when(detectionIndices).initDetectionStateIndex(any());
        doReturn(false).when(detectionIndices).doesDetectorStateIndexExist();
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector);

        adTaskManager.startDetector(detector, detectionDateRange, randomUser(), transportService, listener);
        verify(listener, never()).onFailure(any());
    }

    public void testCreateTaskIndexWithException() throws IOException {
        String error = randomAlphaOfLength(5);
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException(error));
            return null;
        }).when(detectionIndices).initDetectionStateIndex(any());
        doReturn(false).when(detectionIndices).doesDetectorStateIndexExist();
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector);

        adTaskManager.startDetector(detector, detectionDateRange, randomUser(), transportService, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(error, exceptionCaptor.getValue().getMessage());
    }

    public void testStartDetectorWithNoEnabledFeature() throws IOException {
        AnomalyDetector detector = randomDetector(
            ImmutableList.of(randomFeature(false)),
            randomAlphaOfLength(5),
            1,
            randomAlphaOfLength(5)
        );
        setupGetDetector(detector);

        adTaskManager
            .startDetector(
                detector.getDetectorId(),
                detectionDateRange,
                indexAnomalyDetectorJobActionHandler,
                randomUser(),
                transportService,
                listener
            );
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
    }

    @SuppressWarnings("unchecked")
    public void testStartDetectorForHistoricalAnalysis() throws IOException {
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector);
        setupHashRingWithOwningNode();

        adTaskManager
            .startDetector(
                detector.getDetectorId(),
                detectionDateRange,
                indexAnomalyDetectorJobActionHandler,
                randomUser(),
                transportService,
                listener
            );
        verify(adTaskManager, times(1)).forwardRequestToLeadNode(any(), any(), any());
    }

    private void setupTaskSlots(int node1UsedTaskSlots, int node1AssignedTaskSLots, int node2UsedTaskSlots, int node2AssignedTaskSLots) {
        doAnswer(invocation -> {
            ActionListener<ADStatsNodesResponse> listener = invocation.getArgument(2);
            listener
                .onResponse(
                    new ADStatsNodesResponse(
                        new ClusterName(randomAlphaOfLength(5)),
                        ImmutableList
                            .of(
                                new ADStatsNodeResponse(
                                    node1,
                                    ImmutableMap
                                        .of(
                                            InternalStatNames.AD_USED_BATCH_TASK_SLOT_COUNT.getName(),
                                            node1UsedTaskSlots,
                                            InternalStatNames.AD_DETECTOR_ASSIGNED_BATCH_TASK_SLOT_COUNT.getName(),
                                            node1AssignedTaskSLots
                                        )
                                ),
                                new ADStatsNodeResponse(
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
        ADTask adTask = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.INIT,
                Instant.now(),
                randomAlphaOfLength(5),
                TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
            );
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, 2, 2, 2);

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
        doAnswer(invocation -> {
            ActionListener<List<Entity>> listener = invocation.getArgument(6);
            listener.onResponse(entities);
            return null;
        }).when(searchFeatureDao).getHighestCountEntities(any(), anyLong(), anyLong(), anyInt(), anyInt(), anyInt(), any());
    }

    public void testCheckTaskSlotsWithAvailableTaskSlotsForHC() throws IOException {
        ADTask adTask = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.INIT,
                Instant.now(),
                randomAlphaOfLength(5),
                TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
            );
        setupSearchTopEntities(4);
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, 2, 2, 1);

        adTaskManager
            .checkTaskSlots(adTask, adTask.getDetector(), detectionDateRange, randomUser(), ADTaskAction.START, transportService, listener);
        verify(adTaskManager, times(1))
            .startHistoricalAnalysis(eq(adTask.getDetector()), eq(detectionDateRange), any(), eq(1), eq(transportService), any());
    }

    public void testCheckTaskSlotsWithAvailableTaskSlotsForSingleEntityDetector() throws IOException {
        ADTask adTask = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.INIT,
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
        ADTask adTask = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.INIT,
                Instant.now(),
                randomAlphaOfLength(5),
                TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
            );
        setupSearchTopEntities(0);
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, 2, 2, 1);

        adTaskManager
            .checkTaskSlots(adTask, adTask.getDetector(), detectionDateRange, randomUser(), ADTaskAction.START, transportService, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertTrue(exceptionCaptor.getValue().getMessage().contains(NO_ENTITY_FOUND));
    }

    public void testCheckTaskSlotsWithAvailableTaskSlotsForScale() throws IOException {
        ADTask adTask = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.INIT,
                Instant.now(),
                randomAlphaOfLength(5),
                TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
            );
        setupSearchTopEntities(4);
        setupHashRingWithSameLocalADVersionNodes();

        setupTaskSlots(0, 2, 2, 1);

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
        ADTask adTask = TestHelpers.randomAdTask();
        adTaskManager.handleADTaskException(adTask, new DuplicateTaskException("test"));
        verify(client, times(1)).delete(any(), any());
    }

    public void testParseEntityForSingleCategoryHC() throws IOException {
        ADTask adTask = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.INIT,
                Instant.now(),
                randomAlphaOfLength(5),
                TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
            );
        String entityValue = adTaskManager.convertEntityToString(adTask);
        Entity entity = adTaskManager.parseEntityFromString(entityValue, adTask);
        assertEquals(entity, adTask.getEntity());
    }

    public void testParseEntityForMultiCategoryHC() throws IOException {
        ADTask adTask = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.INIT,
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
        when(hashRing.getNodesWithSameLocalAdVersion()).thenReturn(eligibleDataNodes);
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
        when(hashRing.getNodesWithSameLocalAdVersion()).thenReturn(eligibleDataNodes);
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
                    AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX,
                    MapperService.SINGLE_MAPPING_NAME,
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
            ADTask adTask = TestHelpers.randomAdTask();
            GetResponse response = new GetResponse(
                new GetResult(
                    AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX,
                    MapperService.SINGLE_MAPPING_NAME,
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
        String state = ADTaskState.RUNNING.name();
        Long rcfTotalUpdates = randomLongBetween(200, 1000);
        Long detectorIntervalInMinutes = 1L;
        String error = randomAlphaOfLength(5);
        ActionListener<UpdateResponse> actionListener = mock(ActionListener.class);
        doReturn(node1).when(clusterService).localNode();
        when(adTaskCacheManager.isRealtimeTaskChanged(anyString(), anyString(), anyFloat(), anyString())).thenReturn(true);
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> listener = invocation.getArgument(3);
            listener
                .onResponse(
                    new UpdateResponse(
                        ShardId.fromString("[test][1]"),
                        CommonName.MAPPING_TYPE,
                        "1",
                        0L,
                        1L,
                        1L,
                        DocWriteResponse.Result.UPDATED
                    )
                );
            return null;
        }).when(adTaskManager).updateLatestADTask(anyString(), any(), anyMap(), any());
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
        when(adTaskCacheManager.getShingle(anyString())).thenReturn(shingle);
        when(adTaskCacheManager.getRcfModelTotalUpdates(anyString())).thenReturn(randomLongBetween(100, 1000));
        when(adTaskCacheManager.isThresholdModelTrained(anyString())).thenReturn(true);
        when(adTaskCacheManager.getThresholdModelTrainingDataSize(anyString())).thenReturn(randomIntBetween(100, 1000));
        when(adTaskCacheManager.getModelSize(anyString())).thenReturn(randomLongBetween(100, 1000));
        Entity entity = createSingleAttributeEntity(randomAlphaOfLength(5), randomAlphaOfLength(5));
        when(adTaskCacheManager.getEntity(anyString())).thenReturn(entity);
        String detectorId = randomAlphaOfLength(5);
        ADTaskProfile taskProfile = adTaskManager.getLocalADTaskProfilesByDetectorId(detectorId);
        assertEquals(1, taskProfile.getEntityTaskProfiles().size());
    }

    @SuppressWarnings("unchecked")
    public void testRemoveStaleRunningEntity() throws IOException {
        ActionListener<AnomalyDetectorJobResponse> actionListener = mock(ActionListener.class);
        ADTask adTask = TestHelpers.randomAdTask();
        String entity = randomAlphaOfLength(5);
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

        ADTask adTask = TestHelpers.randomAdTask();
        adTasks.add(adTask);
        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(2);
            BulkItemResponse[] responses = new BulkItemResponse[1];
            ShardId shardId = new ShardId(new Index("index_name", "uuid"), 0);
            responses[0] = new BulkItemResponse(
                0,
                randomFrom(DocWriteRequest.OpType.values()),
                new IndexResponse(shardId, "_doc", "id", 1, 1, 1, true)
            );
            listener.onResponse(new BulkResponse(responses, 1));
            return null;
        }).when(client).execute(any(), any(), any());
        adTaskManager.resetLatestFlagAsFalse(adTasks);
        verify(client, times(1)).execute(any(), any(), any());
    }

    public void testCleanADResultOfDeletedDetectorWithNoDeletedDetector() {
        when(adTaskCacheManager.pollDeletedDetector()).thenReturn(null);
        adTaskManager.cleanADResultOfDeletedDetector();
        verify(client, never()).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
    }

    public void testCleanADResultOfDeletedDetectorWithException() {
        String detectorId = randomAlphaOfLength(5);
        when(adTaskCacheManager.pollDeletedDetector()).thenReturn(detectorId);

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
            .put(REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .put(DELETE_AD_RESULT_WHEN_DELETE_DETECTOR.getKey(), true)
            .build();

        clusterSettings = clusterSetting(
            settings,
            MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
            BATCH_TASK_PIECE_INTERVAL_SECONDS,
            REQUEST_TIMEOUT,
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
                dataMigrator,
                searchFeatureDao
            )
        );
        adTaskManager.cleanADResultOfDeletedDetector();
        verify(client, times(1)).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        verify(adTaskCacheManager, times(1)).addDeletedDetector(eq(detectorId));

        adTaskManager.cleanADResultOfDeletedDetector();
        verify(client, times(2)).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());
        verify(adTaskCacheManager, times(1)).addDeletedDetector(eq(detectorId));
    }

    public void testMaintainRunningHistoricalTasksWithOwningNodeIsNotLocalNode() {
        // Test no owning node
        when(hashRing.getOwningNodeWithHighestAdVersion(anyString())).thenReturn(Optional.empty());
        adTaskManager.maintainRunningHistoricalTasks(transportService, 10);
        verify(client, never()).search(any(), any());

        // Test owning node is not local node
        when(hashRing.getOwningNodeWithHighestAdVersion(anyString())).thenReturn(Optional.of(node2));
        doReturn(node1).when(clusterService).localNode();
        adTaskManager.maintainRunningHistoricalTasks(transportService, 10);
        verify(client, never()).search(any(), any());
    }

    public void testMaintainRunningHistoricalTasksWithNoRunningTask() {
        when(hashRing.getOwningNodeWithHighestAdVersion(anyString())).thenReturn(Optional.of(node1));
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
        when(hashRing.getOwningNodeWithHighestAdVersion(anyString())).thenReturn(Optional.of(node1));
        doReturn(node1).when(clusterService).localNode();

        String taskContent = "{\"_index\":\".opendistro-anomaly-detection-state\",\"_type\":\"_doc\",\"_id\":\"test_id\","
            + "\"_score\":1,\"_source\":{\"last_update_time\":1630999442827,\"state\":\"RUNNING\",\"detector_id\":"
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
        doAnswer(invocation -> {
            ActionListener<SearchResponse> listener = invocation.getArgument(1);
            SearchHit task = SearchHit.fromXContent(TestHelpers.parser(taskContent));
            SearchHits searchHits = new SearchHits(new SearchHit[] { task }, new TotalHits(2, TotalHits.Relation.EQUAL_TO), Float.NaN);
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
        when(adTaskCacheManager.getDetectorIdsInRealtimeTaskCache()).thenReturn(null);
        adTaskManager.maintainRunningRealtimeTasks();
        verify(adTaskCacheManager, never()).removeRealtimeTaskCache(anyString());

        when(adTaskCacheManager.getDetectorIdsInRealtimeTaskCache()).thenReturn(new String[0]);
        adTaskManager.maintainRunningRealtimeTasks();
        verify(adTaskCacheManager, never()).removeRealtimeTaskCache(anyString());
    }

    public void testMaintainRunningRealtimeTasks() {
        String detectorId1 = randomAlphaOfLength(5);
        String detectorId2 = randomAlphaOfLength(5);
        String detectorId3 = randomAlphaOfLength(5);
        when(adTaskCacheManager.getDetectorIdsInRealtimeTaskCache()).thenReturn(new String[] { detectorId1, detectorId2, detectorId3 });
        when(adTaskCacheManager.getRealtimeTaskCache(detectorId1)).thenReturn(null);

        ADRealtimeTaskCache cacheOfDetector2 = mock(ADRealtimeTaskCache.class);
        when(cacheOfDetector2.expired()).thenReturn(false);
        when(adTaskCacheManager.getRealtimeTaskCache(detectorId2)).thenReturn(cacheOfDetector2);

        ADRealtimeTaskCache cacheOfDetector3 = mock(ADRealtimeTaskCache.class);
        when(cacheOfDetector3.expired()).thenReturn(true);
        when(adTaskCacheManager.getRealtimeTaskCache(detectorId3)).thenReturn(cacheOfDetector3);

        adTaskManager.maintainRunningRealtimeTasks();
        verify(adTaskCacheManager, times(1)).removeRealtimeTaskCache(anyString());
    }
}
