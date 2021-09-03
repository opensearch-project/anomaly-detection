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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
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
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.function.Consumer;

import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.cluster.ADDataMigrator;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.DuplicateTaskException;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.AnomalyDetector;
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
import org.opensearch.index.get.GetResult;
import org.opensearch.index.mapper.MapperService;
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

        clusterService = new ClusterService(settings, clusterSettings, null);

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
}
