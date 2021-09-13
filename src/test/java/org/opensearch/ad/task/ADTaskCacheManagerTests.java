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

import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.MemoryTracker.Origin.HISTORICAL_SINGLE_ENTITY_DETECTOR;
import static org.opensearch.ad.constant.CommonErrorMessages.DETECTOR_IS_RUNNING;
import static org.opensearch.ad.task.ADTaskCacheManager.TASK_RETRY_LIMIT;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.common.exception.DuplicateTaskException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import com.google.common.collect.ImmutableList;

public class ADTaskCacheManagerTests extends OpenSearchTestCase {
    private MemoryTracker memoryTracker;
    private ADTaskCacheManager adTaskCacheManager;
    private ClusterService clusterService;
    private Settings settings;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE.getKey(), 2)
            .put(AnomalyDetectorSettings.MAX_CACHED_DELETED_TASKS.getKey(), 100)
            .build();

        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays.asList(AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE, AnomalyDetectorSettings.MAX_CACHED_DELETED_TASKS)
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        memoryTracker = mock(MemoryTracker.class);
        adTaskCacheManager = new ADTaskCacheManager(settings, clusterService, memoryTracker);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        adTaskCacheManager.clear();
    }

    public void testPutTask() throws IOException {
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(true);
        ADTask adTask = TestHelpers.randomAdTask();
        adTaskCacheManager.add(adTask);
        assertEquals(1, adTaskCacheManager.size());
        assertTrue(adTaskCacheManager.contains(adTask.getTaskId()));
        assertTrue(adTaskCacheManager.containsTaskOfDetector(adTask.getDetectorId()));
        assertNotNull(adTaskCacheManager.getRcfModel(adTask.getTaskId()));
        assertNotNull(adTaskCacheManager.getShingle(adTask.getTaskId()));
        assertNotNull(adTaskCacheManager.getThresholdModel(adTask.getTaskId()));
        assertNotNull(adTaskCacheManager.getThresholdModelTrainingData(adTask.getTaskId()));
        assertFalse(adTaskCacheManager.isThresholdModelTrained(adTask.getTaskId()));
        adTaskCacheManager.remove(adTask.getTaskId());
        assertEquals(0, adTaskCacheManager.size());
    }

    public void testPutDuplicateTask() throws IOException {
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(true);
        ADTask adTask1 = TestHelpers.randomAdTask();
        adTaskCacheManager.add(adTask1);
        assertEquals(1, adTaskCacheManager.size());
        DuplicateTaskException e1 = expectThrows(DuplicateTaskException.class, () -> adTaskCacheManager.add(adTask1));
        assertEquals(DETECTOR_IS_RUNNING, e1.getMessage());

        ADTask adTask2 = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.INIT,
                adTask1.getExecutionEndTime(),
                adTask1.getStoppedBy(),
                adTask1.getDetectorId(),
                adTask1.getDetector(),
                ADTaskType.HISTORICAL_SINGLE_ENTITY
            );
        DuplicateTaskException e2 = expectThrows(DuplicateTaskException.class, () -> adTaskCacheManager.add(adTask2));
        assertEquals(DETECTOR_IS_RUNNING, e2.getMessage());
    }

    public void testPutMultipleEntityTasks() throws IOException {
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(true);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(TestHelpers.randomFeature(true)),
                null,
                Instant.now(),
                true,
                ImmutableList.of(randomAlphaOfLength(5))
            );
        ADTask adTask1 = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.CREATED,
                Instant.now(),
                null,
                detector.getDetectorId(),
                detector,
                ADTaskType.HISTORICAL_HC_ENTITY
            );
        ADTask adTask2 = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.CREATED,
                Instant.now(),
                null,
                detector.getDetectorId(),
                detector,
                ADTaskType.HISTORICAL_HC_ENTITY
            );
        adTaskCacheManager.add(adTask1);
        adTaskCacheManager.add(adTask2);
        List<String> tasks = adTaskCacheManager.getTasksOfDetector(detector.getDetectorId());
        assertEquals(2, tasks.size());
    }

    public void testAddDetector() throws IOException {
        String detectorId = randomAlphaOfLength(10);
        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR);
        adTaskCacheManager.add(detectorId, adTask);
        DuplicateTaskException e1 = expectThrows(DuplicateTaskException.class, () -> adTaskCacheManager.add(detectorId, adTask));
        assertEquals(DETECTOR_IS_RUNNING, e1.getMessage());
    }

    public void testPutTaskWithMemoryExceedLimit() {
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(false);
        LimitExceededException exception = expectThrows(
            LimitExceededException.class,
            () -> adTaskCacheManager.add(TestHelpers.randomAdTask())
        );
        assertEquals("No enough memory to run detector", exception.getMessage());
    }

    public void testThresholdModelTrained() throws IOException {
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(true);
        ADTask adTask = TestHelpers.randomAdTask();
        adTaskCacheManager.add(adTask);
        assertEquals(1, adTaskCacheManager.size());
        int size = adTaskCacheManager.addThresholdModelTrainingData(adTask.getTaskId(), randomDouble(), randomDouble());
        long cacheSize = adTaskCacheManager.trainingDataMemorySize(size);
        adTaskCacheManager.setThresholdModelTrained(adTask.getTaskId(), false);
        verify(memoryTracker, never()).releaseMemory(anyLong(), anyBoolean(), eq(HISTORICAL_SINGLE_ENTITY_DETECTOR));
        adTaskCacheManager.setThresholdModelTrained(adTask.getTaskId(), true);
        verify(memoryTracker, times(1)).releaseMemory(eq(cacheSize), eq(true), eq(HISTORICAL_SINGLE_ENTITY_DETECTOR));
    }

    public void testTaskNotExist() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> adTaskCacheManager.getRcfModel(randomAlphaOfLength(5))
        );
        assertEquals("AD task not in cache", e.getMessage());
    }

    public void testRemoveTaskWhichNotExist() {
        adTaskCacheManager.remove(randomAlphaOfLength(5));
        verify(memoryTracker, never()).releaseMemory(anyLong(), anyBoolean(), eq(HISTORICAL_SINGLE_ENTITY_DETECTOR));
    }

    public void testExceedRunningTaskLimit() throws IOException {
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(true);
        adTaskCacheManager.add(TestHelpers.randomAdTask());
        adTaskCacheManager.add(TestHelpers.randomAdTask());
        assertEquals(2, adTaskCacheManager.size());
        LimitExceededException e = expectThrows(LimitExceededException.class, () -> adTaskCacheManager.add(TestHelpers.randomAdTask()));
        assertEquals("Exceed max historical analysis limit per node: 2", e.getMessage());
    }

    public void testCancelByDetectorIdWhichNotExist() {
        String detectorId = randomAlphaOfLength(10);
        String reason = randomAlphaOfLength(10);
        String userName = randomAlphaOfLength(5);
        ADTaskCancellationState state = adTaskCacheManager.cancelByDetectorId(detectorId, reason, userName);
        assertEquals("Wrong task cancellation state", ADTaskCancellationState.NOT_FOUND, state);
    }

    public void testCancelByDetectorId() throws IOException {
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(true);
        ADTask adTask = TestHelpers.randomAdTask();
        adTaskCacheManager.add(adTask);
        String detectorId = adTask.getDetectorId();
        String reason = randomAlphaOfLength(10);
        String userName = randomAlphaOfLength(5);
        ADTaskCancellationState state = adTaskCacheManager.cancelByDetectorId(detectorId, reason, userName);
        assertEquals("Wrong task cancellation state", ADTaskCancellationState.CANCELLED, state);
        assertTrue(adTaskCacheManager.isCancelled(adTask.getTaskId()));

        state = adTaskCacheManager.cancelByDetectorId(detectorId, reason, userName);
        assertEquals("Wrong task cancellation state", ADTaskCancellationState.ALREADY_CANCELLED, state);
    }

    public void testTopEntityInited() throws IOException {
        String detectorId = randomAlphaOfLength(10);
        assertFalse(adTaskCacheManager.topEntityInited(detectorId));
        adTaskCacheManager.add(detectorId, TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR));
        assertFalse(adTaskCacheManager.topEntityInited(detectorId));
        adTaskCacheManager.setTopEntityInited(detectorId);
        assertTrue(adTaskCacheManager.topEntityInited(detectorId));
    }

    public void testEntityCache() throws IOException {
        String detectorId = randomAlphaOfLength(10);
        assertEquals(0, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(0, adTaskCacheManager.getRunningEntityCount(detectorId));
        assertEquals(0, adTaskCacheManager.getTopEntityCount(detectorId).intValue());
        adTaskCacheManager.add(detectorId, TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR));
        String entity1 = randomAlphaOfLength(5);
        String entity2 = randomAlphaOfLength(5);
        String entity3 = randomAlphaOfLength(5);
        List<String> entities = ImmutableList.of(entity1, entity2, entity3);
        adTaskCacheManager.addPendingEntities(detectorId, entities);
        adTaskCacheManager.setTopEntityCount(detectorId, entities.size());
        adTaskCacheManager.pollEntity(detectorId);
        assertEquals(3, adTaskCacheManager.getTopEntityCount(detectorId).intValue());
        assertEquals(2, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(0, adTaskCacheManager.getRunningEntityCount(detectorId));
        adTaskCacheManager.moveToRunningEntity(detectorId, entity1);
        assertEquals(3, adTaskCacheManager.getTopEntityCount(detectorId).intValue());
        assertEquals(2, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(1, adTaskCacheManager.getRunningEntityCount(detectorId));
        assertArrayEquals(new String[] { entity1 }, adTaskCacheManager.getRunningEntities(detectorId).toArray(new String[0]));

        assertFalse(adTaskCacheManager.removeRunningEntity(randomAlphaOfLength(10), entity1));
        assertFalse(adTaskCacheManager.removeRunningEntity(detectorId, randomAlphaOfLength(5)));

        assertTrue(adTaskCacheManager.removeRunningEntity(detectorId, entity1));
        assertEquals(2, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(0, adTaskCacheManager.getRunningEntityCount(detectorId));

        adTaskCacheManager.removeEntity(detectorId, entity2);
        assertEquals(1, adTaskCacheManager.getPendingEntityCount(detectorId));

        adTaskCacheManager.clearPendingEntities(detectorId);
        assertEquals(0, adTaskCacheManager.getPendingEntityCount(detectorId));

        assertNull(adTaskCacheManager.pollEntity(detectorId));

        assertNull(adTaskCacheManager.getRunningEntities(randomAlphaOfLength(10)));
    }

    public void testPushBackEntity() throws IOException {
        String detectorId = randomAlphaOfLength(10);
        adTaskCacheManager.add(detectorId, TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR));
        String entity1 = randomAlphaOfLength(5);
        String taskId = randomAlphaOfLength(5);
        adTaskCacheManager.pushBackEntity(taskId, detectorId, entity1);

        assertFalse(adTaskCacheManager.exceedRetryLimit(detectorId, taskId));
        for (int i = 0; i < TASK_RETRY_LIMIT; i++) {
            adTaskCacheManager.pushBackEntity(taskId, detectorId, entity1);
        }
        assertTrue(adTaskCacheManager.exceedRetryLimit(detectorId, taskId));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> adTaskCacheManager.exceedRetryLimit(randomAlphaOfLength(10), taskId)
        );
        assertEquals("Can't find HC detector in cache", exception.getMessage());
    }

    public void testRealtimeTaskCache() {
        String detectorId1 = randomAlphaOfLength(10);
        String newState = ADTaskState.INIT.name();
        Float newInitProgress = 0.0f;
        String newError = randomAlphaOfLength(5);
        assertTrue(adTaskCacheManager.isRealtimeTaskChanged(detectorId1, newState, newInitProgress, newError));
        // Init realtime task cache.
        adTaskCacheManager.initRealtimeTaskCache(detectorId1, 60_000);

        adTaskCacheManager.updateRealtimeTaskCache(detectorId1, newState, newInitProgress, newError);
        assertFalse(adTaskCacheManager.isRealtimeTaskChanged(detectorId1, newState, newInitProgress, newError));
        assertArrayEquals(new String[] { detectorId1 }, adTaskCacheManager.getDetectorIdsInRealtimeTaskCache());

        String detectorId2 = randomAlphaOfLength(10);
        adTaskCacheManager.updateRealtimeTaskCache(detectorId2, newState, newInitProgress, newError);
        assertEquals(1, adTaskCacheManager.getDetectorIdsInRealtimeTaskCache().length);
        adTaskCacheManager.initRealtimeTaskCache(detectorId2, 60_000);
        adTaskCacheManager.updateRealtimeTaskCache(detectorId2, newState, newInitProgress, newError);
        assertEquals(2, adTaskCacheManager.getDetectorIdsInRealtimeTaskCache().length);

        newState = ADTaskState.RUNNING.name();
        newInitProgress = 1.0f;
        newError = "test error";
        assertTrue(adTaskCacheManager.isRealtimeTaskChanged(detectorId1, newState, newInitProgress, newError));
        adTaskCacheManager.updateRealtimeTaskCache(detectorId1, newState, newInitProgress, newError);
        assertEquals(newInitProgress, adTaskCacheManager.getRealtimeTaskCache(detectorId1).getInitProgress());
        assertEquals(newState, adTaskCacheManager.getRealtimeTaskCache(detectorId1).getState());
        assertEquals(newError, adTaskCacheManager.getRealtimeTaskCache(detectorId1).getError());

        adTaskCacheManager.removeRealtimeTaskCache(detectorId1);
        assertArrayEquals(new String[] { detectorId2 }, adTaskCacheManager.getDetectorIdsInRealtimeTaskCache());

        adTaskCacheManager.clearRealtimeTaskCache();
        assertEquals(0, adTaskCacheManager.getDetectorIdsInRealtimeTaskCache().length);

    }

    public void testGetAndDecreaseEntityTaskLanes() throws IOException {
        String detectorId = randomAlphaOfLength(10);
        adTaskCacheManager.add(detectorId, TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR));
        adTaskCacheManager.setAllowedRunningEntities(detectorId, 1);
        assertEquals(1, adTaskCacheManager.getAndDecreaseEntityTaskLanes(detectorId));
        assertEquals(0, adTaskCacheManager.getAndDecreaseEntityTaskLanes(detectorId));
    }

    public void testDeletedTask() {
        String taskId = randomAlphaOfLength(10);
        adTaskCacheManager.addDeletedDetectorTask(taskId);
        assertTrue(adTaskCacheManager.hasDeletedDetectorTask());
        assertEquals(taskId, adTaskCacheManager.pollDeletedDetectorTask());
        assertFalse(adTaskCacheManager.hasDeletedDetectorTask());
    }

    public void testAcquireTaskUpdatingSemaphore() throws IOException {
        String detectorId = randomAlphaOfLength(10);
        ADTask adTask = TestHelpers.randomAdTask(ADTaskType.HISTORICAL_HC_DETECTOR);
        adTaskCacheManager.add(detectorId, adTask);
        assertTrue(adTaskCacheManager.tryAcquireTaskUpdatingSemaphore(detectorId));
        assertFalse(adTaskCacheManager.tryAcquireTaskUpdatingSemaphore(detectorId));
    }

    public void testGetTasksOfDetectorWithNonExistingDetectorId() throws IOException {
        List<String> tasksOfDetector = adTaskCacheManager.getTasksOfDetector(randomAlphaOfLength(10));
        assertEquals(0, tasksOfDetector.size());
    }

    public void testHistoricalTaskCache() throws IOException, InterruptedException {
        List<String> result = addHCDetectorCache();
        String detectorId = result.get(0);
        String detectorTaskId = result.get(1);
        assertTrue(adTaskCacheManager.containsTaskOfDetector(detectorId));
        assertTrue(adTaskCacheManager.isHCTaskCoordinatingNode(detectorId));
        assertTrue(adTaskCacheManager.isHCTaskRunning(detectorId));
        assertEquals(detectorTaskId, adTaskCacheManager.getDetectorTaskId(detectorId));
        Instant lastScaleEntityTaskLaneTime = adTaskCacheManager.getLastScaleEntityTaskLaneTime(detectorId);
        assertNotNull(lastScaleEntityTaskLaneTime);
        Thread.sleep(500);
        adTaskCacheManager.refreshLastScaleEntityTaskLaneTime(detectorId);
        assertTrue(lastScaleEntityTaskLaneTime.isBefore(adTaskCacheManager.getLastScaleEntityTaskLaneTime(detectorId)));

        adTaskCacheManager.removeHistoricalTaskCache(detectorId);
        assertFalse(adTaskCacheManager.containsTaskOfDetector(detectorId));
        assertFalse(adTaskCacheManager.isHCTaskCoordinatingNode(detectorId));
        assertFalse(adTaskCacheManager.isHCTaskRunning(detectorId));
        assertNull(adTaskCacheManager.getDetectorTaskId(detectorId));
        assertNull(adTaskCacheManager.getLastScaleEntityTaskLaneTime(detectorId));
    }

    private List<String> addHCDetectorCache() throws IOException {
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(true);
        AnomalyDetector detector = TestHelpers
            .randomAnomalyDetector(
                ImmutableList.of(TestHelpers.randomFeature(true)),
                null,
                Instant.now(),
                true,
                ImmutableList.of(randomAlphaOfLength(5))
            );
        String detectorId = detector.getDetectorId();
        ADTask adDetectorTask = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.CREATED,
                Instant.now(),
                null,
                detectorId,
                detector,
                ADTaskType.HISTORICAL_HC_DETECTOR
            );
        ADTask adEntityTask = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.CREATED,
                Instant.now(),
                null,
                detectorId,
                detector,
                ADTaskType.HISTORICAL_HC_ENTITY
            );
        adTaskCacheManager.add(detectorId, adDetectorTask);
        adTaskCacheManager.add(adEntityTask);
        assertEquals(adEntityTask.getEntity(), adTaskCacheManager.getEntity(adEntityTask.getTaskId()));
        String entityValue = randomAlphaOfLength(5);
        adTaskCacheManager.addPendingEntities(detectorId, ImmutableList.of(entityValue));
        assertEquals(1, adTaskCacheManager.getUnfinishedEntityCount(detectorId));
        return ImmutableList.of(detectorId, adDetectorTask.getTaskId(), adEntityTask.getTaskId(), entityValue);
    }

    public void testCancelHCDetector() throws IOException {
        List<String> result = addHCDetectorCache();
        String detectorId = result.get(0);
        String entityTaskId = result.get(2);
        assertFalse(adTaskCacheManager.isCancelled(entityTaskId));
        adTaskCacheManager.cancelByDetectorId(detectorId, "testReason", "testUser");
        assertTrue(adTaskCacheManager.isCancelled(entityTaskId));
    }

    public void testTempEntity() throws IOException {
        List<String> result = addHCDetectorCache();
        String detectorId = result.get(0);
        String entityValue = result.get(3);
        assertEquals(1, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(0, adTaskCacheManager.getTempEntityCount(detectorId));
        adTaskCacheManager.pollEntity(detectorId);
        assertEquals(0, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(1, adTaskCacheManager.getTempEntityCount(detectorId));
        adTaskCacheManager.addPendingEntity(detectorId, entityValue);
        assertEquals(1, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(0, adTaskCacheManager.getTempEntityCount(detectorId));
        assertNotNull(adTaskCacheManager.pollEntity(detectorId));
        assertNull(adTaskCacheManager.pollEntity(detectorId));
    }

    public void testScaleTaskSlots() throws IOException {
        List<String> result = addHCDetectorCache();
        String detectorId = result.get(0);
        int taskSlots = randomIntBetween(6, 10);
        int taskLaneLimit = randomIntBetween(1, 10);
        adTaskCacheManager.setDetectorTaskLaneLimit(detectorId, taskLaneLimit);
        adTaskCacheManager.setDetectorTaskSlots(detectorId, taskSlots);
        assertEquals(taskSlots, adTaskCacheManager.getDetectorTaskSlots(detectorId));
        int scaleUpDelta = randomIntBetween(1, 5);
        adTaskCacheManager.scaleUpDetectorTaskSlots(detectorId, scaleUpDelta);
        assertEquals(taskSlots + scaleUpDelta, adTaskCacheManager.getDetectorTaskSlots(detectorId));
        int scaleDownDelta = randomIntBetween(1, 5);
        int newTaskSlots = adTaskCacheManager.scaleDownHCDetectorTaskSlots(detectorId, scaleDownDelta);
        assertEquals(taskSlots + scaleUpDelta - scaleDownDelta, newTaskSlots);
        assertEquals(taskSlots + scaleUpDelta - scaleDownDelta, adTaskCacheManager.getDetectorTaskSlots(detectorId));
        int newTaskSlots2 = adTaskCacheManager.scaleDownHCDetectorTaskSlots(detectorId, taskSlots * 10);
        assertEquals(newTaskSlots, adTaskCacheManager.getDetectorTaskSlots(detectorId));
        assertEquals(newTaskSlots, newTaskSlots2);
    }

    public void testTaskLanes() throws IOException {
        List<String> result = addHCDetectorCache();
        String detectorId = result.get(0);
        int maxTaskLanes = randomIntBetween(1, 10);
        adTaskCacheManager.setAllowedRunningEntities(detectorId, maxTaskLanes);
        assertEquals(maxTaskLanes, adTaskCacheManager.getAvailableNewEntityTaskLanes(detectorId));
    }

    public void testRefreshRealtimeJobRunTime() throws InterruptedException {
        String detectorId = randomAlphaOfLength(5);
        adTaskCacheManager.initRealtimeTaskCache(detectorId, 1_000);
        ADRealtimeTaskCache realtimeTaskCache = adTaskCacheManager.getRealtimeTaskCache(detectorId);
        assertFalse(realtimeTaskCache.expired());
        Thread.sleep(3_000);
        assertTrue(realtimeTaskCache.expired());
        adTaskCacheManager.refreshRealtimeJobRunTime(detectorId);
        assertFalse(realtimeTaskCache.expired());
    }

    public void testAddDeletedDetector() {
        String detectorId = randomAlphaOfLength(5);
        adTaskCacheManager.addDeletedDetector(detectorId);
        String polledDetectorId = adTaskCacheManager.pollDeletedDetector();
        assertEquals(detectorId, polledDetectorId);
        assertNull(adTaskCacheManager.pollDeletedDetector());
    }

    public void testAddPendingEntitiesWithEmptyList() {
        String detectorId = randomAlphaOfLength(5);
        adTaskCacheManager.addPendingEntities(detectorId, null);
        assertEquals(0, adTaskCacheManager.getPendingEntityCount(detectorId));
        adTaskCacheManager.addPendingEntities(detectorId, ImmutableList.of());
        assertEquals(0, adTaskCacheManager.getPendingEntityCount(detectorId));
    }

    public void testMoveToRunningEntity() {
        String detectorId = randomAlphaOfLength(5);
        String entity = randomAlphaOfLength(5);
        adTaskCacheManager.addPendingEntities(detectorId, ImmutableList.of(entity));
        assertEquals(1, adTaskCacheManager.getPendingEntityCount(detectorId));
        adTaskCacheManager.moveToRunningEntity(detectorId, null);
        assertEquals(1, adTaskCacheManager.getPendingEntityCount(detectorId));
        adTaskCacheManager.moveToRunningEntity(detectorId, entity);
        assertEquals(0, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(1, adTaskCacheManager.getRunningEntityCount(detectorId));
        adTaskCacheManager.moveToRunningEntity(detectorId, entity);
        assertEquals(0, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(1, adTaskCacheManager.getRunningEntityCount(detectorId));
    }

    public void testRemoveEntity() {
        String detectorId = randomAlphaOfLength(5);
        String entity = randomAlphaOfLength(5);
        adTaskCacheManager.addPendingEntities(detectorId, ImmutableList.of(entity));
        assertEquals(1, adTaskCacheManager.getPendingEntityCount(detectorId));
        adTaskCacheManager.removeEntity(detectorId, null);
        assertEquals(1, adTaskCacheManager.getPendingEntityCount(detectorId));

        adTaskCacheManager.pollEntity(detectorId);
        assertEquals(0, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(1, adTaskCacheManager.getTempEntityCount(detectorId));
        adTaskCacheManager.removeEntity(detectorId, entity);
        assertEquals(0, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(0, adTaskCacheManager.getTempEntityCount(detectorId));
        assertEquals(0, adTaskCacheManager.getRunningEntityCount(detectorId));

        adTaskCacheManager.addPendingEntities(detectorId, ImmutableList.of(entity));
        adTaskCacheManager.moveToRunningEntity(detectorId, entity);
        assertEquals(0, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(0, adTaskCacheManager.getTempEntityCount(detectorId));
        assertEquals(1, adTaskCacheManager.getRunningEntityCount(detectorId));

        adTaskCacheManager.removeEntity(detectorId, entity);
        assertEquals(0, adTaskCacheManager.getPendingEntityCount(detectorId));
        assertEquals(0, adTaskCacheManager.getTempEntityCount(detectorId));
        assertEquals(0, adTaskCacheManager.getRunningEntityCount(detectorId));

    }
}
