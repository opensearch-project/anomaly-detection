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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.ad.caching;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchException;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelManager.ModelType;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;

public class PriorityCacheTests extends AbstractCacheTest {
    private static final Logger LOG = LogManager.getLogger(PriorityCacheTests.class);

    EntityCache cacheProvider;
    CheckpointDao checkpoint;
    ModelManager modelManager;

    ClusterService clusterService;
    Settings settings;
    String detectorId2;
    AnomalyDetector detector2;
    double[] point;
    int dedicatedCacheSize;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        checkpoint = mock(CheckpointDao.class);

        modelManager = mock(ModelManager.class);

        clusterService = mock(ClusterService.class);
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays.asList(AnomalyDetectorSettings.DEDICATED_CACHE_SIZE, AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE)
                    )
                )
        );
        when(clusterService.getClusterSettings()).thenReturn(settings);

        dedicatedCacheSize = 1;

        threadPool = mock(ThreadPool.class);
        setUpADThreadPool(threadPool);

        EntityCache cache = new PriorityCache(
            checkpoint,
            dedicatedCacheSize,
            AnomalyDetectorSettings.CHECKPOINT_TTL,
            AnomalyDetectorSettings.MAX_INACTIVE_ENTITIES,
            memoryTracker,
            AnomalyDetectorSettings.MULTI_ENTITY_NUM_TREES,
            clock,
            clusterService,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            threadPool,
            checkpointWriteQueue,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT
        );

        cacheProvider = new CacheProvider(cache).get();

        when(memoryTracker.estimateModelSize(any(AnomalyDetector.class), anyInt())).thenReturn(memoryPerEntity);
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(true);

        detector2 = mock(AnomalyDetector.class);
        detectorId2 = "456";
        when(detector2.getDetectorId()).thenReturn(detectorId2);
        when(detector2.getDetectionIntervalDuration()).thenReturn(detectorDuration);
        when(detector2.getDetectorIntervalInSeconds()).thenReturn(detectorDuration.getSeconds());

        point = new double[] { 0.1 };
    }

    public void testCacheHit() {
        // cache miss due to door keeper
        assertEquals(null, cacheProvider.get(modelState1.getModelId(), detector));
        // cache miss due to empty cache
        assertEquals(null, cacheProvider.get(modelState1.getModelId(), detector));
        cacheProvider.hostIfPossible(detector, modelState1);
        assertEquals(1, cacheProvider.getTotalActiveEntities());
        assertEquals(1, cacheProvider.getAllModels().size());
        ModelState<EntityModel> hitState = cacheProvider.get(modelState1.getModelId(), detector);
        assertEquals(detectorId, hitState.getDetectorId());
        EntityModel model = hitState.getModel();
        assertEquals(null, model.getRcf());
        assertEquals(null, model.getThreshold());
        assertTrue(model.getSamples().isEmpty());
        modelState1.getModel().addSample(point);
        assertTrue(Arrays.equals(point, model.getSamples().peek()));

        ArgumentCaptor<Long> memoryConsumed = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Boolean> reserved = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<MemoryTracker.Origin> origin = ArgumentCaptor.forClass(MemoryTracker.Origin.class);

        verify(memoryTracker, times(1)).consumeMemory(memoryConsumed.capture(), reserved.capture(), origin.capture());
        assertEquals(dedicatedCacheSize * memoryPerEntity, memoryConsumed.getValue().intValue());
        assertEquals(true, reserved.getValue().booleanValue());
        assertEquals(MemoryTracker.Origin.HC_DETECTOR, origin.getValue());

        for (int i = 0; i < 2; i++) {
            cacheProvider.get(modelId2, detector);
        }
    }

    public void testInActiveCache() {
        // make modelId1 has enough priority
        for (int i = 0; i < 10; i++) {
            cacheProvider.get(modelId1, detector);
        }
        assertTrue(cacheProvider.hostIfPossible(detector, modelState1));
        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);
        for (int i = 0; i < 2; i++) {
            assertEquals(null, cacheProvider.get(modelId2, detector));
        }
        assertTrue(false == cacheProvider.hostIfPossible(detector, modelState2));
        // modelId2 gets put to inactive cache due to nothing in shared cache
        // and it cannot replace modelId1
        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
    }

    public void testSharedCache() {
        // make modelId1 has enough priority
        for (int i = 0; i < 10; i++) {
            cacheProvider.get(modelId1, detector);
        }
        cacheProvider.hostIfPossible(detector, modelState1);
        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        for (int i = 0; i < 2; i++) {
            cacheProvider.get(modelId2, detector);
        }
        cacheProvider.hostIfPossible(detector, modelState2);
        // modelId2 should be in shared cache
        assertEquals(2, cacheProvider.getActiveEntities(detectorId));

        for (int i = 0; i < 10; i++) {
            // put in dedicated cache
            cacheProvider.get(modelId3, detector2);
        }
        modelState3 = new ModelState<>(
            new EntityModel(entity3, new ArrayDeque<>(), null, null),
            modelId3,
            detectorId2,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        cacheProvider.hostIfPossible(detector2, modelState3);
        assertEquals(1, cacheProvider.getActiveEntities(detectorId2));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);
        for (int i = 0; i < 4; i++) {
            // replace modelId2 in shared cache
            cacheProvider.get(modelId4, detector2);
        }
        modelState4 = new ModelState<>(
            new EntityModel(entity4, new ArrayDeque<>(), null, null),
            modelId4,
            detectorId2,
            ModelType.ENTITY.getName(),
            clock,
            0
        );
        cacheProvider.hostIfPossible(detector2, modelState4);
        assertEquals(2, cacheProvider.getActiveEntities(detectorId2));
        assertEquals(3, cacheProvider.getTotalActiveEntities());
        assertEquals(3, cacheProvider.getAllModels().size());

        when(memoryTracker.memoryToShed()).thenReturn(memoryPerEntity);
        cacheProvider.maintenance();
        assertEquals(2, cacheProvider.getTotalActiveEntities());
        assertEquals(2, cacheProvider.getAllModels().size());
        assertEquals(1, cacheProvider.getActiveEntities(detectorId2));
    }

    public void testReplace() {
        for (int i = 0; i < 2; i++) {
            cacheProvider.get(modelState1.getModelId(), detector);
        }

        cacheProvider.hostIfPossible(detector, modelState1);
        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);
        ModelState<EntityModel> state = null;

        for (int i = 0; i < 4; i++) {
            cacheProvider.get(modelId2, detector);
        }

        // emptyState2 replaced emptyState2
        cacheProvider.hostIfPossible(detector, modelState2);
        state = cacheProvider.get(modelId2, detector);

        assertEquals(modelId2, state.getModelId());
        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
    }

    public void testCannotAllocateBuffer() {
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(false);
        expectThrows(LimitExceededException.class, () -> cacheProvider.get(modelId1, detector));
    }

    public void testExpiredCacheBuffer() {
        when(clock.instant()).thenReturn(Instant.MIN);
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        for (int i = 0; i < 3; i++) {
            cacheProvider.get(modelId1, detector);
        }
        for (int i = 0; i < 3; i++) {
            cacheProvider.get(modelId2, detector);
        }

        cacheProvider.hostIfPossible(detector, modelState1);
        cacheProvider.hostIfPossible(detector, modelState2);

        assertEquals(2, cacheProvider.getTotalActiveEntities());
        assertEquals(2, cacheProvider.getAllModels().size());
        when(clock.instant()).thenReturn(Instant.now());
        cacheProvider.maintenance();
        assertEquals(0, cacheProvider.getTotalActiveEntities());
        assertEquals(0, cacheProvider.getAllModels().size());

        for (int i = 0; i < 2; i++) {
            // doorkeeper should have been reset
            assertEquals(null, cacheProvider.get(modelId2, detector));
        }
    }

    public void testClear() {
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);

        for (int i = 0; i < 3; i++) {
            // make modelId1 have higher priority
            cacheProvider.get(modelId1, detector);
        }

        for (int i = 0; i < 2; i++) {
            cacheProvider.get(modelId2, detector);
        }

        cacheProvider.hostIfPossible(detector, modelState1);
        cacheProvider.hostIfPossible(detector, modelState2);

        assertEquals(2, cacheProvider.getTotalActiveEntities());
        assertTrue(cacheProvider.isActive(detectorId, modelId1));
        assertEquals(0, cacheProvider.getTotalUpdates(detectorId));
        modelState1.getModel().addSample(point);
        assertEquals(1, cacheProvider.getTotalUpdates(detectorId));
        assertEquals(1, cacheProvider.getTotalUpdates(detectorId, modelId1));
        cacheProvider.clear(detectorId);
        assertEquals(0, cacheProvider.getTotalActiveEntities());

        for (int i = 0; i < 2; i++) {
            // doorkeeper should have been reset
            assertEquals(null, cacheProvider.get(modelId2, detector));
        }
    }

    class CleanRunnable implements Runnable {
        @Override
        public void run() {
            cacheProvider.maintenance();
        }
    }

    private void setUpConcurrentMaintenance() {
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        for (int i = 0; i < 2; i++) {
            cacheProvider.get(modelId1, detector);
        }
        for (int i = 0; i < 2; i++) {
            cacheProvider.get(modelId2, detector);
        }
        for (int i = 0; i < 2; i++) {
            cacheProvider.get(modelId3, detector);
        }

        cacheProvider.hostIfPossible(detector, modelState1);
        cacheProvider.hostIfPossible(detector, modelState2);
        cacheProvider.hostIfPossible(detector, modelState3);

        when(memoryTracker.memoryToShed()).thenReturn(memoryPerEntity);
        assertEquals(3, cacheProvider.getTotalActiveEntities());
    }

    public void testSuccessfulConcurrentMaintenance() {
        setUpConcurrentMaintenance();
        final CountDownLatch inProgressLatch = new CountDownLatch(1);

        doAnswer(invovacation -> {
            inProgressLatch.await(100, TimeUnit.SECONDS);
            return null;
        }).when(memoryTracker).releaseMemory(anyLong(), anyBoolean(), any(MemoryTracker.Origin.class));

        doAnswer(invocation -> {
            inProgressLatch.countDown();
            return mock(ScheduledCancellable.class);
        }).when(threadPool).schedule(any(), any(), any());

        // both maintenance call will be blocked until schedule gets called
        new Thread(new CleanRunnable()).start();

        cacheProvider.maintenance();

        verify(threadPool, times(1)).schedule(any(), any(), any());
    }

    class FailedCleanRunnable implements Runnable {
        CountDownLatch singalThreadToStart;

        FailedCleanRunnable(CountDownLatch countDown) {
            this.singalThreadToStart = countDown;
        }

        @Override
        public void run() {
            try {
                cacheProvider.maintenance();
            } catch (OpenSearchException e) {
                singalThreadToStart.countDown();
            }
        }
    }

    public void testFailedConcurrentMaintenance() throws InterruptedException {
        setUpConcurrentMaintenance();
        final CountDownLatch scheduleCountDown = new CountDownLatch(1);
        final CountDownLatch scheduledThreadCountDown = new CountDownLatch(1);

        doThrow(NullPointerException.class).when(memoryTracker).releaseMemory(anyLong(), anyBoolean(), any(MemoryTracker.Origin.class));

        doAnswer(invovacation -> {
            scheduleCountDown.await(100, TimeUnit.SECONDS);
            return null;
        }).when(memoryTracker).syncMemoryState(any(MemoryTracker.Origin.class), anyLong(), anyLong());

        AtomicReference<Runnable> runnable = new AtomicReference<Runnable>();
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            runnable.set((Runnable) args[0]);
            scheduleCountDown.countDown();
            return mock(ScheduledCancellable.class);
        }).when(threadPool).schedule(any(), any(), any());

        try {
            // both maintenance call will be blocked until schedule gets called
            new Thread(new FailedCleanRunnable(scheduledThreadCountDown)).start();

            cacheProvider.maintenance();
        } catch (OpenSearchException e) {
            scheduledThreadCountDown.countDown();
        }

        scheduledThreadCountDown.await(100, TimeUnit.SECONDS);

        // first thread finishes and throw exception
        assertTrue(runnable.get() != null);
        try {
            // invoke second thread's runnable object
            runnable.get().run();
        } catch (Exception e2) {
            // runnable will log a line and return. It won't cause any exception.
            assertTrue(false);
            return;
        }
        // we should return here
        return;
    }
}
