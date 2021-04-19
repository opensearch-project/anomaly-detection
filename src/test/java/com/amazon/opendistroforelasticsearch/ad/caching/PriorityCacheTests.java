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

package com.amazon.opendistroforelasticsearch.ad.caching;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;

public class PriorityCacheTests extends OpenSearchTestCase {
    private static final Logger LOG = LogManager.getLogger(PriorityCacheTests.class);

    String modelId1, modelId2, modelId3, modelId4;
    EntityCache cacheProvider;
    CheckpointDao checkpoint;
    MemoryTracker memoryTracker;
    ModelManager modelManager;
    Clock clock;
    ClusterService clusterService;
    Settings settings;
    ThreadPool threadPool;
    float initialPriority;
    CacheBuffer cacheBuffer;
    long memoryPerEntity;
    String detectorId, detectorId2;
    AnomalyDetector detector, detector2;
    double[] point;
    String entityName;
    int dedicatedCacheSize;
    Duration detectorDuration;
    int numMinSamples;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        modelId1 = "1";
        modelId2 = "2";
        modelId3 = "3";
        modelId4 = "4";
        checkpoint = mock(CheckpointDao.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Optional<Entry<EntityModel, Instant>>> listener =
                (ActionListener<Optional<Entry<EntityModel, Instant>>>) args[1];
            listener.onResponse(Optional.empty());
            return null;
        }).when(checkpoint).restoreModelCheckpoint(anyString(), any(ActionListener.class));

        memoryTracker = mock(MemoryTracker.class);
        when(memoryTracker.memoryToShed()).thenReturn(0L);

        modelManager = mock(ModelManager.class);
        doNothing().when(modelManager).processEntityCheckpoint(any(Optional.class), anyString(), anyString(), any(ModelState.class));

        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());

        clusterService = mock(ClusterService.class);
        settings = Settings.EMPTY;
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.MAX_CACHE_MISS_HANDLING_PER_SECOND)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        threadPool = mock(ThreadPool.class);
        dedicatedCacheSize = 1;
        numMinSamples = 3;

        EntityCache cache = new PriorityCache(
            checkpoint,
            dedicatedCacheSize,
            AnomalyDetectorSettings.CHECKPOINT_TTL,
            AnomalyDetectorSettings.MAX_INACTIVE_ENTITIES,
            memoryTracker,
            modelManager,
            AnomalyDetectorSettings.MULTI_ENTITY_NUM_TREES,
            clock,
            clusterService,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            numMinSamples,
            settings,
            threadPool,
            // put a large value since my tests uses a lot of permits in a burst manner
            2000
        );

        cacheProvider = new CacheProvider(cache).get();

        memoryPerEntity = 81920L;
        when(memoryTracker.estimateModelSize(any(AnomalyDetector.class), anyInt())).thenReturn(memoryPerEntity);
        when(memoryTracker.canAllocateReserved(anyString(), anyLong())).thenReturn(true);

        detector = mock(AnomalyDetector.class);
        detectorId = "123";
        when(detector.getDetectorId()).thenReturn(detectorId);
        detectorDuration = Duration.ofMinutes(5);
        when(detector.getDetectionIntervalDuration()).thenReturn(detectorDuration);
        when(detector.getDetectorIntervalInSeconds()).thenReturn(detectorDuration.getSeconds());

        detector2 = mock(AnomalyDetector.class);
        detectorId2 = "456";
        when(detector2.getDetectorId()).thenReturn(detectorId2);
        when(detector2.getDetectionIntervalDuration()).thenReturn(detectorDuration);
        when(detector2.getDetectorIntervalInSeconds()).thenReturn(detectorDuration.getSeconds());

        cacheBuffer = new CacheBuffer(
            1,
            1,
            checkpoint,
            memoryPerEntity,
            memoryTracker,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            detectorId
        );

        initialPriority = cacheBuffer.getPriorityTracker().getUpdatedPriority(0);
        point = new double[] { 0.1 };
        entityName = "1.2.3.4";
    }

    public void testCacheHit() {
        // cache miss due to empty cache
        assertEquals(null, cacheProvider.get(modelId1, detector, point, entityName));
        // cache miss due to door keeper
        assertEquals(null, cacheProvider.get(modelId1, detector, point, entityName));
        assertEquals(1, cacheProvider.getTotalActiveEntities());
        assertEquals(1, cacheProvider.getAllModels().size());
        ModelState<EntityModel> hitState = cacheProvider.get(modelId1, detector, point, entityName);
        assertEquals(detectorId, hitState.getDetectorId());
        EntityModel model = hitState.getModel();
        assertEquals(null, model.getRcf());
        assertEquals(null, model.getThreshold());
        assertTrue(Arrays.equals(point, model.getSamples().peek()));

        ArgumentCaptor<Long> memoryConsumed = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Boolean> reserved = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<MemoryTracker.Origin> origin = ArgumentCaptor.forClass(MemoryTracker.Origin.class);

        verify(memoryTracker, times(1)).consumeMemory(memoryConsumed.capture(), reserved.capture(), origin.capture());
        assertEquals(dedicatedCacheSize * memoryPerEntity, memoryConsumed.getValue().intValue());
        assertEquals(true, reserved.getValue().booleanValue());
        assertEquals(MemoryTracker.Origin.MULTI_ENTITY_DETECTOR, origin.getValue());

        for (int i = 0; i < 2; i++) {
            cacheProvider.get(modelId2, detector, point, entityName);
        }
    }

    public void testInActiveCache() {
        // make modelId1 has enough priority
        for (int i = 0; i < 10; i++) {
            cacheProvider.get(modelId1, detector, point, entityName);
        }
        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);
        for (int i = 0; i < 2; i++) {
            assertEquals(null, cacheProvider.get(modelId2, detector, point, entityName));
        }
        // modelId2 gets put to inactive cache due to nothing in shared cache
        // and it cannot replace modelId1
        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
    }

    public void testSharedCache() {
        // make modelId1 has enough priority
        for (int i = 0; i < 10; i++) {
            cacheProvider.get(modelId1, detector, point, entityName);
        }
        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        for (int i = 0; i < 2; i++) {
            cacheProvider.get(modelId2, detector, point, entityName);
        }
        // modelId2 should be in shared cache
        assertEquals(2, cacheProvider.getActiveEntities(detectorId));

        for (int i = 0; i < 10; i++) {
            // put in dedicated cache
            cacheProvider.get(modelId3, detector2, point, entityName);
        }

        assertEquals(1, cacheProvider.getActiveEntities(detectorId2));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);
        for (int i = 0; i < 4; i++) {
            // replace modelId2 in shared cache
            cacheProvider.get(modelId4, detector2, point, entityName);
        }
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
            cacheProvider.get(modelId1, detector, point, entityName);
        }
        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);
        ModelState<EntityModel> state = null;
        for (int i = 0; i < 4; i++) {
            state = cacheProvider.get(modelId2, detector, point, entityName);
        }

        // modelId2 replaced modelId1
        assertEquals(modelId2, state.getModelId());
        assertTrue(Arrays.equals(point, state.getModel().getSamples().peek()));
        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
    }

    public void testCannotAllocateBuffer() {
        when(memoryTracker.canAllocateReserved(anyString(), anyLong())).thenReturn(false);
        expectThrows(LimitExceededException.class, () -> cacheProvider.get(modelId1, detector, point, entityName));
    }

    /**
     * Test that even though we put more and more samples, there are only numMinSamples stored
     */
    @SuppressWarnings("unchecked")
    public void testTooManySamples() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ModelState<EntityModel> state = (ModelState<EntityModel>) args[3];
            EntityModel model = state.getModel();
            for (int i = 0; i < 10; i++) {
                model.addSample(point);
            }
            try {
                // invalid samples cannot bed added
                model.addSample(null);
                model.addSample(new double[] {});
            } catch (Exception e) {
                assertTrue("add invalid samples should not result in failure", false);
            }

            return null;
        }).when(modelManager).processEntityCheckpoint(any(Optional.class), anyString(), anyString(), any(ModelState.class));

        ModelState<EntityModel> state = null;
        for (int i = 0; i < 10; i++) {
            state = cacheProvider.get(modelId1, detector, point, entityName);
        }
        assertEquals(numMinSamples, state.getModel().getSamples().size());
    }

    /**
     * We should have no problem when the checkpoint index does not exist yet.
     */
    @SuppressWarnings("unchecked")
    public void testIndexNotFoundException() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Optional<Entry<EntityModel, Instant>>> listener =
                (ActionListener<Optional<Entry<EntityModel, Instant>>>) args[1];
            listener.onFailure(new IndexNotFoundException("", CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(checkpoint).restoreModelCheckpoint(anyString(), any(ActionListener.class));
        ModelState<EntityModel> state = null;
        for (int i = 0; i < 3; i++) {
            state = cacheProvider.get(modelId1, detector, point, entityName);
        }
        assertEquals(1, state.getModel().getSamples().size());
    }

    @SuppressWarnings("unchecked")
    public void testThrottledRestore() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Optional<Entry<EntityModel, Instant>>> listener =
                (ActionListener<Optional<Entry<EntityModel, Instant>>>) args[1];
            listener.onFailure(new OpenSearchRejectedExecutionException("", false));
            return null;
        }).when(checkpoint).restoreModelCheckpoint(anyString(), any(ActionListener.class));
        for (int i = 0; i < 3; i++) {
            cacheProvider.get(modelId1, detector, point, entityName);
        }
        for (int i = 0; i < 3; i++) {
            cacheProvider.get(modelId2, detector, point, entityName);
        }

        // due to throttling cool down, we should only restore once
        verify(checkpoint, times(1)).restoreModelCheckpoint(anyString(), any(ActionListener.class));
    }

    // we only log error for this
    @SuppressWarnings("unchecked")
    public void testUnexpectedRestoreError() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Optional<Entry<EntityModel, Instant>>> listener =
                (ActionListener<Optional<Entry<EntityModel, Instant>>>) args[1];
            listener.onFailure(new RuntimeException());
            return null;
        }).when(checkpoint).restoreModelCheckpoint(anyString(), any(ActionListener.class));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        for (int i = 0; i < 3; i++) {
            cacheProvider.get(modelId1, detector, point, entityName);
        }
        for (int i = 0; i < 3; i++) {
            cacheProvider.get(modelId2, detector, point, entityName);
        }

        verify(checkpoint, times(2)).restoreModelCheckpoint(anyString(), any(ActionListener.class));
    }

    public void testExpiredCacheBuffer() {
        when(clock.instant()).thenReturn(Instant.MIN);
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        for (int i = 0; i < 3; i++) {
            cacheProvider.get(modelId1, detector, point, entityName);
        }
        for (int i = 0; i < 3; i++) {
            cacheProvider.get(modelId2, detector, point, entityName);
        }
        assertEquals(2, cacheProvider.getTotalActiveEntities());
        assertEquals(2, cacheProvider.getAllModels().size());
        when(clock.instant()).thenReturn(Instant.now());
        cacheProvider.maintenance();
        assertEquals(0, cacheProvider.getTotalActiveEntities());
        assertEquals(0, cacheProvider.getAllModels().size());

        for (int i = 0; i < 2; i++) {
            // doorkeeper should have been reset
            assertEquals(null, cacheProvider.get(modelId2, detector, point, entityName));
        }
    }

    public void testClear() {
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        for (int i = 0; i < 3; i++) {
            cacheProvider.get(modelId1, detector, point, entityName);
        }
        for (int i = 0; i < 3; i++) {
            cacheProvider.get(modelId2, detector, point, entityName);
        }
        assertEquals(2, cacheProvider.getTotalActiveEntities());
        assertTrue(cacheProvider.isActive(detectorId, modelId1));
        assertEquals(1, cacheProvider.getTotalUpdates(detectorId));
        assertEquals(1, cacheProvider.getTotalUpdates(detectorId, modelId1));
        cacheProvider.clear(detectorId);
        assertEquals(0, cacheProvider.getTotalActiveEntities());

        for (int i = 0; i < 2; i++) {
            // doorkeeper should have been reset
            assertEquals(null, cacheProvider.get(modelId2, detector, point, entityName));
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
            cacheProvider.get(modelId1, detector, point, entityName);
        }
        for (int i = 0; i < 2; i++) {
            cacheProvider.get(modelId2, detector, point, entityName);
        }
        for (int i = 0; i < 2; i++) {
            cacheProvider.get(modelId3, detector, point, entityName);
        }
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
