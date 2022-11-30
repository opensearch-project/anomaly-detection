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

package org.opensearch.ad.caching;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyDouble;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelManager.ModelType;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.jvm.JvmInfo.Mem;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;

public class PriorityCacheTests extends AbstractCacheTest {
    private static final Logger LOG = LogManager.getLogger(PriorityCacheTests.class);

    EntityCache entityCache;
    CheckpointDao checkpoint;
    ModelManager modelManager;

    ClusterService clusterService;
    Settings settings;
    String detectorId2;
    AnomalyDetector detector2;
    double[] point;
    int dedicatedCacheSize;

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
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.DEDICATED_CACHE_SIZE,
                                AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
                                AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
                                AnomalyDetectorSettings.CHECKPOINT_TTL,
                                AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ
                            )
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
            AnomalyDetectorSettings.NUM_TREES,
            clock,
            clusterService,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            threadPool,
            checkpointWriteQueue,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            checkpointMaintainQueue,
            Settings.EMPTY,
            AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ
        );

        CacheProvider cacheProvider = new CacheProvider();
        cacheProvider.set(cache);
        entityCache = cacheProvider.get();

        when(memoryTracker.estimateTRCFModelSize(anyInt(), anyInt(), anyDouble(), anyInt(), anyBoolean())).thenReturn(memoryPerEntity);
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(true);

        detector2 = mock(AnomalyDetector.class);
        detectorId2 = "456";
        when(detector2.getDetectorId()).thenReturn(detectorId2);
        when(detector2.getDetectionIntervalDuration()).thenReturn(detectorDuration);
        when(detector2.getDetectorIntervalInSeconds()).thenReturn(detectorDuration.getSeconds());

        point = new double[] { 0.1 };
    }

    public void testCacheHit() {
        // 800 MB is the limit
        long largeHeapSize = 800_000_000;
        JvmInfo info = mock(JvmInfo.class);
        Mem mem = mock(Mem.class);
        when(info.getMem()).thenReturn(mem);
        when(mem.getHeapMax()).thenReturn(new ByteSizeValue(largeHeapSize));
        JvmService jvmService = mock(JvmService.class);
        when(jvmService.info()).thenReturn(info);

        // ClusterService clusterService = mock(ClusterService.class);
        float modelMaxPercen = 0.1f;
        // Settings settings = Settings.builder().put(AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.getKey(), modelMaxPercen).build();
        // ClusterSettings clusterSettings = new ClusterSettings(
        // settings,
        // Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE)))
        // );
        // when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        memoryTracker = spy(new MemoryTracker(jvmService, modelMaxPercen, 0.002, clusterService, mock(ADCircuitBreakerService.class)));

        EntityCache cache = new PriorityCache(
            checkpoint,
            dedicatedCacheSize,
            AnomalyDetectorSettings.CHECKPOINT_TTL,
            AnomalyDetectorSettings.MAX_INACTIVE_ENTITIES,
            memoryTracker,
            AnomalyDetectorSettings.NUM_TREES,
            clock,
            clusterService,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            threadPool,
            checkpointWriteQueue,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            checkpointMaintainQueue,
            Settings.EMPTY,
            AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ
        );

        CacheProvider cacheProvider = new CacheProvider();
        cacheProvider.set(cache);
        entityCache = cacheProvider.get();

        // cache miss due to door keeper
        assertEquals(null, entityCache.get(modelState1.getModelId(), detector));
        // cache miss due to empty cache
        assertEquals(null, entityCache.get(modelState1.getModelId(), detector));
        entityCache.hostIfPossible(detector, modelState1);
        assertEquals(1, entityCache.getTotalActiveEntities());
        assertEquals(1, entityCache.getAllModels().size());
        ModelState<EntityModel> hitState = entityCache.get(modelState1.getModelId(), detector);
        assertEquals(detectorId, hitState.getDetectorId());
        EntityModel model = hitState.getModel();
        assertEquals(false, model.getTrcf().isPresent());
        assertTrue(model.getSamples().isEmpty());
        modelState1.getModel().addSample(point);
        assertTrue(Arrays.equals(point, model.getSamples().peek()));

        ArgumentCaptor<Long> memoryConsumed = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Boolean> reserved = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<MemoryTracker.Origin> origin = ArgumentCaptor.forClass(MemoryTracker.Origin.class);

        // input dimension: 3, shingle: 4
        long expectedMemoryPerEntity = 436828L;
        verify(memoryTracker, times(1)).consumeMemory(memoryConsumed.capture(), reserved.capture(), origin.capture());
        assertEquals(dedicatedCacheSize * expectedMemoryPerEntity, memoryConsumed.getValue().intValue());
        assertEquals(true, reserved.getValue().booleanValue());
        assertEquals(MemoryTracker.Origin.HC_DETECTOR, origin.getValue());

        // for (int i = 0; i < 2; i++) {
        // cacheProvider.get(modelId2, detector);
        // }
    }

    public void testInActiveCache() {
        // make modelId1 has enough priority
        for (int i = 0; i < 10; i++) {
            entityCache.get(modelId1, detector);
        }
        assertTrue(entityCache.hostIfPossible(detector, modelState1));
        assertEquals(1, entityCache.getActiveEntities(detectorId));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);
        for (int i = 0; i < 2; i++) {
            assertEquals(null, entityCache.get(modelId2, detector));
        }
        assertTrue(false == entityCache.hostIfPossible(detector, modelState2));
        // modelId2 gets put to inactive cache due to nothing in shared cache
        // and it cannot replace modelId1
        assertEquals(1, entityCache.getActiveEntities(detectorId));
    }

    public void testSharedCache() {
        // make modelId1 has enough priority
        for (int i = 0; i < 10; i++) {
            entityCache.get(modelId1, detector);
        }
        entityCache.hostIfPossible(detector, modelState1);
        assertEquals(1, entityCache.getActiveEntities(detectorId));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        for (int i = 0; i < 2; i++) {
            entityCache.get(modelId2, detector);
        }
        entityCache.hostIfPossible(detector, modelState2);
        // modelId2 should be in shared cache
        assertEquals(2, entityCache.getActiveEntities(detectorId));

        for (int i = 0; i < 10; i++) {
            entityCache.get(modelId3, detector2);
        }
        modelState3 = new ModelState<>(
            new EntityModel(entity3, new ArrayDeque<>(), null),
            modelId3,
            detectorId2,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        entityCache.hostIfPossible(detector2, modelState3);
        assertEquals(1, entityCache.getActiveEntities(detectorId2));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);
        for (int i = 0; i < 4; i++) {
            // replace modelId2 in shared cache
            entityCache.get(modelId4, detector2);
        }
        modelState4 = new ModelState<>(
            new EntityModel(entity4, new ArrayDeque<>(), null),
            modelId4,
            detectorId2,
            ModelType.ENTITY.getName(),
            clock,
            0
        );
        entityCache.hostIfPossible(detector2, modelState4);
        assertEquals(2, entityCache.getActiveEntities(detectorId2));
        assertEquals(3, entityCache.getTotalActiveEntities());
        assertEquals(3, entityCache.getAllModels().size());

        when(memoryTracker.memoryToShed()).thenReturn(memoryPerEntity);
        entityCache.maintenance();
        assertEquals(2, entityCache.getTotalActiveEntities());
        assertEquals(2, entityCache.getAllModels().size());
        assertEquals(1, entityCache.getActiveEntities(detectorId2));
    }

    public void testReplace() {
        for (int i = 0; i < 2; i++) {
            entityCache.get(modelState1.getModelId(), detector);
        }

        entityCache.hostIfPossible(detector, modelState1);
        assertEquals(1, entityCache.getActiveEntities(detectorId));
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);
        ModelState<EntityModel> state = null;

        for (int i = 0; i < 4; i++) {
            entityCache.get(modelId2, detector);
        }

        // emptyState2 replaced emptyState2
        entityCache.hostIfPossible(detector, modelState2);
        state = entityCache.get(modelId2, detector);

        assertEquals(modelId2, state.getModelId());
        assertEquals(1, entityCache.getActiveEntities(detectorId));
    }

    public void testCannotAllocateBuffer() {
        when(memoryTracker.canAllocateReserved(anyLong())).thenReturn(false);
        expectThrows(LimitExceededException.class, () -> entityCache.get(modelId1, detector));
    }

    public void testExpiredCacheBuffer() {
        when(clock.instant()).thenReturn(Instant.MIN);
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        for (int i = 0; i < 3; i++) {
            entityCache.get(modelId1, detector);
        }
        for (int i = 0; i < 3; i++) {
            entityCache.get(modelId2, detector);
        }

        entityCache.hostIfPossible(detector, modelState1);
        entityCache.hostIfPossible(detector, modelState2);

        assertEquals(2, entityCache.getTotalActiveEntities());
        assertEquals(2, entityCache.getAllModels().size());
        when(clock.instant()).thenReturn(Instant.now());
        entityCache.maintenance();
        assertEquals(0, entityCache.getTotalActiveEntities());
        assertEquals(0, entityCache.getAllModels().size());

        for (int i = 0; i < 2; i++) {
            // doorkeeper should have been reset
            assertEquals(null, entityCache.get(modelId2, detector));
        }
    }

    public void testClear() {
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);

        for (int i = 0; i < 3; i++) {
            // make modelId1 have higher priority
            entityCache.get(modelId1, detector);
        }

        for (int i = 0; i < 2; i++) {
            entityCache.get(modelId2, detector);
        }

        entityCache.hostIfPossible(detector, modelState1);
        entityCache.hostIfPossible(detector, modelState2);

        assertEquals(2, entityCache.getTotalActiveEntities());
        assertTrue(entityCache.isActive(detectorId, modelId1));
        assertEquals(0, entityCache.getTotalUpdates(detectorId));
        modelState1.getModel().addSample(point);
        assertEquals(1, entityCache.getTotalUpdates(detectorId));
        assertEquals(1, entityCache.getTotalUpdates(detectorId, modelId1));
        entityCache.clear(detectorId);
        assertEquals(0, entityCache.getTotalActiveEntities());

        for (int i = 0; i < 2; i++) {
            // doorkeeper should have been reset
            assertEquals(null, entityCache.get(modelId2, detector));
        }
    }

    class CleanRunnable implements Runnable {
        @Override
        public void run() {
            entityCache.maintenance();
        }
    }

    private void setUpConcurrentMaintenance() {
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        for (int i = 0; i < 2; i++) {
            entityCache.get(modelId1, detector);
        }
        for (int i = 0; i < 2; i++) {
            entityCache.get(modelId2, detector);
        }
        for (int i = 0; i < 2; i++) {
            entityCache.get(modelId3, detector);
        }

        entityCache.hostIfPossible(detector, modelState1);
        entityCache.hostIfPossible(detector, modelState2);
        entityCache.hostIfPossible(detector, modelState3);

        when(memoryTracker.memoryToShed()).thenReturn(memoryPerEntity);
        assertEquals(3, entityCache.getTotalActiveEntities());
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

        entityCache.maintenance();

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
                entityCache.maintenance();
            } catch (Exception e) {
                // maintenance can throw AnomalyDetectionException, catch it here
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

            entityCache.maintenance();
        } catch (AnomalyDetectionException e) {
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

    private void selectTestCommon(int entityFreq) {
        for (int i = 0; i < entityFreq; i++) {
            // bypass doorkeeper
            entityCache.get(entity1.getModelId(detectorId).get(), detector);
        }
        Collection<Entity> cacheMissEntities = new ArrayList<>();
        cacheMissEntities.add(entity1);
        Pair<List<Entity>, List<Entity>> selectedAndOther = entityCache.selectUpdateCandidate(cacheMissEntities, detectorId, detector);
        List<Entity> selected = selectedAndOther.getLeft();
        assertEquals(1, selected.size());
        assertEquals(entity1, selected.get(0));
        assertEquals(0, selectedAndOther.getRight().size());
    }

    public void testSelectToDedicatedCache() {
        selectTestCommon(2);
    }

    public void testSelectToSharedCache() {
        for (int i = 0; i < 2; i++) {
            // bypass doorkeeper
            entityCache.get(entity2.getModelId(detectorId).get(), detector);
        }
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);

        // fill in dedicated cache
        entityCache.hostIfPossible(detector, modelState2);
        selectTestCommon(2);
        verify(memoryTracker, times(1)).canAllocate(anyLong());
    }

    public void testSelectToReplaceInCache() {
        for (int i = 0; i < 2; i++) {
            // bypass doorkeeper
            entityCache.get(entity2.getModelId(detectorId).get(), detector);
        }
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);

        // fill in dedicated cache
        entityCache.hostIfPossible(detector, modelState2);
        // make entity1 have enough priority to replace entity2
        selectTestCommon(10);
        verify(memoryTracker, times(1)).canAllocate(anyLong());
    }

    private void replaceInOtherCacheSetUp() {
        Entity entity5 = Entity.createSingleAttributeEntity("attributeName1", "attributeVal5");
        Entity entity6 = Entity.createSingleAttributeEntity("attributeName1", "attributeVal6");
        ModelState<EntityModel> modelState5 = new ModelState<>(
            new EntityModel(entity5, new ArrayDeque<>(), null),
            entity5.getModelId(detectorId2).get(),
            detectorId2,
            ModelType.ENTITY.getName(),
            clock,
            0
        );
        ModelState<EntityModel> modelState6 = new ModelState<>(
            new EntityModel(entity6, new ArrayDeque<>(), null),
            entity6.getModelId(detectorId2).get(),
            detectorId2,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        for (int i = 0; i < 3; i++) {
            // bypass doorkeeper and leave room for lower frequency entity in testSelectToCold
            entityCache.get(entity5.getModelId(detectorId2).get(), detector2);
            entityCache.get(entity6.getModelId(detectorId2).get(), detector2);
        }
        for (int i = 0; i < 10; i++) {
            // entity1 cannot replace entity2 due to frequency
            entityCache.get(entity2.getModelId(detectorId).get(), detector);
        }
        // put modelState5 in dedicated and modelState6 in shared cache
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        entityCache.hostIfPossible(detector2, modelState5);
        entityCache.hostIfPossible(detector2, modelState6);

        // fill in dedicated cache
        entityCache.hostIfPossible(detector, modelState2);

        // don't allow to use shared cache afterwards
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);
    }

    public void testSelectToReplaceInOtherCache() {
        replaceInOtherCacheSetUp();

        // make entity1 have enough priority to replace entity2
        selectTestCommon(10);
        // once when deciding whether to host modelState6;
        // once when calling selectUpdateCandidate on entity1
        verify(memoryTracker, times(2)).canAllocate(anyLong());
    }

    public void testSelectToCold() {
        replaceInOtherCacheSetUp();

        for (int i = 0; i < 2; i++) {
            // bypass doorkeeper
            entityCache.get(entity1.getModelId(detectorId).get(), detector);
        }
        Collection<Entity> cacheMissEntities = new ArrayList<>();
        cacheMissEntities.add(entity1);
        Pair<List<Entity>, List<Entity>> selectedAndOther = entityCache.selectUpdateCandidate(cacheMissEntities, detectorId, detector);
        List<Entity> cold = selectedAndOther.getRight();
        assertEquals(1, cold.size());
        assertEquals(entity1, cold.get(0));
        assertEquals(0, selectedAndOther.getLeft().size());
    }

    /*
     * Test the scenario:
     * 1. A detector's buffer uses dedicated and shared memory
     * 2. a new detector's buffer is created and triggers clearMemory (every new
     *  CacheBuffer creation will trigger it)
     * 3. clearMemory found we can reclaim shared memory
     */
    public void testClearMemory() {
        for (int i = 0; i < 2; i++) {
            // bypass doorkeeper
            entityCache.get(entity2.getModelId(detectorId).get(), detector);
        }

        for (int i = 0; i < 10; i++) {
            // bypass doorkeeper and make entity1 have higher frequency
            entityCache.get(entity1.getModelId(detectorId).get(), detector);
        }

        // put modelState5 in dedicated and modelState6 in shared cache
        when(memoryTracker.canAllocate(anyLong())).thenReturn(true);
        entityCache.hostIfPossible(detector, modelState1);
        entityCache.hostIfPossible(detector, modelState2);

        // two entities get inserted to cache
        assertTrue(null != entityCache.get(entity1.getModelId(detectorId).get(), detector));
        assertTrue(null != entityCache.get(entity2.getModelId(detectorId).get(), detector));

        Entity entity5 = Entity.createSingleAttributeEntity("attributeName1", "attributeVal5");
        when(memoryTracker.memoryToShed()).thenReturn(memoryPerEntity);
        for (int i = 0; i < 2; i++) {
            // bypass doorkeeper, CacheBuffer created, and trigger clearMemory
            entityCache.get(entity5.getModelId(detectorId2).get(), detector2);
        }

        assertTrue(null != entityCache.get(entity1.getModelId(detectorId).get(), detector));
        // entity 2 removed
        assertTrue(null == entityCache.get(entity2.getModelId(detectorId).get(), detector));
        assertTrue(null == entityCache.get(entity5.getModelId(detectorId2).get(), detector));
    }

    public void testSelectEmpty() {
        Collection<Entity> cacheMissEntities = new ArrayList<>();
        cacheMissEntities.add(entity1);
        Pair<List<Entity>, List<Entity>> selectedAndOther = entityCache.selectUpdateCandidate(cacheMissEntities, detectorId, detector);
        assertEquals(0, selectedAndOther.getLeft().size());
        assertEquals(0, selectedAndOther.getRight().size());
    }

    // test that detector interval is more than 1 hour that maintenance is called before
    // the next get method
    public void testLongDetectorInterval() {
        try {
            EnabledSetting.getInstance().setSettingValue(EnabledSetting.DOOR_KEEPER_IN_CACHE_ENABLED, true);
            when(clock.instant()).thenReturn(Instant.ofEpochSecond(1000));
            when(detector.getDetectionIntervalDuration()).thenReturn(Duration.ofHours(12));
            String modelId = entity1.getModelId(detectorId).get();
            // record last access time 1000
            assertTrue(null == entityCache.get(modelId, detector));
            assertEquals(-1, entityCache.getLastActiveMs(detectorId, modelId));
            // 2 hour = 7200 seconds have passed
            long currentTimeEpoch = 8200;
            when(clock.instant()).thenReturn(Instant.ofEpochSecond(currentTimeEpoch));
            // door keeper should not be expired since we reclaim space every 60 intervals
            entityCache.maintenance();
            // door keeper still has the record and won't blocks entity state being created
            entityCache.get(modelId, detector);
            // * 1000 to convert to milliseconds
            assertEquals(currentTimeEpoch * 1000, entityCache.getLastActiveMs(detectorId, modelId));
        } finally {
            EnabledSetting.getInstance().setSettingValue(EnabledSetting.DOOR_KEEPER_IN_CACHE_ENABLED, false);
        }
    }

    public void testGetNoPriorityUpdate() {
        for (int i = 0; i < 3; i++) {
            // bypass doorkeeper
            entityCache.get(entity2.getModelId(detectorId).get(), detector);
        }

        // fill in dedicated cache
        entityCache.hostIfPossible(detector, modelState2);

        // don't allow to use shared cache afterwards
        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);

        for (int i = 0; i < 2; i++) {
            // bypass doorkeeper
            entityCache.get(entity1.getModelId(detectorId).get(), detector);
        }
        for (int i = 0; i < 10; i++) {
            // won't increase frequency
            entityCache.getForMaintainance(detectorId, entity1.getModelId(detectorId).get());
        }

        entityCache.hostIfPossible(detector, modelState1);

        // entity1 does not replace entity2
        assertTrue(null == entityCache.get(entity1.getModelId(detectorId).get(), detector));
        assertTrue(null != entityCache.get(entity2.getModelId(detectorId).get(), detector));

        for (int i = 0; i < 10; i++) {
            // increase frequency
            entityCache.get(entity1.getModelId(detectorId).get(), detector);
        }

        entityCache.hostIfPossible(detector, modelState1);

        // entity1 replace entity2
        assertTrue(null != entityCache.get(entity1.getModelId(detectorId).get(), detector));
        assertTrue(null == entityCache.get(entity2.getModelId(detectorId).get(), detector));
    }

    public void testRemoveEntityModel() {
        for (int i = 0; i < 3; i++) {
            // bypass doorkeeper
            entityCache.get(entity2.getModelId(detectorId).get(), detector);
        }

        // fill in dedicated cache
        entityCache.hostIfPossible(detector, modelState2);

        assertTrue(null != entityCache.get(entity2.getModelId(detectorId).get(), detector));

        entityCache.removeEntityModel(detectorId, entity2.getModelId(detectorId).get());

        assertTrue(null == entityCache.get(entity2.getModelId(detectorId).get(), detector));

        verify(checkpoint, times(1)).deleteModelCheckpoint(eq(entity2.getModelId(detectorId).get()), any());
        verify(checkpointWriteQueue, never()).write(any(), anyBoolean(), any());
    }
}
