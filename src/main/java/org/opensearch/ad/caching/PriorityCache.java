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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.COOLDOWN_MINUTES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_CACHE_MISS_HANDLING_PER_SECOND;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.TransportActions;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.MemoryTracker.Origin;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelManager.ModelType;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.threadpool.ThreadPool;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.RateLimiter;

public class PriorityCache implements EntityCache {
    private final Logger LOG = LogManager.getLogger(PriorityCache.class);

    // detector id -> CacheBuffer, weight based
    private final Map<String, CacheBuffer> activeEnities;
    private final CheckpointDao checkpointDao;
    private final int dedicatedCacheSize;
    // LRU Cache
    private Cache<String, ModelState<EntityModel>> inActiveEntities;
    private final MemoryTracker memoryTracker;
    private final ModelManager modelManager;
    private final ReentrantLock maintenanceLock;
    private final int numberOfTrees;
    private final Clock clock;
    private final Duration modelTtl;
    private final int numMinSamples;
    private Map<String, DoorKeeper> doorKeepers;
    private Instant cooldownStart;
    private int coolDownMinutes;
    private ThreadPool threadPool;
    private Random random;
    private RateLimiter cacheMissHandlingLimiter;

    public PriorityCache(
        CheckpointDao checkpointDao,
        int dedicatedCacheSize,
        Duration inactiveEntityTtl,
        int maxInactiveStates,
        MemoryTracker memoryTracker,
        ModelManager modelManager,
        int numberOfTrees,
        Clock clock,
        ClusterService clusterService,
        Duration modelTtl,
        int numMinSamples,
        Settings settings,
        ThreadPool threadPool,
        int cacheMissRateHandlingLimiter
    ) {
        this.checkpointDao = checkpointDao;
        this.dedicatedCacheSize = dedicatedCacheSize;
        this.activeEnities = new ConcurrentHashMap<>();
        this.memoryTracker = memoryTracker;
        this.modelManager = modelManager;
        this.maintenanceLock = new ReentrantLock();
        this.numberOfTrees = numberOfTrees;
        this.clock = clock;
        this.modelTtl = modelTtl;
        this.numMinSamples = numMinSamples;
        this.doorKeepers = new ConcurrentHashMap<>();

        this.inActiveEntities = CacheBuilder
            .newBuilder()
            .expireAfterAccess(inactiveEntityTtl.toHours(), TimeUnit.HOURS)
            .maximumSize(maxInactiveStates)
            .concurrencyLevel(1)
            .build();

        this.cooldownStart = Instant.MIN;
        this.coolDownMinutes = (int) (COOLDOWN_MINUTES.get(settings).getMinutes());
        this.threadPool = threadPool;
        this.random = new Random(42);

        this.cacheMissHandlingLimiter = RateLimiter.create(cacheMissRateHandlingLimiter);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_CACHE_MISS_HANDLING_PER_SECOND, it -> this.cacheMissHandlingLimiter = RateLimiter.create(it));
    }

    @Override
    public ModelState<EntityModel> get(String modelId, AnomalyDetector detector, double[] datapoint, String entityName) {
        String detectorId = detector.getDetectorId();
        CacheBuffer buffer = computeBufferIfAbsent(detector, detectorId);
        ModelState<EntityModel> modelState = buffer.get(modelId);

        // during maintenance period, stop putting new entries
        if (modelState == null) {
            DoorKeeper doorKeeper = doorKeepers
                .computeIfAbsent(
                    detectorId,
                    id -> {
                        // reset every 60 intervals
                        return new DoorKeeper(
                            AnomalyDetectorSettings.DOOR_KEEPER_MAX_INSERTION,
                            AnomalyDetectorSettings.DOOR_KEEPER_FAULSE_POSITIVE_RATE,
                            detector.getDetectionIntervalDuration().multipliedBy(60),
                            clock
                        );
                    }
                );

            // first hit, ignore
            if (doorKeeper.mightContain(modelId) == false) {
                doorKeeper.put(modelId);
                return null;
            }

            ModelState<EntityModel> state = inActiveEntities.getIfPresent(modelId);

            // compute updated priority
            // We don’t want to admit the latest entity for correctness by throwing out a
            // hot entity. We have a priority (time-decayed count) sensitive to
            // the number of hits, length of time, and sampling interval. Examples:
            // 1) an entity from a 5-minute interval detector that is hit 5 times in the
            // past 25 minutes should have an equal chance of using the cache along with
            // an entity from a 1-minute interval detector that is hit 5 times in the past
            // 5 minutes.
            // 2) It might be the case that the frequency of entities changes dynamically
            // during run-time. For example, entity A showed up for the first 500 times,
            // but entity B showed up for the next 500 times. Our priority should give
            // entity B higher priority than entity A as time goes by.
            // 3) Entity A will have a higher priority than entity B if A runs
            // for a longer time given other things are equal.
            //
            // We ensure fairness by using periods instead of absolute duration. Entity A
            // accessed once three intervals ago should have the same priority with entity B
            // accessed once three periods ago, though they belong to detectors of different
            // intervals.
            float priority = 0;
            if (state != null) {
                priority = state.getPriority();
            }
            priority = buffer.getPriorityTracker().getUpdatedPriority(priority);

            // update state using new priority or create a new one
            if (state != null) {
                state.setPriority(priority);
            } else {
                EntityModel model = new EntityModel(modelId, new ArrayDeque<>(), null, null);
                state = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
            }

            if (random.nextInt(10_000) == 1) {
                // clear up memory with 1/10000 probability since this operation is costly, but we have to do it from time to time.
                // e.g., we need to adjust shared entity memory size if some reserved memory gets allocated. Use 10_000 since our
                // query limit is 1k by default and we can have 10 detectors: 10 * 1k. We also do this in hourly maintenance window no
                // matter what.
                tryClearUpMemory();
            }
            if (!maintenanceLock.isLocked()
                && cacheMissHandlingLimiter.tryAcquire()
                && hostIfPossible(buffer, detectorId, modelId, entityName, detector, state, priority)) {
                addSample(state, datapoint);
                inActiveEntities.invalidate(modelId);
            } else {
                // put to inactive cache if we cannot host or get the lock or get rate permits
                // only keep weights in inactive cache to keep it small.
                // It can be dangerous to exceed a few dozen MBs, especially
                // in small heap machine like t2.
                inActiveEntities.put(modelId, state);
            }
        }

        return modelState;
    }

    /**
     * Whether host an entity is possible
     * @param buffer the destination buffer for the given entity
     * @param detectorId Detector Id
     * @param modelId Model Id
     * @param entityName Entity's name
     * @param detector Detector Config
     * @param state State to host
     * @param priority The entity's priority
     * @return true if possible; false otherwise
     */
    private boolean hostIfPossible(
        CacheBuffer buffer,
        String detectorId,
        String modelId,
        String entityName,
        AnomalyDetector detector,
        ModelState<EntityModel> state,
        float priority
    ) {
        // current buffer's dedicated cache has free slots
        // thread safe as each detector has one thread at one time and only the
        // thread can access its buffer.
        if (buffer.dedicatedCacheAvailable()) {
            buffer.put(modelId, state);
        } else if (memoryTracker.canAllocate(buffer.getMemoryConsumptionPerEntity())) {
            // can allocate in shared cache
            // race conditions can happen when multiple threads evaluating this condition.
            // This is a problem as our AD memory usage is close to full and we put
            // more things than we planned. One model in multi-entity case is small,
            // it is fine we exceed a little. We have regular maintenance to remove
            // extra memory usage.
            buffer.put(modelId, state);
        } else if (buffer.canReplaceWithinDetector(priority)) {
            // can replace an entity in the same CacheBuffer living in reserved
            // or shared cache
            // thread safe as each detector has one thread at one time and only the
            // thread can access its buffer.
            ModelState<EntityModel> removed = buffer.replace(modelId, state);
            if (removed != null) {
                // set last used time for profile API so that we know when an entities is evicted
                removed.setLastUsedTime(clock.instant());
                inActiveEntities.put(removed.getModelId(), removed);
            }
        } else {
            // If two threads try to remove the same entity and add their own state, the 2nd remove
            // returns null and only the first one succeeds.
            float scaledPriority = buffer.getPriorityTracker().getScaledPriority(priority);
            Entry<CacheBuffer, String> bufferToRemoveEntity = canReplaceInSharedCache(buffer, scaledPriority);
            CacheBuffer bufferToRemove = bufferToRemoveEntity.getKey();
            String entityModelId = bufferToRemoveEntity.getValue();
            ModelState<EntityModel> removed = null;
            if (bufferToRemove != null && ((removed = bufferToRemove.remove(entityModelId)) != null)) {
                buffer.put(modelId, state);
                // set last used time for profile API so that we know when an entities is evicted
                removed.setLastUsedTime(clock.instant());
                inActiveEntities.put(removed.getModelId(), removed);
            } else {
                return false;
            }
        }

        maybeRestoreOrTrainModel(modelId, entityName, state);
        return true;
    }

    private void addSample(ModelState<EntityModel> stateToPromote, double[] datapoint) {
        // add samples
        Queue<double[]> samples = stateToPromote.getModel().getSamples();
        samples.add(datapoint);
        // only keep the recent numMinSamples
        while (samples.size() > this.numMinSamples) {
            samples.remove();
        }
    }

    private void maybeRestoreOrTrainModel(String modelId, String entityName, ModelState<EntityModel> state) {
        EntityModel entityModel = state.getModel();
        // rate limit in case of OpenSearchRejectedExecutionException from get threadpool whose queue capacity is 1k
        if (entityModel != null
            && (entityModel.getRcf() == null || entityModel.getThreshold() == null)
            && cooldownStart.plus(Duration.ofMinutes(coolDownMinutes)).isBefore(clock.instant())) {
            checkpointDao
                .restoreModelCheckpoint(
                    modelId,
                    ActionListener
                        .wrap(checkpoint -> modelManager.processEntityCheckpoint(checkpoint, modelId, entityName, state), exception -> {
                            Throwable cause = Throwables.getRootCause(exception);
                            if (cause instanceof IndexNotFoundException) {
                                modelManager.processEntityCheckpoint(Optional.empty(), modelId, entityName, state);
                            } else if (cause instanceof RejectedExecutionException
                                || TransportActions.isShardNotAvailableException(cause)) {
                                LOG.error("too many get AD model checkpoint requests or shard not avialble");
                                cooldownStart = clock.instant();
                            } else {
                                LOG.error("Fail to restore models for " + modelId, exception);
                            }
                        })
                );
        }
    }

    private CacheBuffer computeBufferIfAbsent(AnomalyDetector detector, String detectorId) {
        return activeEnities.computeIfAbsent(detectorId, k -> {
            long requiredBytes = getReservedDetectorMemory(detector);
            tryClearUpMemory();
            if (memoryTracker.canAllocateReserved(detectorId, requiredBytes)) {
                memoryTracker.consumeMemory(requiredBytes, true, Origin.MULTI_ENTITY_DETECTOR);
                long intervalSecs = detector.getDetectorIntervalInSeconds();
                return new CacheBuffer(
                    dedicatedCacheSize,
                    intervalSecs,
                    checkpointDao,
                    memoryTracker.estimateModelSize(detector, numberOfTrees),
                    memoryTracker,
                    clock,
                    modelTtl,
                    detectorId
                );
            }
            // if hosting not allowed, exception will be thrown by isHostingAllowed
            throw new LimitExceededException(detectorId, CommonErrorMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG);
        });
    }

    private long getReservedDetectorMemory(AnomalyDetector detector) {
        return dedicatedCacheSize * memoryTracker.estimateModelSize(detector, numberOfTrees);
    }

    /**
     * Whether the candidate entity can replace any entity in the shared cache.
     * We can have race conditions when multiple threads try to evaluate this
     * function. The result is that we can have multiple threads thinks they
     * can replace entities in the cache.
     *
     *
     * @param originBuffer the CacheBuffer that the entity belongs to (with the same detector Id)
     * @param candidatePriority the candidate entity's priority
     * @return the CacheBuffer if we can find a CacheBuffer to make room for the candidate entity
     */
    private Entry<CacheBuffer, String> canReplaceInSharedCache(CacheBuffer originBuffer, float candidatePriority) {
        CacheBuffer minPriorityBuffer = null;
        float minPriority = Float.MAX_VALUE;
        String minPriorityEntityModelId = null;
        for (Map.Entry<String, CacheBuffer> entry : activeEnities.entrySet()) {
            CacheBuffer buffer = entry.getValue();
            if (buffer != originBuffer && buffer.canRemove()) {
                Entry<String, Float> priorityEntry = buffer.getPriorityTracker().getMinimumScaledPriority();
                float priority = priorityEntry.getValue();
                if (candidatePriority > priority && priority < minPriority) {
                    minPriority = priority;
                    minPriorityBuffer = buffer;
                    minPriorityEntityModelId = priorityEntry.getKey();
                }
            }
        }
        return new SimpleImmutableEntry<>(minPriorityBuffer, minPriorityEntityModelId);
    }

    private void tryClearUpMemory() {
        try {
            if (maintenanceLock.tryLock()) {
                clearMemory();
            } else {
                threadPool.schedule(() -> {
                    try {
                        tryClearUpMemory();
                    } catch (Exception e) {
                        LOG.error("Fail to clear up memory taken by CacheBuffer.  Will retry during maintenance.");
                    }
                }, new TimeValue(random.nextInt(90), TimeUnit.SECONDS), AnomalyDetectorPlugin.AD_THREAD_POOL_NAME);
            }
        } finally {
            if (maintenanceLock.isHeldByCurrentThread()) {
                maintenanceLock.unlock();
            }
        }
    }

    private void clearMemory() {
        recalculateUsedMemory();
        long memoryToShed = memoryTracker.memoryToShed();
        float minPriority = Float.MAX_VALUE;
        CacheBuffer minPriorityBuffer = null;
        String minPriorityEntityModelId = null;
        while (memoryToShed > 0) {
            for (Map.Entry<String, CacheBuffer> entry : activeEnities.entrySet()) {
                CacheBuffer buffer = entry.getValue();
                Entry<String, Float> priorityEntry = buffer.getPriorityTracker().getMinimumScaledPriority();
                float priority = priorityEntry.getValue();
                if (buffer.canRemove() && priority < minPriority) {
                    minPriority = priority;
                    minPriorityBuffer = buffer;
                    minPriorityEntityModelId = priorityEntry.getKey();
                }
            }
            if (minPriorityBuffer != null) {
                minPriorityBuffer.remove(minPriorityEntityModelId);
                long memoryReleased = minPriorityBuffer.getMemoryConsumptionPerEntity();
                memoryTracker.releaseMemory(memoryReleased, false, Origin.MULTI_ENTITY_DETECTOR);
                memoryToShed -= memoryReleased;
            } else {
                break;
            }
        }
    }

    /**
     * Recalculate memory consumption in case of bugs/race conditions when allocating/releasing memory
     */
    private void recalculateUsedMemory() {
        long reserved = 0;
        long shared = 0;
        for (Map.Entry<String, CacheBuffer> entry : activeEnities.entrySet()) {
            CacheBuffer buffer = entry.getValue();
            reserved += buffer.getReservedBytes();
            shared += buffer.getBytesInSharedCache();
        }
        memoryTracker.syncMemoryState(Origin.MULTI_ENTITY_DETECTOR, reserved + shared, reserved);
    }

    /**
     * Maintain active entity's cache and door keepers.
     *
     * inActiveEntities is a Guava's LRU cache. The data structure itself is
     * gonna evict items if they are inactive for 3 days or its maximum size
     * reached (1 million entries)
     */
    @Override
    public void maintenance() {
        try {
            // clean up memory if we allocate more memory than we should
            tryClearUpMemory();
            activeEnities.entrySet().stream().forEach(cacheBufferEntry -> {
                String detectorId = cacheBufferEntry.getKey();
                CacheBuffer cacheBuffer = cacheBufferEntry.getValue();
                // remove expired cache buffer
                if (cacheBuffer.expired(modelTtl)) {
                    activeEnities.remove(detectorId);
                    cacheBuffer.clear();
                } else {
                    cacheBuffer.maintenance();
                }
            });
            checkpointDao.flush();
            doorKeepers.entrySet().stream().forEach(doorKeeperEntry -> {
                String detectorId = doorKeeperEntry.getKey();
                DoorKeeper doorKeeper = doorKeeperEntry.getValue();
                if (doorKeeper.expired(modelTtl)) {
                    doorKeepers.remove(detectorId);
                } else {
                    doorKeeper.maintenance();
                }
            });
        } catch (Exception e) {
            // will be thrown to ES's transport broadcast handler
            throw new OpenSearchException("Fail to maintain cache", e);
        }

    }

    /**
     * Permanently deletes models hosted in memory and persisted in index.
     *
     * @param detectorId id the of the detector for which models are to be permanently deleted
     */
    @Override
    public void clear(String detectorId) {
        if (detectorId == null) {
            return;
        }
        CacheBuffer buffer = activeEnities.remove(detectorId);
        if (buffer != null) {
            buffer.clear();
        }
        checkpointDao.deleteModelCheckpointByDetectorId(detectorId);
        doorKeepers.remove(detectorId);
    }

    /**
     * Get the number of active entities of a detector
     * @param detectorId Detector Id
     * @return The number of active entities
     */
    @Override
    public int getActiveEntities(String detectorId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            return cacheBuffer.getActiveEntities();
        }
        return 0;
    }

    /**
     * Whether an entity is active or not
     * @param detectorId The Id of the detector that an entity belongs to
     * @param entityModelId Entity's Model Id
     * @return Whether an entity is active or not
     */
    @Override
    public boolean isActive(String detectorId, String entityModelId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            return cacheBuffer.isActive(entityModelId);
        }
        return false;
    }

    @Override
    public long getTotalUpdates(String detectorId) {
        return Optional
            .of(activeEnities)
            .map(entities -> entities.get(detectorId))
            .map(buffer -> buffer.getPriorityTracker().getHighestPriorityEntityId())
            .map(entityModelIdOptional -> entityModelIdOptional.get())
            .map(entityModelId -> getTotalUpdates(detectorId, entityModelId))
            .orElse(0L);
    }

    @Override
    public long getTotalUpdates(String detectorId, String entityModelId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            Optional<EntityModel> modelOptional = cacheBuffer.getModel(entityModelId);
            // TODO: make it work for shingles. samples.size() is not the real shingle
            long accumulatedShingles = modelOptional
                .map(model -> model.getRcf())
                .map(rcf -> rcf.getTotalUpdates())
                .orElseGet(
                    () -> modelOptional.map(model -> model.getSamples()).map(samples -> samples.size()).map(Long::valueOf).orElse(0L)
                );
            return accumulatedShingles;
        }
        return 0L;
    }

    /**
     *
     * @return total active entities in the cache
     */
    @Override
    public int getTotalActiveEntities() {
        AtomicInteger total = new AtomicInteger();
        activeEnities.values().stream().forEach(cacheBuffer -> { total.addAndGet(cacheBuffer.getActiveEntities()); });
        return total.get();
    }

    /**
     * Gets modelStates of all model hosted on a node
     *
     * @return list of modelStates
     */
    @Override
    public List<ModelState<?>> getAllModels() {
        List<ModelState<?>> states = new ArrayList<>();
        activeEnities.values().stream().forEach(cacheBuffer -> states.addAll(cacheBuffer.getAllModels()));
        return states;
    }

    /**
     * Gets all of a detector's model sizes hosted on a node
     *
     * @return a map of model id to its memory size
     */
    @Override
    public Map<String, Long> getModelSize(String detectorId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        Map<String, Long> res = new HashMap<>();
        if (cacheBuffer != null) {
            long size = cacheBuffer.getMemoryConsumptionPerEntity();
            cacheBuffer.getAllModels().forEach(entry -> res.put(entry.getModelId(), size));
        }
        return res;
    }

    @Override
    /**
     * Gets an entity's model state
     *
     * @param detectorId detector id
     * @param entityModelId  entity model id
     * @return the model state
     */
    public long getModelSize(String detectorId, String entityModelId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null && cacheBuffer.getModel(entityModelId).isPresent()) {
            return cacheBuffer.getMemoryConsumptionPerEntity();
        }
        return -1L;
    }

    /**
     * Return the last active time of an entity's state.
     *
     * If the entity's state is active in the cache, the value indicates when the cache
     * is lastly accessed (get/put).  If the entity's state is inactive in the cache,
     * the value indicates when the cache state is created or when the entity is evicted
     * from active entity cache.
     *
     * @param detectorId The Id of the detector that an entity belongs to
     * @param entityModelId Entity's Model Id
     * @return if the entity is in the cache, return the timestamp in epoch
     * milliseconds when the entity's state is lastly used.  Otherwise, return -1.
     */
    @Override
    public long getLastActiveMs(String detectorId, String entityModelId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            return cacheBuffer.getLastUsedTime(entityModelId);
        }
        ModelState<EntityModel> stateInActive = inActiveEntities.getIfPresent(entityModelId);
        if (stateInActive != null) {
            return stateInActive.getLastUsedTime().getEpochSecond();
        }
        return -1L;
    }
}
