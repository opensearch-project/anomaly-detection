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

package org.opensearch.timeseries.caching;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.ml.CheckpointDao;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.ModelProfile;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainWorker;
import org.opensearch.timeseries.ratelimit.CheckpointWriteWorker;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DateUtils;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public abstract class PriorityCache<RCFModelType extends ThresholdedRandomCutForest, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, CheckpointDaoType extends CheckpointDao<RCFModelType, IndexType, IndexManagementType>, CheckpointWriterType extends CheckpointWriteWorker<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType>, CheckpointMaintainerType extends CheckpointMaintainWorker, CacheBufferType extends CacheBuffer<RCFModelType, IndexType, IndexManagementType, CheckpointDaoType, CheckpointWriterType, CheckpointMaintainerType>>
    implements
        TimeSeriesCache<RCFModelType> {

    private static final Logger LOG = LogManager.getLogger(PriorityCache.class);

    // detector id -> CacheBuffer, weight based
    private final Map<String, CacheBufferType> activeEnities;
    private final CheckpointDaoType checkpointDao;
    protected volatile int hcDedicatedCacheSize;
    // LRU Cache, key is model id
    private Cache<String, ModelState<RCFModelType>> inActiveEntities;
    protected final MemoryTracker memoryTracker;
    private final ReentrantLock maintenanceLock;
    private final int numberOfTrees;
    protected final Clock clock;
    protected final Duration modelTtl;
    // A bloom filter placed in front of inactive entity cache to
    // filter out unpopular items that are not likely to appear more
    // than once. Key is detector id
    private Map<String, DoorKeeper> doorKeepers;
    private ThreadPool threadPool;
    private String threadPoolName;
    private Random random;
    // iterating through all of inactive entities is heavy. We don't want to do
    // it again and again for no obvious benefits.
    private Instant lastInActiveEntityMaintenance;
    protected int maintenanceFreqConstant;
    protected int checkpointIntervalHrs;
    private Origin origin;
    // mapping config id to priority tracker.
    // Used to track entity priorities
    private Map<String, PriorityTracker> priorityTrackerMap;

    public PriorityCache(
        CheckpointDaoType checkpointDao,
        int hcDedicatedCacheSize,
        Setting<TimeValue> checkpointTtl,
        int maxInactiveStates,
        MemoryTracker memoryTracker,
        int numberOfTrees,
        Clock clock,
        ClusterService clusterService,
        Duration modelTtl,
        ThreadPool threadPool,
        String threadPoolName,
        int maintenanceFreqConstant,
        Settings settings,
        Setting<TimeValue> checkpointSavingFreq,
        Origin origin,
        Setting<Integer> dedicatedCacheSizeSetting,
        Setting<Double> modelMaxSizePercent
    ) {
        this.checkpointDao = checkpointDao;

        this.activeEnities = new ConcurrentHashMap<>();
        this.hcDedicatedCacheSize = hcDedicatedCacheSize;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(dedicatedCacheSizeSetting, (it) -> {
            this.hcDedicatedCacheSize = it;
            this.setHCDedicatedCacheSizeListener();
            this.tryClearUpMemory();
        }, this::validateDedicatedCacheSize);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(modelMaxSizePercent, it -> this.tryClearUpMemory());

        this.memoryTracker = memoryTracker;
        this.maintenanceLock = new ReentrantLock();
        this.numberOfTrees = numberOfTrees;
        this.clock = clock;
        this.modelTtl = modelTtl;
        this.doorKeepers = new ConcurrentHashMap<>();

        Duration inactiveEntityTtl = DateUtils.toDuration(checkpointTtl.get(settings));

        this.inActiveEntities = createInactiveCache(inactiveEntityTtl, maxInactiveStates);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(checkpointTtl, it -> {
            this.inActiveEntities = createInactiveCache(DateUtils.toDuration(it), maxInactiveStates);
        });

        this.threadPool = threadPool;
        this.threadPoolName = threadPoolName;
        this.random = new Random(42);
        this.lastInActiveEntityMaintenance = Instant.MIN;
        this.maintenanceFreqConstant = maintenanceFreqConstant;

        this.checkpointIntervalHrs = DateUtils.toDuration(checkpointSavingFreq.get(settings)).toHoursPart();
        clusterService.getClusterSettings().addSettingsUpdateConsumer(checkpointSavingFreq, it -> {
            this.checkpointIntervalHrs = DateUtils.toDuration(it).toHoursPart();
            this.setCheckpointFreqListener();
        });
        this.origin = origin;
        this.priorityTrackerMap = new ConcurrentHashMap<>();
    }

    @Override
    public ModelState<RCFModelType> get(String modelId, Config config) {
        String configId = config.getId();
        CacheBufferType buffer = activeEnities.get(configId);
        ModelState<RCFModelType> modelState = null;
        if (buffer != null) {
            modelState = buffer.get(modelId);
        }

        // during maintenance period, stop putting new entries
        if (!maintenanceLock.isLocked() && modelState == null) {
            if (isDoorKeeperInCacheEnabled()) {
                DoorKeeper doorKeeper = doorKeepers.computeIfAbsent(configId, id -> {
                    // reset every 60 intervals
                    return new DoorKeeper(
                        TimeSeriesSettings.DOOR_KEEPER_FOR_CACHE_MAX_INSERTION,
                        config.getIntervalDuration().multipliedBy(TimeSeriesSettings.DOOR_KEEPER_MAINTENANCE_FREQ),
                        clock,
                        TimeSeriesSettings.CACHE_DOOR_KEEPER_COUNT_THRESHOLD
                    );
                });

                // first few hits, ignore
                // since door keeper may get reset during maintenance, it is possible
                // the entity is still active even though door keeper has no record of
                // this model Id. We have to call isActive method to make sure. Otherwise,
                // the entity might miss a result every 60 intervals due to door keeper
                // reset.
                if (!doorKeeper.appearsMoreThanOrEqualToThreshold(modelId) && !isActive(configId, modelId)) {
                    doorKeeper.put(modelId);
                    return null;
                }
            }

            try {
                ModelState<RCFModelType> state = inActiveEntities.get(modelId, new Callable<ModelState<RCFModelType>>() {
                    @Override
                    public ModelState<RCFModelType> call() {
                        return createEmptyModelState(modelId, configId);
                    }

                });

                // make sure no model has been stored due to previous race conditions
                state.setModel(null);

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

                // update state using new priority or create a new one
                PriorityTracker tracker = priorityTrackerMap.computeIfAbsent(configId, id -> {
                    return new PriorityTracker(
                        clock,
                        config.getIntervalInSeconds(),
                        clock.instant().getEpochSecond(),
                        TimeSeriesSettings.MAX_TRACKING_ENTITIES
                    );
                });
                state.setPriority(tracker.getUpdatedPriority(state.getPriority()));

                // adjust shared memory in case we have used dedicated cache memory for other detectors
                if (random.nextInt(maintenanceFreqConstant) == 1) {
                    tryClearUpMemory();
                }
            } catch (Exception e) {
                LOG.error(new ParameterizedMessage("Fail to update priority of [{}]", modelId), e);
            }

        }

        return modelState;
    }

    private Optional<ModelState<RCFModelType>> getStateFromInactiveEntiiyCache(String modelId) {
        if (modelId == null) {
            return Optional.empty();
        }

        // null if not even recorded in inActiveEntities yet because of doorKeeper or first time start config
        return Optional.ofNullable(inActiveEntities.getIfPresent(modelId));
    }

    @Override
    public boolean hostIfPossible(Config config, ModelState<RCFModelType> toUpdate) {
        // Although toUpdate may not have samples or model, we'll continue.
        if (toUpdate == null) {
            return false;
        }
        String modelId = toUpdate.getModelId();
        String configId = toUpdate.getConfigId();

        if (Strings.isEmpty(modelId) || Strings.isEmpty(configId)) {
            return false;
        }

        CacheBufferType buffer = computeBufferIfAbsent(config, configId);

        Optional<ModelState<RCFModelType>> state = getStateFromInactiveEntiiyCache(modelId);
        ModelState<RCFModelType> modelState = null;
        if (state.isPresent()) {
            modelState = state.get();
        } else {
            modelState = createEmptyModelState(modelId, configId);
        }

        float priority = modelState.getPriority();

        toUpdate.setLastUsedTime(clock.instant());
        toUpdate.setPriority(priority);

        // current buffer's dedicated cache has free slots or can allocate in shared cache
        if (buffer.dedicatedCacheAvailable() || memoryTracker.canAllocate(buffer.getMemoryConsumptionPerModel())) {
            // buffer.put will call MemoryTracker.consumeMemory
            buffer.put(modelId, toUpdate);
            return true;
        }

        if (memoryTracker.canAllocate(buffer.getMemoryConsumptionPerModel())) {
            // buffer.put will call MemoryTracker.consumeMemory
            buffer.put(modelId, toUpdate);
            return true;
        }

        // can replace an entity in the same CacheBuffer living in reserved or shared cache
        if (buffer.canReplaceWithinConfig(priority)) {
            ModelState<RCFModelType> removed = buffer.replace(modelId, toUpdate);
            // null in the case of some other threads have emptied the queue at
            // the same time so there is nothing to replace
            if (removed != null) {
                addIntoInactiveCache(removed);
                return true;
            }
        }

        // If two threads try to remove the same entity and add their own state, the 2nd remove
        // returns null and only the first one succeeds.
        float scaledPriority = buffer.getPriorityTracker().getScaledPriority(priority);
        Triple<CacheBufferType, String, Float> bufferToRemoveEntity = canReplaceInSharedCache(buffer, scaledPriority);
        CacheBufferType bufferToRemove = bufferToRemoveEntity.getLeft();
        String entityModelId = bufferToRemoveEntity.getMiddle();
        ModelState<RCFModelType> removed = null;
        if (bufferToRemove != null && ((removed = bufferToRemove.remove(entityModelId)) != null)) {
            buffer.put(modelId, toUpdate);
            addIntoInactiveCache(removed);
            return true;
        }

        return false;
    }

    private void addIntoInactiveCache(ModelState<RCFModelType> removed) {
        if (removed == null) {
            return;
        }
        // set last used time for profile API so that we know when an entities is evicted
        removed.setLastUsedTime(clock.instant());
        removed.setModel(null);
        inActiveEntities.put(removed.getModelId(), removed);
    }

    private void addEntity(List<Entity> destination, Entity entity, String configId) {
        // It's possible our doorkeepr prevented the entity from entering inactive entities cache
        if (entity != null) {
            Optional<String> modelId = entity.getModelId(configId);
            if (modelId.isPresent() && inActiveEntities.getIfPresent(modelId.get()) != null) {
                destination.add(entity);
            }
        }
    }

    @Override
    public Pair<List<Entity>, List<Entity>> selectUpdateCandidate(Collection<Entity> cacheMissEntities, String configId, Config config) {
        List<Entity> hotEntities = new ArrayList<>();
        List<Entity> coldEntities = new ArrayList<>();

        CacheBufferType buffer = activeEnities.get(configId);
        if (buffer == null) {
            // when a config is just started or during run once, there is
            // no cache buffer yet. Make every cache miss entities hot
            return Pair.of(new ArrayList<>(cacheMissEntities), coldEntities);
        }

        Iterator<Entity> cacheMissEntitiesIter = cacheMissEntities.iterator();
        // current buffer's dedicated cache has free slots
        while (cacheMissEntitiesIter.hasNext() && buffer.dedicatedCacheAvailable()) {
            addEntity(hotEntities, cacheMissEntitiesIter.next(), configId);
        }

        while (cacheMissEntitiesIter.hasNext() && memoryTracker.canAllocate(buffer.getMemoryConsumptionPerModel())) {
            // can allocate in shared cache
            // race conditions can happen when multiple threads evaluating this condition.
            // This is a problem as our AD memory usage is close to full and we put
            // more things than we planned. One model in HCAD is small,
            // it is fine we exceed a little. We have regular maintenance to remove
            // extra memory usage.
            addEntity(hotEntities, cacheMissEntitiesIter.next(), configId);
        }

        // check if we can replace anything in dedicated or shared cache
        // have a copy since we need to do the iteration twice: one for
        // dedicated cache and one for shared cache
        List<Entity> otherBufferReplaceCandidates = new ArrayList<>();

        while (cacheMissEntitiesIter.hasNext()) {
            // can replace an entity in the same CacheBuffer living in reserved
            // or shared cache
            // thread safe as each detector has one thread at one time and only the
            // thread can access its buffer.
            Entity entity = cacheMissEntitiesIter.next();
            Optional<String> modelId = entity.getModelId(configId);

            if (false == modelId.isPresent()) {
                continue;
            }

            Optional<ModelState<RCFModelType>> state = getStateFromInactiveEntiiyCache(modelId.get());
            if (false == state.isPresent()) {
                // not even recorded in inActiveEntities yet because of doorKeeper
                continue;
            }

            ModelState<RCFModelType> modelState = state.get();
            float priority = modelState.getPriority();

            if (buffer.canReplaceWithinConfig(priority)) {
                addEntity(hotEntities, entity, configId);
            } else {
                // re-evaluate replacement condition in other buffers
                otherBufferReplaceCandidates.add(entity);
            }
        }

        // record current minimum priority among all detectors to save redundant
        // scanning of all CacheBuffers
        CacheBufferType bufferToRemove = null;
        float minPriority = Float.MIN_VALUE;

        // check if we can replace in other CacheBuffer
        cacheMissEntitiesIter = otherBufferReplaceCandidates.iterator();

        while (cacheMissEntitiesIter.hasNext()) {
            // If two threads try to remove the same entity and add their own state, the 2nd remove
            // returns null and only the first one succeeds.
            Entity entity = cacheMissEntitiesIter.next();
            Optional<String> modelId = entity.getModelId(configId);

            if (false == modelId.isPresent()) {
                continue;
            }

            Optional<ModelState<RCFModelType>> inactiveState = getStateFromInactiveEntiiyCache(modelId.get());
            if (false == inactiveState.isPresent()) {
                // empty state should not stand a chance to replace others
                continue;
            }

            ModelState<RCFModelType> state = inactiveState.get();

            float priority = state.getPriority();
            float scaledPriority = buffer.getPriorityTracker().getScaledPriority(priority);

            if (scaledPriority <= minPriority) {
                // not even larger than the minPriority, we can put this to coldEntities
                addEntity(coldEntities, entity, configId);
                continue;
            }

            // Float.MIN_VALUE means we need to re-iterate through all CacheBuffers
            if (minPriority == Float.MIN_VALUE) {
                Triple<CacheBufferType, String, Float> bufferToRemoveEntity = canReplaceInSharedCache(buffer, scaledPriority);
                bufferToRemove = bufferToRemoveEntity.getLeft();
                minPriority = bufferToRemoveEntity.getRight();
            }

            if (bufferToRemove != null) {
                addEntity(hotEntities, entity, configId);
                // reset minPriority after the replacement so that we need to iterate all CacheBuffer
                // again
                minPriority = Float.MIN_VALUE;
            } else {
                // after trying everything, we can now safely put this to cold entities list
                addEntity(coldEntities, entity, configId);
            }
        }

        return Pair.of(hotEntities, coldEntities);
    }

    public CacheBufferType computeBufferIfAbsent(Config config, String configId) {
        CacheBufferType buffer = activeEnities.get(configId);
        if (buffer == null) {
            long bytesPerEntityModel = getRequiredMemoryPerEntity(config, memoryTracker, numberOfTrees);
            long requiredBytes = bytesPerEntityModel * (config.isHighCardinality() ? hcDedicatedCacheSize : 1);
            if (memoryTracker.canAllocateReserved(requiredBytes)) {
                memoryTracker.consumeMemory(requiredBytes, true, origin);
                buffer = createEmptyCacheBuffer(
                    config,
                    bytesPerEntityModel,
                    priorityTrackerMap
                        .getOrDefault(
                            configId,
                            new PriorityTracker(
                                clock,
                                config.getIntervalInSeconds(),
                                clock.instant().getEpochSecond(),
                                TimeSeriesSettings.MAX_TRACKING_ENTITIES
                            )
                        )
                );
                activeEnities.put(configId, buffer);
                // There can be race conditions between tryClearUpMemory and
                // activeEntities.put above as tryClearUpMemory accesses activeEnities too.
                // Put tryClearUpMemory after consumeMemory to prevent that.
                tryClearUpMemory();
            } else {
                throw new LimitExceededException(configId, CommonMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG);
            }

        }
        return buffer;
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
    private Triple<CacheBufferType, String, Float> canReplaceInSharedCache(CacheBufferType originBuffer, float candidatePriority) {
        CacheBufferType minPriorityBuffer = null;
        float minPriority = candidatePriority;
        String minPriorityEntityModelId = null;
        for (Map.Entry<String, CacheBufferType> entry : activeEnities.entrySet()) {
            CacheBufferType buffer = entry.getValue();
            if (buffer != originBuffer && buffer.canRemove()) {
                Optional<Entry<String, Float>> priorityEntry = buffer.getPriorityTracker().getMinimumScaledPriority();
                if (!priorityEntry.isPresent()) {
                    continue;
                }
                float priority = priorityEntry.get().getValue();
                if (candidatePriority > priority && priority < minPriority) {
                    minPriority = priority;
                    minPriorityBuffer = buffer;
                    minPriorityEntityModelId = priorityEntry.get().getKey();
                }
            }
        }
        return Triple.of(minPriorityBuffer, minPriorityEntityModelId, minPriority);
    }

    /**
     * Clear up overused memory.  Can happen due to race condition or other detectors
     * consumes resources from shared memory.
     * tryClearUpMemory is ran using analysis-specific threadpool because the function is expensive.
     */
    private void tryClearUpMemory() {
        try {
            if (maintenanceLock.tryLock()) {
                threadPool.executor(threadPoolName).execute(() -> clearMemory());
            } else {
                threadPool.schedule(() -> {
                    try {
                        tryClearUpMemory();
                    } catch (Exception e) {
                        LOG.error("Fail to clear up memory taken by CacheBuffer.  Will retry during maintenance.");
                    }
                }, new TimeValue(random.nextInt(90), TimeUnit.SECONDS), threadPoolName);
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
        PriorityQueue<Triple<Float, CacheBufferType, String>> removalCandiates = null;
        if (memoryToShed > 0) {
            // sort the triple in an ascending order of priority
            removalCandiates = new PriorityQueue<>((x, y) -> Float.compare(x.getLeft(), y.getLeft()));
            for (Map.Entry<String, CacheBufferType> entry : activeEnities.entrySet()) {
                CacheBufferType buffer = entry.getValue();
                Optional<Entry<String, Float>> priorityEntry = buffer.getPriorityTracker().getMinimumScaledPriority();
                if (!priorityEntry.isPresent()) {
                    continue;
                }
                float priority = priorityEntry.get().getValue();
                if (buffer.canRemove()) {
                    removalCandiates.add(Triple.of(priority, buffer, priorityEntry.get().getKey()));
                }
            }
        }
        while (memoryToShed > 0) {
            if (false == removalCandiates.isEmpty()) {
                Triple<Float, CacheBufferType, String> toRemove = removalCandiates.poll();
                CacheBufferType minPriorityBuffer = toRemove.getMiddle();
                String minPriorityEntityModelId = toRemove.getRight();

                ModelState<RCFModelType> removed = minPriorityBuffer.remove(minPriorityEntityModelId);
                memoryToShed -= minPriorityBuffer.getMemoryConsumptionPerModel();
                addIntoInactiveCache(removed);

                if (minPriorityBuffer.canRemove()) {
                    // can remove another one
                    Optional<Entry<String, Float>> priorityEntry = minPriorityBuffer.getPriorityTracker().getMinimumScaledPriority();
                    if (priorityEntry.isPresent()) {
                        removalCandiates.add(Triple.of(priorityEntry.get().getValue(), minPriorityBuffer, priorityEntry.get().getKey()));
                    }
                }
            }

            if (removalCandiates.isEmpty()) {
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
        for (Map.Entry<String, CacheBufferType> entry : activeEnities.entrySet()) {
            CacheBufferType buffer = entry.getValue();
            reserved += buffer.getReservedBytes();
            shared += buffer.getBytesInSharedCache();
        }
        memoryTracker.syncMemoryState(origin, reserved + shared, reserved);
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
                String configId = cacheBufferEntry.getKey();
                CacheBufferType cacheBuffer = cacheBufferEntry.getValue();
                // remove expired cache buffer
                if (cacheBuffer.expired(modelTtl)) {
                    activeEnities.remove(configId);
                    cacheBuffer.clear();
                    priorityTrackerMap.remove(configId);
                } else {
                    List<ModelState<RCFModelType>> removedStates = cacheBuffer.maintenance();
                    for (ModelState<RCFModelType> state : removedStates) {
                        addIntoInactiveCache(state);
                    }
                }
            });

            maintainInactiveCache();

            doorKeepers.entrySet().stream().forEach(doorKeeperEntry -> {
                String configId = doorKeeperEntry.getKey();
                DoorKeeper doorKeeper = doorKeeperEntry.getValue();
                // doorKeeper has its own state ttl
                if (doorKeeper.expired(null)) {
                    doorKeepers.remove(configId);
                } else {
                    doorKeeper.maintenance();
                }
            });
        } catch (Exception e) {
            // will be thrown to ES's transport broadcast handler
            throw new TimeSeriesException("Fail to maintain cache", e);
        }

    }

    /**
     * Permanently deletes models hosted in memory and persisted in index.
     *
     * @param configId id the of the config for which models are to be permanently deleted
     */
    @Override
    public void clear(String configId) {
        if (Strings.isEmpty(configId)) {
            return;
        }
        CacheBufferType buffer = activeEnities.remove(configId);
        if (buffer != null) {
            buffer.clear();
        }
        priorityTrackerMap.remove(configId);
        checkpointDao.deleteModelCheckpointByConfigId(configId);
        doorKeepers.remove(configId);
        priorityTrackerMap.remove(configId);
    }

    /**
     * Get the number of active entities of a detector
     * @param detectorId Detector Id
     * @return The number of active entities
     */
    @Override
    public int getActiveEntities(String detectorId) {
        CacheBufferType cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            return cacheBuffer.getActiveEntities();
        }
        return 0;
    }

    /**
     * Whether an entity is active or not
     * @param configId The Id of the detector that an entity belongs to
     * @param entityModelId Entity's Model Id
     * @return Whether an entity is active or not
     */
    @Override
    public boolean isActive(String configId, String entityModelId) {
        CacheBufferType cacheBuffer = activeEnities.get(configId);
        if (cacheBuffer != null) {
            return cacheBuffer.isActive(entityModelId);
        }
        return false;
    }

    @Override
    public long getTotalUpdates(String configId) {
        return Optional
            .of(activeEnities)
            .map(entities -> entities.get(configId))
            .map(buffer -> buffer.getPriorityTracker().getHighestPriorityEntityId())
            .map(entityModelIdOptional -> entityModelIdOptional.get())
            .map(entityModelId -> getTotalUpdates(configId, entityModelId))
            .orElse(0L);
    }

    @Override
    public long getTotalUpdates(String configId, String entityModelId) {
        return Optional
            .ofNullable(activeEnities.get(configId))
            .map(cacheBuffer -> getTotalUpdates(cacheBuffer.getModelState(entityModelId)))
            .orElse(0L);
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
    public List<ModelState<RCFModelType>> getAllModels() {
        List<ModelState<RCFModelType>> states = new ArrayList<>();
        activeEnities.values().stream().forEach(cacheBuffer -> states.addAll(cacheBuffer.getAllModelStates()));
        return states;
    }

    /**
     * Gets a config's modelStates hosted on a node
     *
     * @return list of modelStates
     */
    @Override
    public List<ModelState<RCFModelType>> getAllModels(String configId) {
        List<ModelState<RCFModelType>> states = new ArrayList<>();
        CacheBufferType cacheBuffer = activeEnities.get(configId);
        if (cacheBuffer != null) {
            states.addAll(cacheBuffer.getAllModelStates());
        }
        return states;
    }

    /**
     * Gets all of a config's model sizes hosted on a node
     *
     * @param configId config Id
     * @return a map of model id to its memory size
     */
    @Override
    public Map<String, Long> getModelSize(String configId) {
        CacheBufferType cacheBuffer = activeEnities.get(configId);
        Map<String, Long> res = new HashMap<>();
        if (cacheBuffer != null) {
            long size = cacheBuffer.getMemoryConsumptionPerModel();
            cacheBuffer.getAllModelStates().forEach(entry -> res.put(entry.getModelId(), size));
        }
        return res;
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
    public long getLastActiveTime(String detectorId, String entityModelId) {
        CacheBufferType cacheBuffer = activeEnities.get(detectorId);
        long lastUsedMs = -1;
        if (cacheBuffer != null) {
            lastUsedMs = cacheBuffer.getLastUsedTime(entityModelId);
            if (lastUsedMs != -1) {
                return lastUsedMs;
            }
        }
        ModelState<RCFModelType> stateInActive = inActiveEntities.getIfPresent(entityModelId);
        if (stateInActive != null) {
            lastUsedMs = stateInActive.getLastUsedTime().toEpochMilli();
        }
        return lastUsedMs;
    }

    @Override
    public void releaseMemoryForOpenCircuitBreaker() {
        maintainInactiveCache();

        tryClearUpMemory();
        activeEnities.values().stream().forEach(cacheBuffer -> {
            if (cacheBuffer.canRemove()) {
                ModelState<RCFModelType> removed = cacheBuffer.remove();
                addIntoInactiveCache(removed);
            }
        });
    }

    private void maintainInactiveCache() {
        if (lastInActiveEntityMaintenance.plus(this.modelTtl).isAfter(clock.instant())) {
            // don't scan inactive cache too frequently as it is costly
            return;
        }

        // force maintenance of the cache. ref: https://tinyurl.com/pyy3p9v6
        inActiveEntities.cleanUp();

        // // make sure no model has been stored due to bugs
        for (ModelState<RCFModelType> state : inActiveEntities.asMap().values()) {
            Optional<RCFModelType> modelOptional = state.getModel();
            if (modelOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("Inactive entity's model is null: [{}]. Maybe there are bugs.", state.getModelId()));
                state.setModel(null);
            }
        }

        lastInActiveEntityMaintenance = clock.instant();
    }

    /**
     * Called when dedicated cache size changes.  Will adjust existing cache buffer's
     * cache size
     */
    private void setHCDedicatedCacheSizeListener() {
        activeEnities.values().stream().forEach(cacheBuffer -> cacheBuffer.setMinimumCapacity(hcDedicatedCacheSize));
    }

    private void setCheckpointFreqListener() {
        activeEnities.values().stream().forEach(cacheBuffer -> cacheBuffer.setCheckpointIntervalHrs(checkpointIntervalHrs));
    }

    @Override
    public List<ModelProfile> getAllModelProfile(String detectorId) {
        CacheBufferType cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            long size = cacheBuffer.getMemoryConsumptionPerModel();
            return cacheBuffer
                .getAllModelStates()
                .stream()
                .map(entry -> new ModelProfile(entry.getModelId(), entry.getEntity().orElse(null), size))
                .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    /**
     * Gets an entity's model state
     *
     * @param detectorId detector id
     * @param entityModelId  entity model id
     * @return the model state
     */
    @Override
    public Optional<ModelProfile> getModelProfile(String detectorId, String entityModelId) {
        CacheBufferType cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null && cacheBuffer.getModelState(entityModelId) != null) {
            ModelState<RCFModelType> modelState = cacheBuffer.getModelState(entityModelId);
            Entity entity = null;
            if (modelState != null && modelState.getEntity().isPresent()) {
                entity = modelState.getEntity().get();
            }
            return Optional.of(new ModelProfile(entityModelId, entity, cacheBuffer.getMemoryConsumptionPerModel()));
        }
        return Optional.empty();
    }

    /**
     * Throw an IllegalArgumentException even the dedicated size increases cannot
     * be fulfilled.
     *
     * @param newDedicatedCacheSize the new dedicated cache size to validate
     */
    private void validateDedicatedCacheSize(Integer newDedicatedCacheSize) {
        if (this.hcDedicatedCacheSize < newDedicatedCacheSize) {
            int delta = newDedicatedCacheSize - this.hcDedicatedCacheSize;
            long totalIncreasedBytes = 0;
            for (CacheBufferType cacheBuffer : activeEnities.values()) {
                totalIncreasedBytes += cacheBuffer.getMemoryConsumptionPerModel() * delta;
            }

            if (false == memoryTracker.canAllocateReserved(totalIncreasedBytes)) {
                throw new IllegalArgumentException("We don't have enough memory for the required change");
            }
        }
    }

    /**
     * Get a model state without incurring priority update. Used in maintenance.
     * @param configId Config Id
     * @param modelId Model Id
     * @return Model state
     */
    @Override
    public Optional<ModelState<RCFModelType>> getForMaintainance(String configId, String modelId) {
        CacheBufferType buffer = activeEnities.get(configId);
        if (buffer == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(buffer.getWithoutUpdatePriority(modelId));
    }

    /**
     * Remove model from active entity buffer and delete checkpoint. Used to clean corrupted model.
     * @param configId config Id
     * @param modelId Model Id
     */
    @Override
    public void removeModel(String configId, String modelId) {
        CacheBufferType buffer = activeEnities.get(configId);
        if (buffer != null) {
            ModelState<RCFModelType> removed = buffer.remove(modelId, false);
            if (removed != null) {
                addIntoInactiveCache(removed);
            }
        }
        checkpointDao
            .deleteModelCheckpoint(
                modelId,
                ActionListener
                    .wrap(
                        r -> LOG.debug(new ParameterizedMessage("Succeeded in deleting checkpoint [{}].", modelId)),
                        e -> LOG.error(new ParameterizedMessage("Failed to delete checkpoint [{}].", modelId), e)
                    )
            );
    }

    private Cache<String, ModelState<RCFModelType>> createInactiveCache(Duration inactiveEntityTtl, int maxInactiveStates) {
        return CacheBuilder
            .newBuilder()
            .expireAfterAccess(inactiveEntityTtl.toHours(), TimeUnit.HOURS)
            .maximumSize(maxInactiveStates)
            .concurrencyLevel(1)
            .build();
    }

    protected abstract ModelState<RCFModelType> createEmptyModelState(String modelId, String configId);

    protected abstract CacheBufferType createEmptyCacheBuffer(Config config, long memoryConsumptionPerEntity, PriorityTracker tracker);

    protected abstract boolean isDoorKeeperInCacheEnabled();
}
