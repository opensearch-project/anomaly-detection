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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.ExpiringState;
import org.opensearch.ad.MaintenanceState;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.MemoryTracker.Origin;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.model.InitProgressProfile;

/**
 * We use a layered cache to manage active entities’ states.  We have a two-level
 * cache that stores active entity states in each node.  Each detector has its
 * dedicated cache that stores ten (dynamically adjustable) entities’ states per
 * node.  A detector’s hottest entities load their states in the dedicated cache.
 * If less than 10 entities use the dedicated cache, the secondary cache can use
 * the rest of the free memory available to AD.  The secondary cache is a shared
 * memory among all detectors for the long tail.  The shared cache size is 10%
 * heap minus all of the dedicated cache consumed by single-entity and multi-entity
 * detectors.  The shared cache’s size shrinks as the dedicated cache is filled
 * up or more detectors are started.
 *
 * Implementation-wise, both dedicated cache and shared cache are stored in items
 * and minimumCapacity controls the boundary. If items size is equals to or less
 * than minimumCapacity, consider items as dedicated cache; otherwise, consider
 * top minimumCapacity active entities (last X entities in priorityList) as in dedicated
 * cache and all others in shared cache.
 */
public class CacheBuffer implements ExpiringState, MaintenanceState {
    private static final Logger LOG = LogManager.getLogger(CacheBuffer.class);

    // max entities to track per detector
    private final int MAX_TRACKING_ENTITIES = 1000000;

    private final int minimumCapacity;
    // key -> value
    private final ConcurrentHashMap<String, ModelState<EntityModel>> items;
    // memory consumption per entity
    private final long memoryConsumptionPerEntity;
    private final MemoryTracker memoryTracker;
    private final CheckpointDao checkpointDao;
    private final Duration modelTtl;
    private final String detectorId;
    private Instant lastUsedTime;
    private final long reservedBytes;
    private final PriorityTracker priorityTracker;
    private final Clock clock;

    public CacheBuffer(
        int minimumCapacity,
        long intervalSecs,
        CheckpointDao checkpointDao,
        long memoryConsumptionPerEntity,
        MemoryTracker memoryTracker,
        Clock clock,
        Duration modelTtl,
        String detectorId
    ) {
        if (minimumCapacity <= 0) {
            throw new IllegalArgumentException("minimum capacity should be larger than 0");
        }
        this.minimumCapacity = minimumCapacity;

        this.items = new ConcurrentHashMap<>();

        this.memoryConsumptionPerEntity = memoryConsumptionPerEntity;
        this.memoryTracker = memoryTracker;

        this.checkpointDao = checkpointDao;
        this.modelTtl = modelTtl;
        this.detectorId = detectorId;
        this.lastUsedTime = clock.instant();

        this.reservedBytes = memoryConsumptionPerEntity * minimumCapacity;
        this.clock = clock;
        this.priorityTracker = new PriorityTracker(clock, intervalSecs, clock.instant().getEpochSecond(), MAX_TRACKING_ENTITIES);
    }

    /**
     * Update step at period t_k:
     * new priority = old priority + log(1+e^{\log(g(t_k-L))-old priority}) where g(n) = e^{0.125n},
     * and n is the period.
     * @param entityModelId model Id
     */
    private void update(String entityModelId) {
        priorityTracker.updatePriority(entityModelId);

        Instant now = clock.instant();
        items.get(entityModelId).setLastUsedTime(now);
        lastUsedTime = now;
    }

    /**
     * Insert the model state associated with a model Id to the cache
     * @param entityModelId the model Id
     * @param value the ModelState
     */
    public void put(String entityModelId, ModelState<EntityModel> value) {
        // race conditions can happen between the put and one of the following operations:
        // remove: not a problem as it is unlikely we are removing and putting the same thing
        // maintenance: not a problem as we are unlikely to maintain an entry that's not
        // already in the cache
        // clear: not a problem as we are releasing memory in MemoryTracker.
        // The newly added one loses references and soon GC will collect it.
        // We have memory tracking correction to fix incorrect memory usage record.
        // put from other threads: not a problem as the entry is associated with
        // entityModelId and our put is idempotent
        put(entityModelId, value, value.getPriority());
    }

    /**
    * Insert the model state associated with a model Id to the cache.  Update priority.
    * @param entityModelId the model Id
    * @param value the ModelState
    * @param priority the priority
    */
    private void put(String entityModelId, ModelState<EntityModel> value, float priority) {
        ModelState<EntityModel> contentNode = items.get(entityModelId);
        if (contentNode == null) {
            priorityTracker.addPriority(entityModelId, priority);
            items.put(entityModelId, value);
            Instant now = clock.instant();
            value.setLastUsedTime(now);
            lastUsedTime = now;
            // shared cache empty means we are consuming reserved cache.
            // Since we have already considered them while allocating CacheBuffer,
            // skip bookkeeping.
            if (!sharedCacheEmpty()) {
                memoryTracker.consumeMemory(memoryConsumptionPerEntity, false, Origin.MULTI_ENTITY_DETECTOR);
            }
        } else {
            update(entityModelId);
            items.put(entityModelId, value);
        }
    }

    /**
     * Retrieve the ModelState associated with the model Id or null if the CacheBuffer
     * contains no mapping for the model Id
     * @param key the model Id
     * @return the Model state to which the specified model Id is mapped, or null
     * if this CacheBuffer contains no mapping for the model Id
     */
    public ModelState<EntityModel> get(String key) {
        // We can get an item that is to be removed soon due to race condition.
        // This is acceptable as it won't cause any corruption and exception.
        // And this item is used for scoring one last time.
        ModelState<EntityModel> node = items.get(key);
        if (node == null) {
            return null;
        }
        update(key);
        return node;
    }

    /**
     *
     * @return whether there is one item that can be removed from shared cache
     */
    public boolean canRemove() {
        return !items.isEmpty() && items.size() > minimumCapacity;
    }

    /**
     * remove the smallest priority item.
     * @return the associated ModelState associated with the key, or null if there
     * is no associated ModelState for the key
     */
    public ModelState<EntityModel> remove() {
        // race conditions can happen between the put and one of the following operations:
        // remove from other threads: not a problem. If they remove the same item,
        // our method is idempotent. If they remove two different items,
        // they don't impact each other.
        // maintenance: not a problem as all of the data structures are concurrent.
        // Two threads removing the same entry is not a problem.
        // clear: not a problem as we are releasing memory in MemoryTracker.
        // The removed one loses references and soon GC will collect it.
        // We have memory tracking correction to fix incorrect memory usage record.
        // put: not a problem as it is unlikely we are removing and putting the same thing
        Optional<String> key = priorityTracker.getMinimumPriorityEntityId();
        if (key.isPresent()) {
            return remove(key.get());
        }
        return null;
    }

    /**
     * Remove everything associated with the key and make a checkpoint.
     *
     * @param keyToRemove The key to remove
     * @return the associated ModelState associated with the key, or null if there
     * is no associated ModelState for the key
     */
    public ModelState<EntityModel> remove(String keyToRemove) {
        priorityTracker.removePriority(keyToRemove);

        // if shared cache is empty, we are using reserved memory
        boolean reserved = sharedCacheEmpty();

        ModelState<EntityModel> valueRemoved = items.remove(keyToRemove);

        if (valueRemoved != null) {
            // if we releasing a shared cache item, release memory as well.
            if (!reserved) {
                memoryTracker.releaseMemory(memoryConsumptionPerEntity, false, Origin.MULTI_ENTITY_DETECTOR);
            }
            checkpointDao.write(valueRemoved, keyToRemove);

            EntityModel modelRemoved = valueRemoved.getModel();
            if (modelRemoved != null) {
                modelRemoved.clear();
            }
        }

        return valueRemoved;
    }

    /**
     * @return whether dedicated cache is available or not
     */
    public boolean dedicatedCacheAvailable() {
        return items.size() < minimumCapacity;
    }

    /**
     * @return whether shared cache is empty or not
     */
    public boolean sharedCacheEmpty() {
        return items.size() <= minimumCapacity;
    }

    /**
     *
     * @return the estimated number of bytes per entity state
     */
    public long getMemoryConsumptionPerEntity() {
        return memoryConsumptionPerEntity;
    }

    /**
     *
     * If the cache is not full, check if some other items can replace internal entities
     * within the same detector.
     *
     * @param priority another entity's priority
     * @return whether one entity can be replaced by another entity with a certain priority
     */
    public boolean canReplaceWithinDetector(float priority) {
        if (items.isEmpty()) {
            return false;
        }
        Entry<String, Float> minPriorityItem = priorityTracker.getMinimumPriority();
        return minPriorityItem != null && priority > minPriorityItem.getValue();
    }

    /**
     * Replace the smallest priority entity with the input entity
     * @param entityModelId the Model Id
     * @param value the model State
     * @return the associated ModelState associated with the key, or null if there
     * is no associated ModelState for the key
     */
    public ModelState<EntityModel> replace(String entityModelId, ModelState<EntityModel> value) {
        ModelState<EntityModel> replaced = remove();
        put(entityModelId, value);
        return replaced;
    }

    @Override
    public void maintenance() {
        items.entrySet().stream().forEach(entry -> {
            String entityModelId = entry.getKey();
            try {
                ModelState<EntityModel> modelState = entry.getValue();
                Instant now = clock.instant();

                if (modelState.getLastUsedTime().plus(modelTtl).isBefore(now)) {
                    // race conditions can happen between the put and one of the following operations:
                    // remove: not a problem as all of the data structures are concurrent.
                    // Two threads removing the same entry is not a problem.
                    // clear: not a problem as we are releasing memory in MemoryTracker.
                    // The removed one loses references and soon GC will collect it.
                    // We have memory tracking correction to fix incorrect memory usage record.
                    // put: not a problem as we are unlikely to maintain an entry that's not
                    // already in the cache
                    // remove method saves checkpoint as well
                    remove(entityModelId);
                } else {
                    // we can have ConcurrentModificationException when serializing
                    // and updating rcf model at the same time. To prevent this,
                    // we need to have a deep copy of models or have a lock. Both
                    // options are costly.
                    // As we are gonna retry serializing either when the entity is
                    // evicted out of cache or during the next maintenance period,
                    // don't do anything when the exception happens.
                    checkpointDao.write(modelState, entityModelId);
                }
            } catch (Exception e) {
                LOG.warn("Failed to finish maintenance for model id " + entityModelId, e);
            }
        });
    }

    /**
     *
     * @return the number of active entities
     */
    public int getActiveEntities() {
        return items.size();
    }

    /**
     *
     * @param entityModelId Model Id
     * @return Whether the model is active or not
     */
    public boolean isActive(String entityModelId) {
        return items.containsKey(entityModelId);
    }

    /**
     *
     * @param entityModelId Model Id
     * @return Last used time of the model
     */
    public long getLastUsedTime(String entityModelId) {
        ModelState<EntityModel> state = items.get(entityModelId);
        if (state != null) {
            return state.getLastUsedTime().toEpochMilli();
        }
        return -1;
    }

    /**
     *
     * @param entityModelId entity Id
     * @return Get the model of an entity
     */
    public Optional<EntityModel> getModel(String entityModelId) {
        return Optional.of(items).map(map -> map.get(entityModelId)).map(state -> state.getModel());
    }

    /**
     * Clear associated memory.  Used when we are removing an detector.
     */
    public void clear() {
        // race conditions can happen between the put and remove/maintenance/put:
        // not a problem as we are releasing memory in MemoryTracker.
        // The newly added one loses references and soon GC will collect it.
        // We have memory tracking correction to fix incorrect memory usage record.
        memoryTracker.releaseMemory(getReservedBytes(), true, Origin.MULTI_ENTITY_DETECTOR);
        if (!sharedCacheEmpty()) {
            memoryTracker.releaseMemory(getBytesInSharedCache(), false, Origin.MULTI_ENTITY_DETECTOR);
        }
        items.clear();
        priorityTracker.clearPriority();
    }

    /**
     *
     * @return reserved bytes by the CacheBuffer
     */
    public long getReservedBytes() {
        return reservedBytes;
    }

    /**
     *
     * @return bytes consumed in the shared cache by the CacheBuffer
     */
    public long getBytesInSharedCache() {
        int sharedCacheEntries = items.size() - minimumCapacity;
        if (sharedCacheEntries > 0) {
            return memoryConsumptionPerEntity * sharedCacheEntries;
        }
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        if (obj instanceof InitProgressProfile) {
            CacheBuffer other = (CacheBuffer) obj;

            EqualsBuilder equalsBuilder = new EqualsBuilder();
            equalsBuilder.append(detectorId, other.detectorId);

            return equalsBuilder.isEquals();
        }
        return false;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(detectorId).toHashCode();
    }

    @Override
    public boolean expired(Duration stateTtl) {
        return expired(lastUsedTime, stateTtl, clock.instant());
    }

    public String getDetectorId() {
        return detectorId;
    }

    public List<ModelState<?>> getAllModels() {
        return items.values().stream().collect(Collectors.toList());
    }

    public PriorityTracker getPriorityTracker() {
        return priorityTracker;
    }
}
