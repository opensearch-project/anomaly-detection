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

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.ad.CleanState;
import org.opensearch.ad.DetectorModelSize;
import org.opensearch.ad.MaintenanceState;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.ModelProfile;

public interface EntityCache extends MaintenanceState, CleanState, DetectorModelSize {
    /**
     * Get the ModelState associated with the entity.  May or may not load the
     * ModelState depending on the underlying cache's eviction policy.
     *
     * @param modelId Model Id
     * @param detector Detector config object
     * @return the ModelState associated with the model or null if no cached item
     * for the entity
     */
    ModelState<EntityModel> get(String modelId, AnomalyDetector detector);

    /**
     * Get the number of active entities of a detector
     * @param detector Detector Id
     * @return The number of active entities
     */
    int getActiveEntities(String detector);

    /**
      *
      * @return total active entities in the cache
    */
    int getTotalActiveEntities();

    /**
     * Whether an entity is active or not
     * @param detectorId The Id of the detector that an entity belongs to
     * @param entityModelId Entity model Id
     * @return Whether an entity is active or not
     */
    boolean isActive(String detectorId, String entityModelId);

    /**
     * Get total updates of detector's most active entity's RCF model.
     *
     * @param detectorId detector id
     * @return RCF model total updates of most active entity.
     */
    long getTotalUpdates(String detectorId);

    /**
     * Get RCF model total updates of specific entity
     *
     * @param detectorId detector id
     * @param entityModelId  entity model id
     * @return RCF model total updates of specific entity.
     */
    long getTotalUpdates(String detectorId, String entityModelId);

    /**
     * Gets modelStates of all model hosted on a node
     *
     * @return list of modelStates
     */
    List<ModelState<?>> getAllModels();

    /**
     * Return when the last active time of an entity's state.
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
    long getLastActiveMs(String detectorId, String entityModelId);

    /**
     * Release memory when memory circuit breaker is open
     */
    void releaseMemoryForOpenCircuitBreaker();

    /**
     * Select candidate entities for which we can load models
     * @param cacheMissEntities Cache miss entities
     * @param detectorId Detector Id
     * @param detector Detector object
     * @return A list of entities that are admitted into the cache as a result of the
     *  update and the left-over entities
     */
    Pair<List<Entity>, List<Entity>> selectUpdateCandidate(
        Collection<Entity> cacheMissEntities,
        String detectorId,
        AnomalyDetector detector
    );

    /**
     *
     * @param detector Detector config
     * @param toUpdate Model state candidate
     * @return if we can host the given model state
     */
    boolean hostIfPossible(AnomalyDetector detector, ModelState<EntityModel> toUpdate);

    /**
     *
     * @param detectorId Detector Id
     * @return a detector's model information
     */
    List<ModelProfile> getAllModelProfile(String detectorId);

    /**
     * Gets an entity's model sizes
     *
     * @param detectorId Detector Id
     * @param entityModelId Entity's model Id
     * @return the entity's memory size
     */
    Optional<ModelProfile> getModelProfile(String detectorId, String entityModelId);
}
