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

package org.opensearch.timeseries.ml;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A customized ConcurrentHashMap that can automatically consume and release memory.
 * This enables minimum change to our single-stream code as we just have to replace
 * the map implementation.
 *
 * Note: this is mainly used for single-stream configs. The key is model id.
 */
public class MemoryAwareConcurrentHashmap<RCFModelType extends ThresholdedRandomCutForest> extends
    ConcurrentHashMap<String, ModelState<RCFModelType>> {
    protected final MemoryTracker memoryTracker;

    public MemoryAwareConcurrentHashmap(MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
    }

    @Override
    public ModelState<RCFModelType> remove(Object key) {
        ModelState<RCFModelType> deletedModelState = super.remove(key);
        if (deletedModelState != null && deletedModelState.getModel().isPresent()) {
            long memoryToRelease = memoryTracker.estimateTRCFModelSize(deletedModelState.getModel().get());
            memoryTracker.releaseMemory(memoryToRelease, true, Origin.REAL_TIME_DETECTOR);
        }
        return deletedModelState;
    }

    @Override
    public ModelState<RCFModelType> put(String key, ModelState<RCFModelType> value) {
        ModelState<RCFModelType> previousAssociatedState = super.put(key, value);
        if (value != null && value.getModel().isPresent()) {
            long memoryToConsume = memoryTracker.estimateTRCFModelSize(value.getModel().get());
            memoryTracker.consumeMemory(memoryToConsume, true, Origin.REAL_TIME_DETECTOR);
        }
        return previousAssociatedState;
    }

    /**
     * Gets all of a config's model sizes hosted on a node
     *
     * @param configId config Id
     * @return a map of model id to its memory size
     */
    public Map<String, Long> getModelSize(String configId) {
        Map<String, Long> res = new HashMap<>();
        super.entrySet()
            .stream()
            .filter(entry -> SingleStreamModelIdMapper.getConfigIdForModelId(entry.getKey()).equals(configId))
            .forEach(entry -> {
                Optional<RCFModelType> modelOptional = entry.getValue().getModel();
                if (modelOptional.isPresent()) {
                    res.put(entry.getKey(), memoryTracker.estimateTRCFModelSize(modelOptional.get()));
                }
            });
        return res;
    }

    /**
    * Checks if a model exists for the given config.
    * @param configId Config Id
    * @return `true` if the model exists, `false` otherwise.
    */
    public boolean doesModelExist(String configId) {
        return super.entrySet()
            .stream()
            .filter(entry -> SingleStreamModelIdMapper.getConfigIdForModelId(entry.getKey()).equals(configId))
            .anyMatch(n -> true);
    }

    public boolean hostIfPossible(String modelId, ModelState<RCFModelType> toUpdate) {
        return Optional
            .ofNullable(toUpdate)
            .filter(state -> state.getModel().isPresent())
            .filter(state -> memoryTracker.isHostingAllowed(modelId, state.getModel().get()))
            .map(state -> {
                super.put(modelId, toUpdate);
                return true;
            })
            .orElse(false);
    }
}
