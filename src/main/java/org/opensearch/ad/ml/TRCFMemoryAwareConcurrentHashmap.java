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

package org.opensearch.ad.ml;

import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.MemoryTracker.Origin;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A customized ConcurrentHashMap that can automatically consume and release memory.
 * This enables minimum change to our single-entity code as we just have to replace
 * the map implementation.
 *
 * Note: this is mainly used for single-entity detectors.
 */
public class TRCFMemoryAwareConcurrentHashmap<K> extends ConcurrentHashMap<K, ModelState<ThresholdedRandomCutForest>> {
    private final MemoryTracker memoryTracker;

    public TRCFMemoryAwareConcurrentHashmap(MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
    }

    @Override
    public ModelState<ThresholdedRandomCutForest> remove(Object key) {
        ModelState<ThresholdedRandomCutForest> deletedModelState = super.remove(key);
        if (deletedModelState != null && deletedModelState.getModel() != null) {
            long memoryToRelease = memoryTracker.estimateTRCFModelSize(deletedModelState.getModel());
            memoryTracker.releaseMemory(memoryToRelease, true, Origin.REAL_TIME_DETECTOR);
        }
        return deletedModelState;
    }

    @Override
    public ModelState<ThresholdedRandomCutForest> put(K key, ModelState<ThresholdedRandomCutForest> value) {
        ModelState<ThresholdedRandomCutForest> previousAssociatedState = super.put(key, value);
        if (value != null && value.getModel() != null) {
            long memoryToConsume = memoryTracker.estimateTRCFModelSize(value.getModel());
            memoryTracker.consumeMemory(memoryToConsume, true, Origin.REAL_TIME_DETECTOR);
        }
        return previousAssociatedState;
    }
}
