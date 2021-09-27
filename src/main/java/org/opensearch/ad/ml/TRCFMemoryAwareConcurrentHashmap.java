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

package org.opensearch.ad.ml;

import java.util.concurrent.ConcurrentHashMap;

import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.MemoryTracker.Origin;

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
            memoryTracker.releaseMemory(memoryToRelease, true, Origin.SINGLE_ENTITY_DETECTOR);
        }
        return deletedModelState;
    }

    @Override
    public ModelState<ThresholdedRandomCutForest> put(K key, ModelState<ThresholdedRandomCutForest> value) {
        ModelState<ThresholdedRandomCutForest> previousAssociatedState = super.put(key, value);
        if (value != null && value.getModel() != null) {
            long memoryToConsume = memoryTracker.estimateTRCFModelSize(value.getModel());
            memoryTracker.consumeMemory(memoryToConsume, true, Origin.SINGLE_ENTITY_DETECTOR);
        }
        return previousAssociatedState;
    }
}
