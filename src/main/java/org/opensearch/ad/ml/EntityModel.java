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

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;

import org.opensearch.ad.model.Entity;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class EntityModel {
    private Entity entity;
    // TODO: sample should record timestamp
    private Queue<double[]> samples;
    private ThresholdedRandomCutForest trcf;

    /**
     * Constructor with TRCF.
     *
     * @param entity entity if any
     * @param samples samples with the model
     * @param trcf thresholded rcf model
     */
    public EntityModel(Entity entity, Queue<double[]> samples, ThresholdedRandomCutForest trcf) {
        this.entity = entity;
        this.samples = samples;
        this.trcf = trcf;
    }

    /**
     * In old checkpoint mapping, we don't have entity. It's fine we are missing
     * entity as it is mostly used for debugging.
     * @return entity
     */
    public Optional<Entity> getEntity() {
        return Optional.ofNullable(entity);
    }

    public Queue<double[]> getSamples() {
        return this.samples;
    }

    public void addSample(double[] sample) {
        if (this.samples == null) {
            this.samples = new ArrayDeque<>();
        }
        if (sample != null && sample.length != 0) {
            this.samples.add(sample);
        }
    }

    /**
     * Sets an trcf model.
     *
     * @param trcf an trcf model
     */
    public void setTrcf(ThresholdedRandomCutForest trcf) {
        this.trcf = trcf;
    }

    /**
     * Returns optional trcf model.
     *
     * @return the trcf model or empty
     */
    public Optional<ThresholdedRandomCutForest> getTrcf() {
        return Optional.ofNullable(this.trcf);
    }

    public void clear() {
        if (samples != null) {
            samples.clear();
        }
        trcf = null;
    }
}
