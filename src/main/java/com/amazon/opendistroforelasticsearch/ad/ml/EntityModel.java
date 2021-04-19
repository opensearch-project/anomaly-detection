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

package com.amazon.opendistroforelasticsearch.ad.ml;

import java.util.Queue;

import com.amazon.randomcutforest.RandomCutForest;

public class EntityModel {
    private String modelId;
    // TODO: sample should record timestamp
    private Queue<double[]> samples;
    private RandomCutForest rcf;
    private ThresholdingModel threshold;

    public EntityModel(String modelId, Queue<double[]> samples, RandomCutForest rcf, ThresholdingModel threshold) {
        this.modelId = modelId;
        this.samples = samples;
        this.rcf = rcf;
        this.threshold = threshold;
    }

    public String getModelId() {
        return this.modelId;
    }

    public Queue<double[]> getSamples() {
        return this.samples;
    }

    public void addSample(double[] sample) {
        if (sample != null && sample.length != 0) {
            this.samples.add(sample);
        }
    }

    public RandomCutForest getRcf() {
        return this.rcf;
    }

    public ThresholdingModel getThreshold() {
        return this.threshold;
    }

    public void setRcf(RandomCutForest rcf) {
        this.rcf = rcf;
    }

    public void setThreshold(ThresholdingModel threshold) {
        this.threshold = threshold;
    }

    public void clear() {
        samples.clear();
        rcf = null;
        threshold = null;
    }
}
