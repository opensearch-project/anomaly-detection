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

package org.opensearch.ad.feature;

import java.util.Optional;

/**
 * Features for one data point.
 *
 * A data point consists of unprocessed features (raw search results) and corresponding processed ML features.
 */
public class SinglePointFeatures {

    private final Optional<double[]> unprocessedFeatures;
    private final Optional<double[]> processedFeatures;

    /**
     * Constructor.
     *
     * @param unprocessedFeatures unprocessed features
     * @param processedFeatures processed features
     */
    public SinglePointFeatures(Optional<double[]> unprocessedFeatures, Optional<double[]> processedFeatures) {
        this.unprocessedFeatures = unprocessedFeatures;
        this.processedFeatures = processedFeatures;
    }

    /**
     * Returns unprocessed features.
     *
     * @return unprocessed features
     */
    public Optional<double[]> getUnprocessedFeatures() {
        return this.unprocessedFeatures;
    }

    /**
     * Returns processed features.
     *
     * @return processed features
     */
    public Optional<double[]> getProcessedFeatures() {
        return this.processedFeatures;
    }
}
