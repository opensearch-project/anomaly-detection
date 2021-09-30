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
