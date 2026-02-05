/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import java.util.List;

/**
 * Metadata about a detector for insights generation.
 * Collected during anomaly query phase to avoid additional lookups.
 */
public class DetectorMetadata {
    private final String detectorId;
    private final String detectorName;
    private final List<String> indices;

    public DetectorMetadata(String detectorId, String detectorName, List<String> indices) {
        this.detectorId = detectorId;
        this.detectorName = detectorName;
        this.indices = indices;
    }

    public String getDetectorId() {
        return detectorId;
    }

    public String getDetectorName() {
        return detectorName;
    }

    public List<String> getIndices() {
        return indices;
    }
}
