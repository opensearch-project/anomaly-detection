/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import java.util.List;

/**
 * Input for {@link org.opensearch.ad.correlation.AnomalyCorrelation}.
 */
public class AnomalyCorrelationInput {
    private final List<org.opensearch.ad.correlation.Anomaly> anomalies;
    private final List<AnomalyDetector> detectors;

    public AnomalyCorrelationInput(List<org.opensearch.ad.correlation.Anomaly> anomalies, List<AnomalyDetector> detectors) {
        this.anomalies = anomalies;
        this.detectors = detectors;
    }

    public List<org.opensearch.ad.correlation.Anomaly> getAnomalies() {
        return anomalies;
    }

    public List<AnomalyDetector> getDetectors() {
        return detectors;
    }

    public int getAnomalyCount() {
        return anomalies.size();
    }

    public int getDetectorCount() {
        return detectors.size();
    }
}
