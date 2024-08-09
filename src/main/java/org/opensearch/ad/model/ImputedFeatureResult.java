/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

public class ImputedFeatureResult {
    boolean[] isFeatureImputed;
    double[] actual;

    public ImputedFeatureResult(boolean[] isFeatureImputed, double[] actual) {
        this.isFeatureImputed = isFeatureImputed;
        this.actual = actual;
    }

    public boolean[] getIsFeatureImputed() {
        return isFeatureImputed;
    }

    public double[] getActual() {
        return actual;
    }
}
