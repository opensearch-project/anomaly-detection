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

package org.opensearch.ad.model;

import java.util.List;

public class EntityAnomalyResult implements Mergeable {

    private List<AnomalyResult> anomalyResults;

    public EntityAnomalyResult(List<AnomalyResult> anomalyResults) {
        this.anomalyResults = anomalyResults;
    }

    public List<AnomalyResult> getAnomalyResults() {
        return anomalyResults;
    }

    @Override
    public void merge(Mergeable other) {
        if (this == other || other == null || getClass() != other.getClass()) {
            return;
        }
        EntityAnomalyResult otherEntityAnomalyResult = (EntityAnomalyResult) other;
        if (otherEntityAnomalyResult.getAnomalyResults() != null) {
            this.anomalyResults.addAll(otherEntityAnomalyResult.getAnomalyResults());
        }
    }
}
