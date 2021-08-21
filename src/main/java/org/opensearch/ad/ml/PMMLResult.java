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

import java.util.Objects;

/**
 * A reference for the format to expect for external call responses from the ML plugin.
 * This class is not used now. In PMML result transport action, we simply convert the results from the ML plugin
 * into values needed for the anomaly result response.
 */
public class PMMLResult {
    private final boolean outlier;
    private final double decisionFunction;

    /**
     * Constructor with all arguments.
     *
     * @param outlier whether the point is an outlier
     * @param decisionFunction value that decides outlier, with negative means outlier, and positive means inlier
     */
    public PMMLResult(boolean outlier, double decisionFunction) {
        this.outlier = outlier;
        this.decisionFunction = decisionFunction;
    }

    /**
     * Returns the outlier boolean.
     *
     * @return the outlier boolean
     */
    public boolean getOutlier() {
        return outlier;
    }

    /**
     * Returns the decision function double.
     *
     * @return the decision function double
     */
    public double getDecisionFunction() {
        return decisionFunction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PMMLResult that = (PMMLResult) o;
        return Objects.equals(this.outlier, that.outlier) && Objects.equals(this.decisionFunction, that.decisionFunction);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outlier, decisionFunction);
    }
}
