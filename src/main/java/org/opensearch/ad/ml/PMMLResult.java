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

import java.util.Objects;

// A reference for the format to expect for external call responses from the ML plugin.
// This class is not used now. In PMML result transport action, we simply convert the results from the ML plugin
// into values needed for the anomaly result response.
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
