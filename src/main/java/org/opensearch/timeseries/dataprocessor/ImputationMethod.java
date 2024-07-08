/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.dataprocessor;

public enum ImputationMethod {
    /**
     * This method replaces all missing values with 0's. It's a simple approach, but it may introduce bias if the data is not centered around zero.
     */
    ZERO,
    /**
     * This method replaces missing values with a predefined set of values. The values are the same for each input dimension, and they need to be specified by the user.
     */
    FIXED_VALUES,
    /**
     * This method replaces missing values with the last known value in the respective input dimension. It's a commonly used method for time series data, where temporal continuity is expected.
     */
    PREVIOUS,
}
