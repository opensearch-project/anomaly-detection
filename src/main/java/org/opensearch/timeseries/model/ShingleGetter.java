/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.model;

public interface ShingleGetter {
    Integer getShingleSize(Integer customShingleSize);
}
