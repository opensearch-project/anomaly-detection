/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

public enum BuildInQuery {
    MIN_CONFIDENCE_INTERVAL_WIDTH,
    MAX_CONFIDENCE_INTERVAL_WIDTH,
    MIN_VALUE_WITHIN_THE_HORIZON,
    MAX_VALUE_WITHIN_THE_HORIZON,
    DISTANCE_TO_THRESHOLD_VALUE
}
