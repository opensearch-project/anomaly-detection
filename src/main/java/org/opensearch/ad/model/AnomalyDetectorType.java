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

public enum AnomalyDetectorType {
    @Deprecated
    REALTIME_SINGLE_ENTITY,
    @Deprecated
    REALTIME_MULTI_ENTITY,
    @Deprecated
    HISTORICAL_SINGLE_ENTITY,
    @Deprecated
    HISTORICAL_MULTI_ENTITY,

    SINGLE_ENTITY,
    MULTI_ENTITY,
}
