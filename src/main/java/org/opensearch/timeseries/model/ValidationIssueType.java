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

package org.opensearch.timeseries.model;

import org.opensearch.ad.Name;
import org.opensearch.ad.model.AnomalyDetector;

public enum ValidationIssueType implements Name {
    NAME(AnomalyDetector.NAME_FIELD),
    TIMEFIELD_FIELD(AnomalyDetector.TIMEFIELD_FIELD),
    SHINGLE_SIZE_FIELD(AnomalyDetector.SHINGLE_SIZE_FIELD),
    INDICES(AnomalyDetector.INDICES_FIELD),
    FEATURE_ATTRIBUTES(AnomalyDetector.FEATURE_ATTRIBUTES_FIELD),
    DETECTION_INTERVAL(AnomalyDetector.DETECTION_INTERVAL_FIELD),
    CATEGORY(AnomalyDetector.CATEGORY_FIELD),
    FILTER_QUERY(AnomalyDetector.FILTER_QUERY_FIELD),
    WINDOW_DELAY(AnomalyDetector.WINDOW_DELAY_FIELD),
    GENERAL_SETTINGS(AnomalyDetector.GENERAL_SETTINGS),
    RESULT_INDEX(AnomalyDetector.RESULT_INDEX_FIELD),
    TIMEOUT(AnomalyDetector.TIMEOUT),
    AGGREGATION(AnomalyDetector.AGGREGATION); // this is a unique case where aggregation failed due to an issue in core but
                                              // don't want to throw exception

    private String name;

    ValidationIssueType(String name) {
        this.name = name;
    }

    /**
     * Get validation type
     *
     * @return name
     */
    @Override
    public String getName() {
        return name;
    }
}
