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

import org.opensearch.ad.Name;

public enum DetectorValidationIssueType implements Name {
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
    RESULT_INDEX(AnomalyDetector.RESULT_INDEX_FIELD);

    private String name;

    DetectorValidationIssueType(String name) {
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
