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

package com.amazon.opendistroforelasticsearch.ad.model;

import java.util.Collection;
import java.util.Set;

import com.amazon.opendistroforelasticsearch.ad.Name;

public enum DetectorValidationIssueType implements Name {
    NAME(AnomalyDetector.NAME_FIELD),
    TIMEFIELD_FIELD(AnomalyDetector.TIMEFIELD_FIELD),
    SHINGLE_SIZE_FIELD(AnomalyDetector.SHINGLE_SIZE_FIELD),
    INDICES(AnomalyDetector.INDICES_FIELD),
    FEATURE_ATTRIBUTES(AnomalyDetector.FEATURE_ATTRIBUTES_FIELD),
    DETECTION_INTERVAL(AnomalyDetector.DETECTION_INTERVAL_FIELD),
    CATEGORY(AnomalyDetector.CATEGORY_FIELD),
    FILTER_QUERY(AnomalyDetector.FILTER_QUERY_FIELD);

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

    public static DetectorValidationIssueType getName(String name) {
        switch (name) {
            case AnomalyDetector.NAME_FIELD:
                return NAME;
            case AnomalyDetector.INDICES_FIELD:
                return INDICES;
            case AnomalyDetector.FEATURE_ATTRIBUTES_FIELD:
                return FEATURE_ATTRIBUTES;
            case AnomalyDetector.DETECTION_INTERVAL_FIELD:
                return DETECTION_INTERVAL;
            case AnomalyDetector.CATEGORY_FIELD:
                return CATEGORY;
            case AnomalyDetector.FILTER_QUERY_FIELD:
                return FILTER_QUERY;
            default:
                throw new IllegalArgumentException("Unsupported validation type");
        }
    }

    public static Set<DetectorValidationIssueType> getNames(Collection<String> names) {
        return Name.getNameFromCollection(names, DetectorValidationIssueType::getName);
    }
}
