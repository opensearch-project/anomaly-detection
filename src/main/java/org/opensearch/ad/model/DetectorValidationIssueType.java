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

package org.opensearch.ad.model;

import java.util.Collection;
import java.util.Set;

import org.opensearch.ad.Name;

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
