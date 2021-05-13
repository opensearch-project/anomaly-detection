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

package org.opensearch.ad.rest;

import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to search anomaly detectors.
 */
public class RestSearchAnomalyDetectorAction extends AbstractSearchAction<AnomalyDetector> {

    private static final String LEGACY_URL_PATH = AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI + "/_search";
    private static final String URL_PATH = AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI + "/_search";
    private final String SEARCH_ANOMALY_DETECTOR_ACTION = "search_anomaly_detector";

    public RestSearchAnomalyDetectorAction() {
        super(
            ImmutableList.of(),
            ImmutableList.of(Pair.of(URL_PATH, LEGACY_URL_PATH)),
            ANOMALY_DETECTORS_INDEX,
            AnomalyDetector.class,
            SearchAnomalyDetectorAction.INSTANCE
        );
    }

    @Override
    public String getName() {
        return SEARCH_ANOMALY_DETECTOR_ACTION;
    }
}
