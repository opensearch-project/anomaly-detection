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

import static org.opensearch.ad.indices.AnomalyDetectionIndices.ALL_AD_RESULTS_INDEX_PATTERN;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.transport.SearchAnomalyResultAction;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to search anomaly results.
 */
public class RestSearchAnomalyResultAction extends AbstractSearchAction<AnomalyResult> {

    private static final String LEGACY_URL_PATH = AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI + "/results/_search";
    private static final String URL_PATH = AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI + "/results/_search";
    private final String SEARCH_ANOMALY_DETECTOR_ACTION = "search_anomaly_result";

    public RestSearchAnomalyResultAction() {
        super(
            ImmutableList.of(),
            ImmutableList.of(Pair.of(URL_PATH, LEGACY_URL_PATH)),
            ALL_AD_RESULTS_INDEX_PATTERN,
            AnomalyResult.class,
            SearchAnomalyResultAction.INSTANCE
        );
    }

    @Override
    public String getName() {
        return SEARCH_ANOMALY_DETECTOR_ACTION;
    }
}
