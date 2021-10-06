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
