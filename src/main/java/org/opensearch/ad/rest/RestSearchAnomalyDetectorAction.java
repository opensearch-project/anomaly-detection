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

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.constant.CommonName;

import com.google.common.collect.ImmutableList;

/**
 * This class consists of the REST handler to search anomaly detectors.
 */
public class RestSearchAnomalyDetectorAction extends AbstractADSearchAction<AnomalyDetector> {

    private static final String LEGACY_URL_PATH = TimeSeriesAnalyticsPlugin.LEGACY_OPENDISTRO_AD_BASE_URI + "/_search";
    private static final String URL_PATH = TimeSeriesAnalyticsPlugin.AD_BASE_DETECTORS_URI + "/_search";
    private final String SEARCH_ANOMALY_DETECTOR_ACTION = "search_anomaly_detector";

    public RestSearchAnomalyDetectorAction() {
        super(
            ImmutableList.of(),
            ImmutableList.of(Pair.of(URL_PATH, LEGACY_URL_PATH)),
            CommonName.CONFIG_INDEX,
            AnomalyDetector.class,
            SearchAnomalyDetectorAction.INSTANCE
        );
    }

    @Override
    public String getName() {
        return SEARCH_ANOMALY_DETECTOR_ACTION;
    }
}
