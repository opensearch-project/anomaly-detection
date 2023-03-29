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

import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;

import com.google.common.collect.ImmutableList;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;

/**
 * This class consists of the REST handler to search anomaly detectors.
 */
public class RestSearchAnomalyDetectorAction extends AbstractSearchAction<AnomalyDetector> {

    private static final String LEGACY_URL_PATH = AnomalyDetectorPlugin.LEGACY_OPENDISTRO_AD_BASE_URI + "/_search";
    private static final String URL_PATH = AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI + "/_search";
    private final String SEARCH_ANOMALY_DETECTOR_ACTION = "search_anomaly_detector";
    private SDKRestClient client;
    private ExtensionsRunner extensionsRunner;

    public RestSearchAnomalyDetectorAction(ExtensionsRunner extensionsRunner, SDKRestClient client) {
        super(
            ImmutableList.of(),
            ImmutableList.of(Pair.of(URL_PATH, LEGACY_URL_PATH)),
            ANOMALY_DETECTORS_INDEX,
            AnomalyDetector.class,
            SearchAnomalyDetectorAction.INSTANCE,
                client,
                extensionsRunner
        );
    }

    public String getName() {
        return SEARCH_ANOMALY_DETECTOR_ACTION;
    }
}
