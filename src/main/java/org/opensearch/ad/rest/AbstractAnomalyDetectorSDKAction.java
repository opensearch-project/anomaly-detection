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

import static org.opensearch.ad.settings.AnomalyDetectorSettings.DETECTION_INTERVAL;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.DETECTION_WINDOW_DELAY;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_ANOMALY_FEATURES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sdk.BaseExtensionRestHandler;

public abstract class AbstractAnomalyDetectorSDKAction extends BaseExtensionRestHandler {

    protected volatile TimeValue requestTimeout;
    protected volatile TimeValue detectionInterval;
    protected volatile TimeValue detectionWindowDelay;
    protected volatile Integer maxSingleEntityDetectors;
    protected volatile Integer maxMultiEntityDetectors;
    protected volatile Integer maxAnomalyFeatures;

    public AbstractAnomalyDetectorSDKAction(Settings settings) {
        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        this.detectionInterval = DETECTION_INTERVAL.get(settings);
        this.detectionWindowDelay = DETECTION_WINDOW_DELAY.get(settings);
        this.maxSingleEntityDetectors = MAX_SINGLE_ENTITY_ANOMALY_DETECTORS.get(settings);
        this.maxMultiEntityDetectors = MAX_MULTI_ENTITY_ANOMALY_DETECTORS.get(settings);
        this.maxAnomalyFeatures = MAX_ANOMALY_FEATURES.get(settings);
        // TODO: Cluster Settings Consumers
    }
}
