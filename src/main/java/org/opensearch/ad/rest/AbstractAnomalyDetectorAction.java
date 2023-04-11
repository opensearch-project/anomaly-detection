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
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClusterService;

public abstract class AbstractAnomalyDetectorAction extends BaseExtensionRestHandler {

    protected volatile TimeValue requestTimeout;
    protected volatile TimeValue detectionInterval;
    protected volatile TimeValue detectionWindowDelay;
    protected volatile Integer maxSingleEntityDetectors;
    protected volatile Integer maxMultiEntityDetectors;
    protected volatile Integer maxAnomalyFeatures;

    public AbstractAnomalyDetectorAction(ExtensionsRunner extensionsRunner) {
        Settings environmentSettings = extensionsRunner.getEnvironmentSettings();
        this.requestTimeout = REQUEST_TIMEOUT.get(environmentSettings);
        this.detectionInterval = DETECTION_INTERVAL.get(environmentSettings);
        this.detectionWindowDelay = DETECTION_WINDOW_DELAY.get(environmentSettings);
        this.maxSingleEntityDetectors = MAX_SINGLE_ENTITY_ANOMALY_DETECTORS.get(environmentSettings);
        this.maxMultiEntityDetectors = MAX_MULTI_ENTITY_ANOMALY_DETECTORS.get(environmentSettings);
        this.maxAnomalyFeatures = MAX_ANOMALY_FEATURES.get(environmentSettings);

        SDKClusterService clusterService = extensionsRunner.getSdkClusterService();
        // TODO: will add more cluster setting consumer later
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DETECTION_INTERVAL, it -> detectionInterval = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(DETECTION_WINDOW_DELAY, it -> detectionWindowDelay = it);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_SINGLE_ENTITY_ANOMALY_DETECTORS, it -> maxSingleEntityDetectors = it);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_MULTI_ENTITY_ANOMALY_DETECTORS, it -> maxMultiEntityDetectors = it);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ANOMALY_FEATURES, it -> maxAnomalyFeatures = it);
    }
}
