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

package org.opensearch.ad.rest.handler;

import java.time.Clock;

import org.opensearch.action.ActionListener;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.transport.ValidateAnomalyDetectorResponse;
import org.opensearch.ad.util.SecurityClientUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;

/**
 * Anomaly detector REST action handler to process POST request.
 * POST request is for validating anomaly detector against detector and/or model configs.
 */
public class ValidateAnomalyDetectorActionHandler extends AbstractAnomalyDetectorActionHandler<ValidateAnomalyDetectorResponse> {

    /**
     * Constructor function.
     *
     * @param clusterService                  ClusterService
     * @param client                          ES node client that executes actions on the local node
     * @param clientUtil                      AD client utility
     * @param listener                        ES channel used to construct bytes / builder based outputs, and send responses
     * @param anomalyDetectionIndices         anomaly detector index manager
     * @param anomalyDetector                 anomaly detector instance
     * @param requestTimeout                  request time out configuration
     * @param maxSingleEntityAnomalyDetectors max single-entity anomaly detectors allowed
     * @param maxMultiEntityAnomalyDetectors  max multi-entity detectors allowed
     * @param maxAnomalyFeatures              max features allowed per detector
     * @param method                          Rest Method type
     * @param xContentRegistry                Registry which is used for XContentParser
     * @param user                            User context
     * @param searchFeatureDao                Search feature DAO
     * @param validationType                  Specified type for validation
     * @param clock                           Clock object to know when to timeout
     * @param settings                        Node settings
     */
    public ValidateAnomalyDetectorActionHandler(
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        ActionListener<ValidateAnomalyDetectorResponse> listener,
        AnomalyDetectionIndices anomalyDetectionIndices,
        AnomalyDetector anomalyDetector,
        TimeValue requestTimeout,
        Integer maxSingleEntityAnomalyDetectors,
        Integer maxMultiEntityAnomalyDetectors,
        Integer maxAnomalyFeatures,
        RestRequest.Method method,
        NamedXContentRegistry xContentRegistry,
        User user,
        SearchFeatureDao searchFeatureDao,
        String validationType,
        Clock clock,
        Settings settings
    ) {
        super(
            clusterService,
            client,
            clientUtil,
            null,
            listener,
            anomalyDetectionIndices,
            AnomalyDetector.NO_ID,
            null,
            null,
            null,
            anomalyDetector,
            requestTimeout,
            maxSingleEntityAnomalyDetectors,
            maxMultiEntityAnomalyDetectors,
            maxAnomalyFeatures,
            method,
            xContentRegistry,
            user,
            null,
            searchFeatureDao,
            validationType,
            true,
            clock,
            settings
        );
    }

    // If validation type is detector then all validation in AbstractAnomalyDetectorActionHandler that is called
    // by super.start() involves validation checks against the detector configurations,
    // any issues raised here would block user from creating the anomaly detector.
    // If validation Aspect is of type model then further non-blocker validation will be executed
    // after the blocker validation is executed. Any issues that are raised for model validation
    // are simply warnings for the user in terms of how configuration could be changed to lead to
    // a higher likelihood of model training completing successfully
    @Override
    public void start() {
        super.start();
    }
}
