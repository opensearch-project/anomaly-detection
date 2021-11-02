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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.ValidationAspect;
import org.opensearch.ad.rest.RestValidateAnomalyDetectorAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.commons.authuser.User;
import org.opensearch.rest.RestRequest;

import com.google.common.collect.Sets;

/**
 * Anomaly detector REST action handler to process POST request.
 * POST request is for validating anomaly detector against detector and/or model configs.
 */
public class ValidateAnomalyDetectorActionHandler extends AbstractAnomalyDetectorActionHandler<ValidateAnomalyDetectorResponse> {

    private static final Set<ValidationAspect> DEFAULT_VALIDATION_ASPECTS = Sets.newHashSet(ValidationAspect.DETECTOR);
    private final Set<ValidationAspect> aspects;

    /**
     * Constructor function.
     *
     * @param clusterService                  ClusterService
     * @param client                          ES node client that executes actions on the local node
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
     * @param validationType                         specified type for validation
     */
    public ValidateAnomalyDetectorActionHandler(
        ClusterService clusterService,
        Client client,
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
        String validationType
    ) {
        super(
            clusterService,
            client,
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
            true
        );
        this.aspects = getValidationTypes(validationType);
    }

    // All current validation that is done in the AbstractAnomalyDetectorActionHandler that is called
    // by super.start() involves validation checks against the detector configurations,
    // any issues raised here would block user from creating the anomaly detector.
    @Override
    public void start() throws IOException {
        super.start();
    }

    // Future additional implementation of the validation API will include model validation
    // which are for non-blocker issues meaning detector creation can be executed after
    // and only suggestions are given on how to improve configs.
    // PR outlining the blocker level validations already implemented above:
    // https://github.com/opensearch-project/anomaly-detection/pull/231
    // TODO: add implementation for model config validation
    private void validateModelConfig() {

    }

    private Set<ValidationAspect> getValidationTypes(String validationType) {
        if (StringUtils.isBlank(validationType)) {
            return DEFAULT_VALIDATION_ASPECTS;
        } else {
            Set<String> typesInRequest = new HashSet<>(Arrays.asList(validationType.split(",")));
            return ValidationAspect
                .getNames(Sets.intersection(RestValidateAnomalyDetectorAction.ALL_VALIDATION_ASPECTS_STRS, typesInRequest));
        }
    }
}
