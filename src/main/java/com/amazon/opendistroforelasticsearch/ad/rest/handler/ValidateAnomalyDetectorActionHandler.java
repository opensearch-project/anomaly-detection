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

package com.amazon.opendistroforelasticsearch.ad.rest.handler;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;

import com.amazon.opendistroforelasticsearch.ad.feature.SearchFeatureDao;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.ValidationAspect;
import com.amazon.opendistroforelasticsearch.ad.transport.ValidateAnomalyDetectorResponse;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.google.common.collect.Sets;

public class ValidateAnomalyDetectorActionHandler extends AbstractAnomalyDetectorActionHandler<ValidateAnomalyDetectorResponse> {

    private static final Set<ValidationAspect> DEFAULT_VALIDATION_ASPECTS = Sets.newHashSet(ValidationAspect.DETECTOR);
    private static final Set<String> ALL_VALIDATION_ASPECTS_STRS = Arrays
        .asList(ValidationAspect.values())
        .stream()
        .map(aspect -> aspect.getName())
        .collect(Collectors.toSet());

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
     * @param typeStr                         specified type for validation
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
        String typeStr
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
        String normalizedTypes = StringUtils.isBlank(typeStr) ? ValidationAspect.MODEL.getName() : typeStr.trim().replaceAll("\\s", "");
        Set<String> typesInRequest = new HashSet<>(Arrays.asList(normalizedTypes.split(",")));

        this.aspects = Sets
            .union(DEFAULT_VALIDATION_ASPECTS, ValidationAspect.getNames(Sets.intersection(ALL_VALIDATION_ASPECTS_STRS, typesInRequest)));
    }

    @Override
    public void start() throws IOException {
        super.start();
        if (aspects.contains(ValidationAspect.MODEL)) {
            validateModelConfig();
        }
    }

    // TODO: add implementation for model config validation
    private void validateModelConfig() {

    }

}
