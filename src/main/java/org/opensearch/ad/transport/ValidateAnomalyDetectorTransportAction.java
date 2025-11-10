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

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.ValidateAnomalyDetectorActionHandler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.rest.handler.Processor;
import org.opensearch.timeseries.transport.BaseValidateConfigTransportAction;
import org.opensearch.timeseries.transport.ValidateConfigRequest;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class ValidateAnomalyDetectorTransportAction extends BaseValidateConfigTransportAction<ADIndex, ADIndexManagement, AnomalyDetector> {
    public static final Logger logger = LogManager.getLogger(ValidateAnomalyDetectorTransportAction.class);

    @Inject
    public ValidateAnomalyDetectorTransportAction(
        Client client,
        SecurityClientUtil clientUtil,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Settings settings,
        ADIndexManagement anomalyDetectionIndices,
        ActionFilters actionFilters,
        TransportService transportService,
        SearchFeatureDao searchFeatureDao,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(
            ValidateAnomalyDetectorAction.NAME,
            client,
            clientUtil,
            clusterService,
            xContentRegistry,
            settings,
            anomalyDetectionIndices,
            actionFilters,
            transportService,
            searchFeatureDao,
            AD_FILTER_BY_BACKEND_ROLES,
            ValidationAspect.DETECTOR,
            AnomalyDetector.class,
            namedWriteableRegistry
        );
    }

    @Override
    protected Processor<ValidateConfigResponse> createProcessor(
        Config detector,
        ValidateConfigRequest request,
        User user,
        Integer maxSingleStreamConfigs,
        Integer maxHCConfigs,
        Integer maxFeatures,
        Integer maxCategoricalFields
    ) {
        return new ValidateAnomalyDetectorActionHandler(
            clusterService,
            client,
            clientUtil,
            indexManagement,
            detector,
            request.getRequestTimeout(),
            maxSingleStreamConfigs,
            maxHCConfigs,
            maxFeatures,
            maxCategoricalFields,
            RestRequest.Method.POST,
            xContentRegistry,
            user,
            searchFeatureDao,
            request.getValidationType(),
            clock,
            settings
        );
    }
}
