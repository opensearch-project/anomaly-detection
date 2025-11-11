/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import static org.opensearch.forecast.settings.ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.rest.handler.ValidateForecasterActionHandler;
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

public class ValidateForecasterTransportAction extends BaseValidateConfigTransportAction<ForecastIndex, ForecastIndexManagement> {
    public static final Logger logger = LogManager.getLogger(ValidateForecasterTransportAction.class);

    @Inject
    public ValidateForecasterTransportAction(
        Client client,
        SecurityClientUtil clientUtil,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Settings settings,
        ForecastIndexManagement anomalyDetectionIndices,
        ActionFilters actionFilters,
        TransportService transportService,
        SearchFeatureDao searchFeatureDao,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(
            ValidateForecasterAction.NAME,
            client,
            clientUtil,
            clusterService,
            xContentRegistry,
            settings,
            anomalyDetectionIndices,
            actionFilters,
            transportService,
            searchFeatureDao,
            FORECAST_FILTER_BY_BACKEND_ROLES,
            ValidationAspect.FORECASTER,
            namedWriteableRegistry
        );
    }

    @Override
    protected Processor<ValidateConfigResponse> createProcessor(Config forecaster, ValidateConfigRequest request, User user) {
        return new ValidateForecasterActionHandler(
            clusterService,
            client,
            clientUtil,
            indexManagement,
            forecaster,
            request.getRequestTimeout(),
            request.getMaxSingleEntityAnomalyDetectors(),
            request.getMaxMultiEntityAnomalyDetectors(),
            request.getMaxAnomalyFeatures(),
            request.getMaxCategoricalFields(),
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
