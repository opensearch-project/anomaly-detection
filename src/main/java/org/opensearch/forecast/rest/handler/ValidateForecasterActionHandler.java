/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.rest.handler;

import java.time.Clock;

import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.rest.RestRequest.Method;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.timeseries.util.SecurityClientUtil;

/**
 * ValidateForecasterActionHandler extends the AbstractForecasterActionHandler to specifically handle
 * the validation of forecasting configurations within OpenSearch. This class is responsible for initiating
 * and executing the validation process for forecasters, ensuring that the configurations provided by the user
 * meet the necessary criteria and constraints for successful forecasting operations.
 *
 * Key responsibilities include:
 * - Performing thorough validation of forecaster configurations, including checks against maximum allowed
 *   configurations, feature constraints, and categorical field limitations.
 * - Utilizing the SearchFeatureDao to validate the feasibility of feature queries included in the forecaster
 *   configuration, ensuring that they can be successfully executed within the OpenSearch environment.
 * - Leveraging the broader framework provided by AbstractForecasterActionHandler to manage common tasks such
 *   as security checks, user context management, and interaction with forecast indices.
 *
 * Usage:
 * This handler is invoked during the forecaster configuration validation process, typically through REST API
 * calls made by users attempting to create or update forecasters. It is designed to provide immediate feedback
 * on the validity of the proposed configuration, helping users to adjust their settings to meet the system's
 * requirements and best practices.
 *
 * The ValidateForecasterActionHandler is instantiated with detailed configuration parameters, including limits
 * on the number of features, categorical fields, and other critical settings. It then proceeds to validate these
 * configurations against the existing system constraints and the data available in the specified indices.
 *
 * Example:
 * The handler could be triggered by a REST call to validate a new forecaster configuration before its creation,
 * ensuring that all specified settings and features are valid and that the forecaster is likely to operate
 * successfully within the given constraints and data environment.
 */
public class ValidateForecasterActionHandler extends AbstractForecasterActionHandler<ValidateConfigResponse> {

    public ValidateForecasterActionHandler(
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        ForecastIndexManagement forecastIndices,
        Config forecaster,
        TimeValue requestTimeout,
        Integer maxSingleStreamForecasters,
        Integer maxHCForecasters,
        Integer maxFeatures,
        Integer maxCategoricalFields,
        Method method,
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
            forecastIndices,
            Config.NO_ID,
            null,
            null,
            null,
            forecaster,
            requestTimeout,
            maxSingleStreamForecasters,
            maxHCForecasters,
            maxFeatures,
            maxCategoricalFields,
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

}
