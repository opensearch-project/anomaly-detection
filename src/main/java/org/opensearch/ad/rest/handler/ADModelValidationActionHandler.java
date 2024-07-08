/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest.handler;

import java.time.Clock;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.rest.handler.ModelValidationActionHandler;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.timeseries.util.SecurityClientUtil;

public class ADModelValidationActionHandler extends ModelValidationActionHandler {

    public ADModelValidationActionHandler(
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        ActionListener<ValidateConfigResponse> listener,
        AnomalyDetector config,
        TimeValue requestTimeout,
        NamedXContentRegistry xContentRegistry,
        SearchFeatureDao searchFeatureDao,
        String validationType,
        Clock clock,
        Settings settings,
        User user
    ) {
        super(
            clusterService,
            client,
            clientUtil,
            listener,
            config,
            requestTimeout,
            xContentRegistry,
            searchFeatureDao,
            validationType,
            clock,
            settings,
            user,
            AnalysisType.AD,
            ValidationIssueType.DETECTION_INTERVAL
        );
    }

}
