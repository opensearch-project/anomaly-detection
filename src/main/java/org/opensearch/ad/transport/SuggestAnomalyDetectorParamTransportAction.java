/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.ValidationException;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.transport.BaseSuggestConfigParamTransportAction;
import org.opensearch.timeseries.transport.SuggestConfigParamRequest;
import org.opensearch.timeseries.transport.SuggestConfigParamResponse;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import com.google.common.collect.Sets;

public class SuggestAnomalyDetectorParamTransportAction extends BaseSuggestConfigParamTransportAction<AnomalyDetector> {
    public static final Logger logger = LogManager.getLogger(SuggestAnomalyDetectorParamTransportAction.class);

    @Inject
    public SuggestAnomalyDetectorParamTransportAction(
        Client client,
        SecurityClientUtil clientUtil,
        ClusterService clusterService,
        Settings settings,
        ADIndexManagement anomalyDetectionIndices,
        ActionFilters actionFilters,
        TransportService transportService,
        SearchFeatureDao searchFeatureDao,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        super(
            SuggestAnomalyDetectorParamAction.NAME,
            client,
            clientUtil,
            clusterService,
            settings,
            actionFilters,
            transportService,
            AD_FILTER_BY_BACKEND_ROLES,
            AnalysisType.AD,
            searchFeatureDao,
            Name.getListStrs(Arrays.asList(ADSuggestName.values())),
            AnomalyDetector.class,
            namedWriteableRegistry

        );
    }

    @Override
    public void suggestExecute(
        SuggestConfigParamRequest request,
        User user,
        ThreadContext.StoredContext storedContext,
        ActionListener<SuggestConfigParamResponse> listener
    ) {
        storedContext.restore();
        // Get parameters to suggest - no need to filter HORIZON since AD SuggestName doesn't have it
        Set<? extends Name> params = getParametersToSuggest(request.getParam());

        if (params.isEmpty()) {
            ValidationException validationException = new ValidationException();
            validationException.addValidationError(CommonMessages.NOT_EXISTENT_SUGGEST_TYPE);
            listener.onFailure(validationException);
            return;
        }

        Config config = request.getConfig();

        int responseSize = params.size();
        // history suggest interval too as history suggest depends on interval suggest
        if (params.contains(ADSuggestName.HISTORY) && params.contains(ADSuggestName.INTERVAL)) {
            responseSize -= 1;
        }

        MultiResponsesDelegateActionListener<SuggestConfigParamResponse> delegateListener =
            new MultiResponsesDelegateActionListener<SuggestConfigParamResponse>(
                listener,
                responseSize,
                CommonMessages.FAIL_SUGGEST_ERR_MSG,
                false
            );

        // history suggest interval too as history suggest depends on interval suggest
        if (params.contains(ADSuggestName.HISTORY)) {
            suggestHistory(
                request.getConfig(),
                user,
                request.getRequestTimeout(),
                params.contains(ADSuggestName.INTERVAL),
                delegateListener
            );
        } else if (params.contains(ADSuggestName.INTERVAL)) {
            suggestInterval(
                request.getConfig(),
                user,
                request.getRequestTimeout(),
                ActionListener
                    .wrap(
                        intervalEntity -> delegateListener
                            .onResponse(new SuggestConfigParamResponse.Builder().interval(intervalEntity.getLeft()).build()),
                        delegateListener::onFailure
                    )
            );
        }

        if (params.contains(ADSuggestName.WINDOW_DELAY)) {
            suggestWindowDelay(request.getConfig(), user, request.getRequestTimeout(), delegateListener);
        }
    }

    @Override
    protected Set<? extends Name> getParametersToSuggest(String typesStr) {
        Set<String> typesInRequest = new HashSet<>(Arrays.asList(typesStr.split(",")));
        return ADSuggestName.getNames(Sets.intersection(allSuggestParamStrs, typesInRequest));
    }
}
