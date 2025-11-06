/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.client;

import java.util.function.Function;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.transport.GetAnomalyDetectorAction;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;
import org.opensearch.ad.transport.SearchAnomalyResultAction;
import org.opensearch.ad.transport.SuggestAnomalyDetectorParamAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorAction;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.timeseries.transport.GetConfigRequest;
import org.opensearch.timeseries.transport.SuggestConfigParamRequest;
import org.opensearch.timeseries.transport.SuggestConfigParamResponse;
import org.opensearch.timeseries.transport.ValidateConfigRequest;
import org.opensearch.timeseries.transport.ValidateConfigResponse;
import org.opensearch.transport.client.Client;

public class AnomalyDetectionNodeClient implements AnomalyDetectionClient {
    private final Client client;
    private final NamedWriteableRegistry namedWriteableRegistry;

    public AnomalyDetectionNodeClient(Client client, NamedWriteableRegistry namedWriteableRegistry) {
        this.client = client;
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    public void searchAnomalyDetectors(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        this.client.execute(SearchAnomalyDetectorAction.INSTANCE, searchRequest, ActionListener.wrap(searchResponse -> {
            listener.onResponse(searchResponse);
        }, listener::onFailure));
    }

    @Override
    public void searchAnomalyResults(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
        this.client.execute(SearchAnomalyResultAction.INSTANCE, searchRequest, ActionListener.wrap(searchResponse -> {
            listener.onResponse(searchResponse);
        }, listener::onFailure));
    }

    @Override
    public void getDetectorProfile(GetConfigRequest profileRequest, ActionListener<GetAnomalyDetectorResponse> listener) {
        this.client.execute(GetAnomalyDetectorAction.INSTANCE, profileRequest, getAnomalyDetectorResponseActionListener(listener));
    }

    @Override
    public void validateAnomalyDetector(ValidateConfigRequest validateRequest, ActionListener<ValidateConfigResponse> listener) {
        this.client.execute(ValidateAnomalyDetectorAction.INSTANCE, validateRequest, validateConfigResponseActionListener(listener));
    }

    @Override
    public void suggestAnomalyDetector(SuggestConfigParamRequest suggestRequest, ActionListener<SuggestConfigParamResponse> listener) {
        this.client.execute(SuggestAnomalyDetectorParamAction.INSTANCE, suggestRequest, suggestConfigResponseActionListener(listener));
    }

    // We need to wrap AD-specific response type listeners around an internal listener, and re-generate the response from a generic
    // ActionResponse. This is needed to prevent classloader issues and ClassCastExceptions when executed by other plugins.
    // Additionally, we need to inject the configured NamedWriteableRegistry so NamedWriteables (present in sub-fields of
    // GetAnomalyDetectorResponse) are able to be re-serialized and prevent errors like the following:
    // "can't read named writeable from StreamInput"
    private ActionListener<GetAnomalyDetectorResponse> getAnomalyDetectorResponseActionListener(
        ActionListener<GetAnomalyDetectorResponse> listener
    ) {
        ActionListener<GetAnomalyDetectorResponse> internalListener = ActionListener.wrap(getAnomalyDetectorResponse -> {
            listener.onResponse(getAnomalyDetectorResponse);
        }, listener::onFailure);
        ActionListener<GetAnomalyDetectorResponse> actionListener = wrapActionListener(internalListener, actionResponse -> {
            GetAnomalyDetectorResponse response = GetAnomalyDetectorResponse
                .fromActionResponse(actionResponse, this.namedWriteableRegistry);
            return response;
        });
        return actionListener;
    }

    private ActionListener<ValidateConfigResponse> validateConfigResponseActionListener(ActionListener<ValidateConfigResponse> listener) {
        ActionListener<ValidateConfigResponse> internalListener = ActionListener.wrap(validateConfigResponse -> {
            listener.onResponse(validateConfigResponse);
        }, listener::onFailure);
        ActionListener<ValidateConfigResponse> actionListener = wrapActionListener(internalListener, actionResponse -> {
            ValidateConfigResponse response = ValidateConfigResponse.fromActionResponse(actionResponse, this.namedWriteableRegistry);
            return response;
        });
        return actionListener;
    }

    private ActionListener<SuggestConfigParamResponse> suggestConfigResponseActionListener(
        ActionListener<SuggestConfigParamResponse> listener
    ) {
        ActionListener<SuggestConfigParamResponse> internalListener = ActionListener.wrap(suggestConfigResponse -> {
            listener.onResponse(suggestConfigResponse);
        }, listener::onFailure);
        ActionListener<SuggestConfigParamResponse> actionListener = wrapActionListener(internalListener, actionResponse -> {
            SuggestConfigParamResponse response = SuggestConfigParamResponse
                .fromActionResponse(actionResponse, this.namedWriteableRegistry);
            return response;
        });
        return actionListener;
    }

    private <T extends ActionResponse> ActionListener<T> wrapActionListener(
        final ActionListener<T> listener,
        final Function<ActionResponse, T> recreate
    ) {
        ActionListener<T> actionListener = ActionListener.wrap(r -> {
            listener.onResponse(recreate.apply(r));
            ;
        }, e -> { listener.onFailure(e); });
        return actionListener;
    }
}
