/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.client;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.transport.GetAnomalyDetectorResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.transport.GetConfigRequest;
import org.opensearch.timeseries.transport.SuggestConfigParamRequest;
import org.opensearch.timeseries.transport.SuggestConfigParamResponse;
import org.opensearch.timeseries.transport.ValidateConfigRequest;
import org.opensearch.timeseries.transport.ValidateConfigResponse;

/**
 * A client to provide interfaces for anomaly detection functionality. This will be used by other plugins.
 */
public interface AnomalyDetectionClient {
    /**
     * Search anomaly detectors - refer to https://opensearch.org/docs/latest/observing-your-data/ad/api/#search-detector
     * @param searchRequest search request to search the anomaly detectors
     * @return ActionFuture of SearchResponse
     */
    default ActionFuture<SearchResponse> searchAnomalyDetectors(SearchRequest searchRequest) {
        PlainActionFuture<SearchResponse> actionFuture = PlainActionFuture.newFuture();
        searchAnomalyDetectors(searchRequest, actionFuture);
        return actionFuture;
    }

    /**
     * Search anomaly detectors - refer to https://opensearch.org/docs/latest/observing-your-data/ad/api/#search-detector
     * @param searchRequest search request to search the anomaly detectors
     * @param listener a listener to be notified of the result
     */
    void searchAnomalyDetectors(SearchRequest searchRequest, ActionListener<SearchResponse> listener);

    /**
     * Search anomaly results - refer to https://opensearch.org/docs/latest/observing-your-data/ad/api/#search-detector-result
     * @param searchRequest search request to search the anomaly results
     * @return ActionFuture of SearchResponse
     */
    default ActionFuture<SearchResponse> searchAnomalyResults(SearchRequest searchRequest) {
        PlainActionFuture<SearchResponse> actionFuture = PlainActionFuture.newFuture();
        searchAnomalyResults(searchRequest, actionFuture);
        return actionFuture;
    }

    /**
     * Search anomaly results - refer to https://opensearch.org/docs/latest/observing-your-data/ad/api/#search-detector-result
     * @param searchRequest search request to search the anomaly results
     * @param listener a listener to be notified of the result
     */
    void searchAnomalyResults(SearchRequest searchRequest, ActionListener<SearchResponse> listener);

    /**
     * Get detector profile - refer to https://opensearch.org/docs/latest/observing-your-data/ad/api/#profile-detector
     * @param profileRequest request to fetch the detector profile
     * @return ActionFuture of GetAnomalyDetectorResponse
     */
    default ActionFuture<GetAnomalyDetectorResponse> getDetectorProfile(GetConfigRequest profileRequest) {
        PlainActionFuture<GetAnomalyDetectorResponse> actionFuture = PlainActionFuture.newFuture();
        getDetectorProfile(profileRequest, actionFuture);
        return actionFuture;
    }

    /**
     * Get detector profile - refer to https://opensearch.org/docs/latest/observing-your-data/ad/api/#profile-detector
     * @param profileRequest request to fetch the detector profile
     * @param listener a listener to be notified of the result
     */
    void getDetectorProfile(GetConfigRequest profileRequest, ActionListener<GetAnomalyDetectorResponse> listener);

    /**
     * Validate anomaly detector - refer to https://opensearch.org/docs/latest/observing-your-data/ad/api/#validate-detector
     * @param validateRequest request to validate the detector configuration
     * @return ActionFuture of ValidateConfigResponse
     */
    default ActionFuture<ValidateConfigResponse> validateAnomalyDetector(ValidateConfigRequest validateRequest) {
        PlainActionFuture<ValidateConfigResponse> actionFuture = PlainActionFuture.newFuture();
        validateAnomalyDetector(validateRequest, actionFuture);
        return actionFuture;
    }

    /**
     * Validate anomaly detector - refer to https://opensearch.org/docs/latest/observing-your-data/ad/api/#validate-detector
     * @param validateRequest request to validate the detector configuration
     * @param listener a listener to be notified of the result
     */
    void validateAnomalyDetector(ValidateConfigRequest validateRequest, ActionListener<ValidateConfigResponse> listener);

    /**
     * Suggest anomaly detector parameters - refer to https://opensearch.org/docs/latest/observing-your-data/ad/api/#suggest-detector
     * @param suggestRequest request to suggest detector configuration parameters
     * @return ActionFuture of SuggestConfigParamResponse
     */
    default ActionFuture<SuggestConfigParamResponse> suggestAnomalyDetector(SuggestConfigParamRequest suggestRequest) {
        PlainActionFuture<SuggestConfigParamResponse> actionFuture = PlainActionFuture.newFuture();
        suggestAnomalyDetector(suggestRequest, actionFuture);
        return actionFuture;
    }

    /**
     * Suggest anomaly detector parameters - refer to https://opensearch.org/docs/latest/observing-your-data/ad/api/#suggest-detector
     * @param suggestRequest request to suggest detector configuration parameters
     * @param listener a listener to be notified of the result
     */
    void suggestAnomalyDetector(SuggestConfigParamRequest suggestRequest, ActionListener<SuggestConfigParamResponse> listener);
}
