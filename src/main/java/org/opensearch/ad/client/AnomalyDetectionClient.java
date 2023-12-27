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
     * @param detectorId the detector ID to fetch the profile for
     * @return ActionFuture of GetAnomalyDetectorResponse
     */
    default ActionFuture<GetAnomalyDetectorResponse> getDetectorProfile(String detectorId) {
        PlainActionFuture<GetAnomalyDetectorResponse> actionFuture = PlainActionFuture.newFuture();
        getDetectorProfile(detectorId, actionFuture);
        return actionFuture;
    }

    /**
     * Get detector profile - refer to https://opensearch.org/docs/latest/observing-your-data/ad/api/#profile-detector
     * @param detectorId the detector ID to fetch the profile for
     * @param listener a listener to be notified of the result
     */
    void getDetectorProfile(String detectorId, ActionListener<GetAnomalyDetectorResponse> listener);

}
