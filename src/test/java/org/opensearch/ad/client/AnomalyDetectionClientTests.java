/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.client;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.transport.ADTaskProfileResponse;
import org.opensearch.core.action.ActionListener;

public class AnomalyDetectionClientTests {

    AnomalyDetectionClient anomalyDetectionClient;

    @Mock
    SearchResponse searchDetectorsResponse;

    @Mock
    SearchResponse searchResultsResponse;

    @Mock
    ADTaskProfileResponse profileResponse;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        // Implementing req'd methods of the interface. These methods are all called internally by the
        // default methods that we test below.
        anomalyDetectionClient = new AnomalyDetectionClient() {
            @Override
            public void searchAnomalyDetectors(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
                listener.onResponse(searchDetectorsResponse);
            }

            @Override
            public void searchAnomalyResults(SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
                listener.onResponse(searchResultsResponse);
            }

            @Override
            public void getDetectorProfile(String detectorId, ActionListener<ADTaskProfileResponse> listener) {
                listener.onResponse(profileResponse);
            }
        };
    }

    @Test
    public void searchAnomalyDetectors() {
        assertEquals(searchDetectorsResponse, anomalyDetectionClient.searchAnomalyDetectors(new SearchRequest()).actionGet());
    }

    @Test
    public void searchAnomalyResults() {
        assertEquals(searchResultsResponse, anomalyDetectionClient.searchAnomalyResults(new SearchRequest()).actionGet());
    }

    @Test
    public void getDetectorProfile() {
        assertEquals(profileResponse, anomalyDetectionClient.getDetectorProfile("foo").actionGet());
    }

}
