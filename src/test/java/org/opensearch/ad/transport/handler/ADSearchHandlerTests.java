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

package org.opensearch.ad.transport.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.TestHelpers.matchAllRequest;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;

import java.util.List;

import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.sdk.Extension;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKClusterService.SDKClusterSettings;

public class ADSearchHandlerTests extends ADUnitTestCase {

    private SDKRestClient client;
    private Settings settings;
    private SDKClusterService clusterService;
    private ADSearchHandler searchHandler;
    private SDKClusterSettings clusterSettings;
    private ExtensionsRunner mockRunner;

    private SearchRequest request;

    private ActionListener<SearchResponse> listener;

    @SuppressWarnings("unchecked")
    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put(FILTER_BY_BACKEND_ROLES.getKey(), false).build();
        mockRunner = mock(ExtensionsRunner.class);
        Settings settings = Settings.EMPTY;
        List<Setting<?>> settingsList = List.of(AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW, AnomalyDetectorSettings.PAGE_SIZE);
        clusterService = mock(SDKClusterService.class);
        ;
        Extension mockExtension = mock(Extension.class);
        when(mockRunner.getEnvironmentSettings()).thenReturn(settings);
        when(mockRunner.getExtension()).thenReturn(mockExtension);
        when(mockExtension.getSettings()).thenReturn(settingsList);
        SDKClusterSettings clusterSettings = new SDKClusterService(mockRunner).getClusterSettings();
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        client = mock(SDKRestClient.class);
        searchHandler = new ADSearchHandler(settings, clusterService, client);

        request = mock(SearchRequest.class);
        listener = mock(ActionListener.class);
    }

    public void testSearchException() {
        doThrow(new RuntimeException("test")).when(client).search(any(), any());
        searchHandler.search(request, listener);
        verify(listener, times(1)).onFailure(any());
    }

    public void testFilterEnabledWithWrongSearch() {
        settings = Settings.builder().put(FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        clusterService = new SDKClusterService(mockRunner);

        searchHandler = new ADSearchHandler(settings, clusterService, client);
        searchHandler.search(request, listener);
        // Thread Context User has been replaced with null as part of https://github.com/opensearch-project/opensearch-sdk/issues/23
        // so the search call will always succeed
        // verify(listener, times(1)).onFailure(any());
        verify(listener, times(0)).onFailure(any());
    }

    public void testFilterEnabled() {
        settings = Settings.builder().put(FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        clusterService = new SDKClusterService(mockRunner);

        searchHandler = new ADSearchHandler(settings, clusterService, client);
        searchHandler.search(matchAllRequest(), listener);
        verify(client, times(1)).search(any(), any());
    }
}
