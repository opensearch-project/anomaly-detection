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

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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

import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.threadpool.ThreadPool;

public class ADSearchHandlerTests extends ADUnitTestCase {

    private Client client;
    private Settings settings;
    private ClusterService clusterService;
    private ADSearchHandler searchHandler;
    private ClusterSettings clusterSettings;

    private SearchRequest request;

    private ActionListener<SearchResponse> listener;

    @SuppressWarnings("unchecked")
    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().put(FILTER_BY_BACKEND_ROLES.getKey(), false).build();
        clusterSettings = clusterSetting(settings, FILTER_BY_BACKEND_ROLES);
        clusterService = new ClusterService(settings, clusterSettings, null);
        client = mock(Client.class);
        searchHandler = new ADSearchHandler(settings, clusterService, client);

        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        org.opensearch.threadpool.ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(client.threadPool().getThreadContext()).thenReturn(threadContext);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

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
        clusterService = new ClusterService(settings, clusterSettings, null);

        searchHandler = new ADSearchHandler(settings, clusterService, client);
        searchHandler.search(request, listener);
        verify(listener, times(1)).onFailure(any());
    }

    public void testFilterEnabled() {
        settings = Settings.builder().put(FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        clusterService = new ClusterService(settings, clusterSettings, null);

        searchHandler = new ADSearchHandler(settings, clusterService, client);
        searchHandler.search(matchAllRequest(), listener);
        verify(client, times(1)).search(any(), any());
    }
}
