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

package org.opensearch.timeseries.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.timeseries.TestHelpers.createEmptySearchResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.SearchAnomalyDetectorInfoAction;
import org.opensearch.ad.transport.SearchAnomalyDetectorInfoTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class SearchAnomalyDetectorInfoActionTests extends OpenSearchIntegTestCase {
    private SearchConfigInfoRequest request;
    private ActionListener<SearchConfigInfoResponse> response;
    private SearchAnomalyDetectorInfoTransportAction action;
    private Task task;
    private ClusterService clusterService;
    private Client client;
    private ThreadPool threadPool;
    ThreadContext threadContext;
    private PlainActionFuture<SearchConfigInfoResponse> future;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        action = new SearchAnomalyDetectorInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client(),
            clusterService()
        );
        task = mock(Task.class);
        response = new ActionListener<SearchConfigInfoResponse>() {
            @Override
            public void onResponse(SearchConfigInfoResponse response) {
                Assert.assertEquals(response.getCount(), 0);
                Assert.assertEquals(response.isNameExists(), false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(true);
            }
        };

        future = mock(PlainActionFuture.class);
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        Settings settings = Settings.builder().build();
        threadContext = new ThreadContext(settings);
        when(threadPool.getThreadContext()).thenReturn(threadContext);

        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }

    @Test
    public void testSearchCount() throws IOException {
        // Anomaly Detectors index will not exist, onResponse will be called
        SearchConfigInfoRequest request = new SearchConfigInfoRequest(null, "count");
        action.doExecute(task, request, response);
    }

    @Test
    public void testSearchMatch() throws IOException {
        // Anomaly Detectors index will not exist, onResponse will be called
        SearchConfigInfoRequest request = new SearchConfigInfoRequest("testDetector", "match");
        action.doExecute(task, request, response);
    }

    @Test
    public void testSearchInfoAction() {
        Assert.assertNotNull(SearchAnomalyDetectorInfoAction.INSTANCE.name());
        Assert.assertEquals(SearchAnomalyDetectorInfoAction.INSTANCE.name(), SearchAnomalyDetectorInfoAction.NAME);
    }

    @Test
    public void testSearchInfoRequest() throws IOException {
        SearchConfigInfoRequest request = new SearchConfigInfoRequest("testDetector", "match");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        SearchConfigInfoRequest newRequest = new SearchConfigInfoRequest(input);
        Assert.assertEquals(request.getName(), newRequest.getName());
        Assert.assertEquals(request.getRawPath(), newRequest.getRawPath());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testSearchInfoResponse() throws IOException {
        SearchConfigInfoResponse response = new SearchConfigInfoResponse(1, true);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        SearchConfigInfoResponse newResponse = new SearchConfigInfoResponse(input);
        Assert.assertEquals(response.getCount(), newResponse.getCount());
        Assert.assertEquals(response.isNameExists(), newResponse.isNameExists());
        Assert.assertNotNull(response.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
    }

    public void testSearchInfoResponse_CountSuccessWithEmptyResponse() throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            SearchResponse searchResponse = createEmptySearchResponse();
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());

        action = new SearchAnomalyDetectorInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clusterService
        );
        SearchConfigInfoRequest request = new SearchConfigInfoRequest("testDetector", "count");
        action.doExecute(task, request, future);
        verify(future).onResponse(any(SearchConfigInfoResponse.class));
    }

    public void testSearchInfoResponse_MatchSuccessWithEmptyResponse() throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            SearchResponse searchResponse = createEmptySearchResponse();
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());

        action = new SearchAnomalyDetectorInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clusterService
        );
        SearchConfigInfoRequest request = new SearchConfigInfoRequest("testDetector", "match");
        action.doExecute(task, request, future);
        verify(future).onResponse(any(SearchConfigInfoResponse.class));
    }

    public void testSearchInfoResponse_CountRuntimeException() throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            listener.onFailure(new RuntimeException("searchResponse failed!"));
            return null;
        }).when(client).search(any(), any());
        action = new SearchAnomalyDetectorInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clusterService
        );
        SearchConfigInfoRequest request = new SearchConfigInfoRequest("testDetector", "count");
        action.doExecute(task, request, future);
        verify(future).onFailure(any(RuntimeException.class));
    }

    public void testSearchInfoResponse_MatchRuntimeException() throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            listener.onFailure(new RuntimeException("searchResponse failed!"));
            return null;
        }).when(client).search(any(), any());
        action = new SearchAnomalyDetectorInfoTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clusterService
        );
        SearchConfigInfoRequest request = new SearchConfigInfoRequest("testDetector", "match");
        action.doExecute(task, request, future);
        verify(future).onFailure(any(RuntimeException.class));
    }
}
