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

package org.opensearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.TestHelpers.createEmptySearchResponse;

import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.sdk.Extension;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKClusterService.SDKClusterSettings;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;

public class SearchAnomalyDetectorInfoActionTests extends OpenSearchIntegTestCase {
    private SearchAnomalyDetectorInfoRequest request;
    private ActionListener<SearchAnomalyDetectorInfoResponse> response;
    private SearchAnomalyDetectorInfoTransportAction action;
    private Task task;
    private SDKClusterService clusterService;
    private SDKRestClient client;
    private ThreadPool threadPool;
    private ExtensionsRunner mockRunner;
    private SDKClusterSettings clusterSettings;
    private PlainActionFuture<SearchAnomalyDetectorInfoResponse> future;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        action = new SearchAnomalyDetectorInfoTransportAction(mock(TaskManager.class), mock(ActionFilters.class), null, null);
        task = mock(Task.class);
        response = new ActionListener<SearchAnomalyDetectorInfoResponse>() {
            @Override
            public void onResponse(SearchAnomalyDetectorInfoResponse response) {
                Assert.assertEquals(response.getCount(), 0);
                Assert.assertEquals(response.isNameExists(), false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(true);
            }
        };

        future = mock(PlainActionFuture.class);
        client = mock(SDKRestClient.class);
        // when(client.threadPool()).thenReturn(threadPool);
        // threadPool = mock(ThreadPool.class);
        // when(client.threadPool()).thenReturn(threadPool);
        Settings settings = Settings.builder().build();

        clusterService = mock(SDKClusterService.class);
        mockRunner = mock(ExtensionsRunner.class);
        List<Setting<?>> settingsList = List.of(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES);
        clusterService = mock(SDKClusterService.class);
        Extension mockExtension = mock(Extension.class);
        when(mockRunner.getEnvironmentSettings()).thenReturn(settings);
        when(mockRunner.getExtension()).thenReturn(mockExtension);
        when(mockExtension.getSettings()).thenReturn(settingsList);
        SDKClusterSettings clusterSettings = new SDKClusterService(mockRunner).getClusterSettings();
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }

    @Test
    public void testSearchCount() throws IOException {
        // Anomaly Detectors index will not exist, onResponse will be called
        SearchAnomalyDetectorInfoRequest request = new SearchAnomalyDetectorInfoRequest(null, "count");
        action.doExecute(task, request, response);
    }

    @Test
    public void testSearchMatch() throws IOException {
        // Anomaly Detectors index will not exist, onResponse will be called
        SearchAnomalyDetectorInfoRequest request = new SearchAnomalyDetectorInfoRequest("testDetector", "match");
        action.doExecute(task, request, response);
    }

    @Test
    public void testSearchInfoAction() {
        Assert.assertNotNull(SearchAnomalyDetectorInfoAction.INSTANCE.name());
        Assert.assertEquals(SearchAnomalyDetectorInfoAction.INSTANCE.name(), SearchAnomalyDetectorInfoAction.NAME);
    }

    @Test
    public void testSearchInfoRequest() throws IOException {
        SearchAnomalyDetectorInfoRequest request = new SearchAnomalyDetectorInfoRequest("testDetector", "match");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        SearchAnomalyDetectorInfoRequest newRequest = new SearchAnomalyDetectorInfoRequest(input);
        Assert.assertEquals(request.getName(), newRequest.getName());
        Assert.assertEquals(request.getRawPath(), newRequest.getRawPath());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testSearchInfoResponse() throws IOException {
        SearchAnomalyDetectorInfoResponse response = new SearchAnomalyDetectorInfoResponse(1, true);
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        SearchAnomalyDetectorInfoResponse newResponse = new SearchAnomalyDetectorInfoResponse(input);
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

        action = new SearchAnomalyDetectorInfoTransportAction(mock(TaskManager.class), mock(ActionFilters.class), client, clusterService);
        SearchAnomalyDetectorInfoRequest request = new SearchAnomalyDetectorInfoRequest("testDetector", "count");
        action.doExecute(task, request, future);
        verify(future).onResponse(any(SearchAnomalyDetectorInfoResponse.class));
    }

    public void testSearchInfoResponse_MatchSuccessWithEmptyResponse() throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            SearchResponse searchResponse = createEmptySearchResponse();
            listener.onResponse(searchResponse);
            return null;
        }).when(client).search(any(), any());

        action = new SearchAnomalyDetectorInfoTransportAction(mock(TaskManager.class), mock(ActionFilters.class), client, clusterService);
        SearchAnomalyDetectorInfoRequest request = new SearchAnomalyDetectorInfoRequest("testDetector", "match");
        action.doExecute(task, request, future);
        verify(future).onResponse(any(SearchAnomalyDetectorInfoResponse.class));
    }

    public void testSearchInfoResponse_CountRuntimeException() throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<SearchResponse> listener = (ActionListener<SearchResponse>) args[1];
            listener.onFailure(new RuntimeException("searchResponse failed!"));
            return null;
        }).when(client).search(any(), any());
        action = new SearchAnomalyDetectorInfoTransportAction(mock(TaskManager.class), mock(ActionFilters.class), client, clusterService);
        SearchAnomalyDetectorInfoRequest request = new SearchAnomalyDetectorInfoRequest("testDetector", "count");
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
        action = new SearchAnomalyDetectorInfoTransportAction(mock(TaskManager.class), mock(ActionFilters.class), client, clusterService);
        SearchAnomalyDetectorInfoRequest request = new SearchAnomalyDetectorInfoRequest("testDetector", "match");
        action.doExecute(task, request, future);
        verify(future).onFailure(any(RuntimeException.class));
    }
}
