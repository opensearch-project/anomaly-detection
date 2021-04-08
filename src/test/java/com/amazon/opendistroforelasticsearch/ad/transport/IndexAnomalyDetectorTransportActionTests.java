/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.rest.RestRequest;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import com.amazon.opendistroforelasticsearch.commons.ConfigConstants;

public class IndexAnomalyDetectorTransportActionTests extends OpenSearchIntegTestCase {
    private IndexAnomalyDetectorTransportAction action;
    private Task task;
    private IndexAnomalyDetectorRequest request;
    private ActionListener<IndexAnomalyDetectorResponse> response;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private ADTaskManager adTaskManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        adTaskManager = mock(ADTaskManager.class);
        action = new IndexAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client(),
            clusterService,
            indexSettings(),
            mock(AnomalyDetectionIndices.class),
            xContentRegistry(),
            adTaskManager
        );
        task = mock(Task.class);
        request = new IndexAnomalyDetectorRequest(
            "1234",
            4567,
            7890,
            WriteRequest.RefreshPolicy.IMMEDIATE,
            mock(AnomalyDetector.class),
            RestRequest.Method.PUT,
            TimeValue.timeValueSeconds(60),
            1000,
            10,
            5
        );
        response = new ActionListener<IndexAnomalyDetectorResponse>() {
            @Override
            public void onResponse(IndexAnomalyDetectorResponse indexResponse) {
                // onResponse will not be called as we do not have the AD index
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testIndexTransportAction() {
        action.doExecute(task, request, response);
    }

    @Test
    public void testIndexTransportActionWithUserAndFilterOn() {
        Settings settings = Settings.builder().put(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.getKey(), true).build();
        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        Client client = mock(Client.class);
        org.opensearch.threadpool.ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

        IndexAnomalyDetectorTransportAction transportAction = new IndexAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clusterService,
            settings,
            mock(AnomalyDetectionIndices.class),
            xContentRegistry(),
            adTaskManager
        );
        transportAction.doExecute(task, request, response);
    }

    @Test
    public void testIndexTransportActionWithUserAndFilterOff() {
        Settings settings = Settings.builder().build();
        ThreadContext threadContext = new ThreadContext(settings);
        threadContext.putTransient(ConfigConstants.OPENDISTRO_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        Client client = mock(Client.class);
        org.opensearch.threadpool.ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

        IndexAnomalyDetectorTransportAction transportAction = new IndexAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clusterService,
            settings,
            mock(AnomalyDetectionIndices.class),
            xContentRegistry(),
            adTaskManager
        );
        transportAction.doExecute(task, request, response);
    }

    @Test
    public void testIndexDetectorAction() {
        Assert.assertNotNull(IndexAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(IndexAnomalyDetectorAction.INSTANCE.name(), IndexAnomalyDetectorAction.NAME);
    }
}
