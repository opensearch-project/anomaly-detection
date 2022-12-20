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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.ConfigConstants;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class AnomalyDetectorJobActionTests extends OpenSearchIntegTestCase {
    private AnomalyDetectorJobTransportAction action;
    private Task task;
    private AnomalyDetectorJobRequest request;
    private ActionListener<AnomalyDetectorJobResponse> response;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES)))
        );

        Settings build = Settings.builder().build();
        ThreadContext threadContext = new ThreadContext(build);
        threadContext.putTransient(ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT, "alice|odfe,aes|engineering,operations");
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        Client client = mock(Client.class);
        org.opensearch.threadpool.ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(threadContext);

        action = new AnomalyDetectorJobTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client,
            clusterService,
            indexSettings(),
            mock(AnomalyDetectionIndices.class),
            xContentRegistry(),
            mock(ADTaskManager.class),
            mock(ExecuteADResultResponseRecorder.class)
        );
        task = mock(Task.class);
        request = new AnomalyDetectorJobRequest("1234", 4567, 7890, "_start");
        response = new ActionListener<AnomalyDetectorJobResponse>() {
            @Override
            public void onResponse(AnomalyDetectorJobResponse adResponse) {
                // Will not be called as there is no detector
                Assert.assertTrue(false);
            }

            @Override
            public void onFailure(Exception e) {
                // Will not be called as there is no detector
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testStartAdJobTransportAction() {
        action.doExecute(task, request, response);
    }

    @Test
    public void testStopAdJobTransportAction() {
        AnomalyDetectorJobRequest stopRequest = new AnomalyDetectorJobRequest("1234", 4567, 7890, "_stop");
        action.doExecute(task, stopRequest, response);
    }

    @Test
    public void testAdJobAction() {
        Assert.assertNotNull(AnomalyDetectorJobAction.INSTANCE.name());
        Assert.assertEquals(AnomalyDetectorJobAction.INSTANCE.name(), AnomalyDetectorJobAction.NAME);
    }

    @Test
    public void testAdJobRequest() throws IOException {
        DetectionDateRange detectionDateRange = new DetectionDateRange(Instant.MIN, Instant.now());
        request = new AnomalyDetectorJobRequest("1234", detectionDateRange, false, 4567, 7890, "_start");

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        AnomalyDetectorJobRequest newRequest = new AnomalyDetectorJobRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());
    }

    @Test
    public void testAdJobRequest_NullDetectionDateRange() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        AnomalyDetectorJobRequest newRequest = new AnomalyDetectorJobRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());
    }

    @Test
    public void testAdJobResponse() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        AnomalyDetectorJobResponse response = new AnomalyDetectorJobResponse("1234", 45, 67, 890, RestStatus.OK);
        response.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        AnomalyDetectorJobResponse newResponse = new AnomalyDetectorJobResponse(input);
        Assert.assertEquals(response.getId(), newResponse.getId());
    }
}
