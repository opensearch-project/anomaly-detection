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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;

public class DeleteAnomalyDetectorActionTests extends OpenSearchIntegTestCase {
    private DeleteAnomalyDetectorTransportAction action;
    private ActionListener<DeleteResponse> response;
    private ADTaskManager adTaskManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        adTaskManager = mock(ADTaskManager.class);
        action = new DeleteAnomalyDetectorTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            client(),
            clusterService,
            Settings.EMPTY,
            xContentRegistry(),
            adTaskManager
        );
        response = new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                Assert.assertTrue(true);
            }

            @Override
            public void onFailure(Exception e) {
                Assert.assertTrue(true);
            }
        };
    }

    @Test
    public void testStatsAction() {
        Assert.assertNotNull(DeleteAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(DeleteAnomalyDetectorAction.INSTANCE.name(), DeleteAnomalyDetectorAction.NAME);
    }

    @Test
    public void testDeleteRequest() throws IOException {
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        DeleteAnomalyDetectorRequest newRequest = new DeleteAnomalyDetectorRequest(input);
        Assert.assertEquals(request.getDetectorID(), newRequest.getDetectorID());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testEmptyDeleteRequest() {
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("");
        ActionRequestValidationException exception = request.validate();
        Assert.assertNotNull(exception);
    }

    @Test
    public void testTransportActionWithAdIndex() {
        // DeleteResponse is not called because detector ID will not exist
        createIndex(".opendistro-anomaly-detector-jobs");
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        action.doExecute(mock(Task.class), request, response);
    }

    @Test
    public void testTransportActionWithoutAdIndex() throws IOException {
        // DeleteResponse is not called because detector ID will not exist
        DeleteAnomalyDetectorRequest request = new DeleteAnomalyDetectorRequest("1234");
        action.doExecute(mock(Task.class), request, response);
    }
}
