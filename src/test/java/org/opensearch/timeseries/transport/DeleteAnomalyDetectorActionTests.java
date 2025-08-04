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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.DeleteAnomalyDetectorAction;
import org.opensearch.ad.transport.DeleteAnomalyDetectorTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.transport.TransportService;

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
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES)))
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
            mock(NodeStateManager.class),
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
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName());
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        DeleteConfigRequest newRequest = new DeleteConfigRequest(input);
        Assert.assertEquals(request.getConfigID(), newRequest.getConfigID());
        Assert.assertNull(newRequest.validate());
    }

    @Test
    public void testEmptyDeleteRequest() {
        DeleteConfigRequest request = new DeleteConfigRequest("", ADIndex.CONFIG.getIndexName());
        ActionRequestValidationException exception = request.validate();
        Assert.assertNotNull(exception);
    }

    @Test
    public void testTransportActionWithAdIndex() {
        // DeleteResponse is not called because detector ID will not exist
        createIndex(".opendistro-anomaly-detector-jobs");
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName());
        action.doExecute(mock(Task.class), request, response);
    }

    @Test
    public void testTransportActionWithoutAdIndex() throws IOException {
        // DeleteResponse is not called because detector ID will not exist
        DeleteConfigRequest request = new DeleteConfigRequest("1234", ADIndex.CONFIG.getIndexName());
        action.doExecute(mock(Task.class), request, response);
    }
}
