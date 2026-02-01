/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.Collections;

import org.junit.Before;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.rest.handler.InsightsJobActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.transport.InsightsJobRequest;
import org.opensearch.timeseries.util.PluginClient;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class InsightsJobTransportActionTests extends OpenSearchTestCase {

    private TransportService transportService;
    private Client client;
    private InsightsJobActionHandler jobHandler;
    private InsightsJobTransportAction transportAction;

    @Before
    public void setUpTransportAction() throws Exception {
        transportService = mock(TransportService.class);
        client = mock(Client.class);
        PluginClient pluginClient = mock(PluginClient.class);
        ClusterService clusterService = mock(ClusterService.class);
        ADIndexManagement indexManagement = mock(ADIndexManagement.class);

        Settings settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.AD_REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(30))
            .build();

        transportAction = new InsightsJobTransportAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            client,
            pluginClient,
            clusterService,
            settings,
            NamedXContentRegistry.EMPTY,
            indexManagement
        );

        jobHandler = mock(InsightsJobActionHandler.class);
        Field handlerField = InsightsJobTransportAction.class.getDeclaredField("jobHandler");
        handlerField.setAccessible(true);
        handlerField.set(transportAction, jobHandler);
    }

    public void testStartOperationDelegatesToHandler() throws Exception {
        PlainActionFuture<InsightsJobResponse> future = PlainActionFuture.newFuture();
        InsightsJobRequest request = new InsightsJobRequest("12h", "/_plugins/_anomaly_detection/insights/_start");

        doAnswer(invocation -> {
            ActionListener<InsightsJobResponse> listener = invocation.getArgument(1);
            listener.onResponse(new InsightsJobResponse("started"));
            return null;
        }).when(jobHandler).startInsightsJob(eq("12h"), any());

        transportAction.doExecute((Task) null, request, future);
        assertEquals("started", future.actionGet().getMessage());
        verify(jobHandler, times(1)).startInsightsJob(eq("12h"), any());
    }

    public void testStopOperationDelegatesToHandler() throws Exception {
        PlainActionFuture<InsightsJobResponse> future = PlainActionFuture.newFuture();
        InsightsJobRequest request = new InsightsJobRequest("/_plugins/_anomaly_detection/insights/_stop");

        doAnswer(invocation -> {
            ActionListener<InsightsJobResponse> listener = invocation.getArgument(0);
            listener.onResponse(new InsightsJobResponse("stopped"));
            return null;
        }).when(jobHandler).stopInsightsJob(any());

        transportAction.doExecute((Task) null, request, future);
        assertEquals("stopped", future.actionGet().getMessage());
        verify(jobHandler, times(1)).stopInsightsJob(any());
    }

    public void testUnknownOperationFails() {
        PlainActionFuture<InsightsJobResponse> future = PlainActionFuture.newFuture();
        InsightsJobRequest request = new InsightsJobRequest("12h", "/_plugins/_anomaly_detection/insights/unsupported");

        transportAction.doExecute((Task) null, request, future);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, future::actionGet);
        assertEquals("Unknown operation", exception.getMessage());
    }
}
