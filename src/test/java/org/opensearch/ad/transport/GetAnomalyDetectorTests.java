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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.get.GetResult;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

public class GetAnomalyDetectorTests extends AbstractTimeSeriesTest {
    private GetAnomalyDetectorTransportAction action;
    private TransportService transportService;
    private DiscoveryNodeFilterer nodeFilter;
    private ActionFilters actionFilters;
    private Client client;
    private SecurityClientUtil clientUtil;
    private GetAnomalyDetectorRequest request;
    private String detectorId = "yecrdnUBqurvo9uKU_d8";
    private String entityValue = "app_0";
    private String categoryField = "categoryField";
    private String typeStr;
    private String rawPath;
    private PlainActionFuture<GetAnomalyDetectorResponse> future;
    private ADTaskManager adTaskManager;
    private Entity entity;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(EntityProfileTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );

        nodeFilter = mock(DiscoveryNodeFilterer.class);

        actionFilters = mock(ActionFilters.class);

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, Settings.EMPTY);

        adTaskManager = mock(ADTaskManager.class);

        action = new GetAnomalyDetectorTransportAction(
            transportService,
            nodeFilter,
            actionFilters,
            clusterService,
            client,
            clientUtil,
            Settings.EMPTY,
            xContentRegistry(),
            adTaskManager
        );

        entity = Entity.createSingleAttributeEntity(categoryField, entityValue);
    }

    public void testInvalidRequest() throws IOException {
        typeStr = "entity_info2,init_progress2";

        rawPath = "_opendistro/_anomaly_detection/detectors/T4c3dXUBj-2IZN7itix_/_profile";

        request = new GetAnomalyDetectorRequest(detectorId, 0L, false, false, typeStr, rawPath, false, entity);

        future = new PlainActionFuture<>();
        action.doExecute(null, request, future);
        assertException(future, OpenSearchStatusException.class, CommonMessages.EMPTY_PROFILES_COLLECT);
    }

    @SuppressWarnings("unchecked")
    public void testValidRequest() throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(CommonName.CONFIG_INDEX)) {
                listener.onResponse(null);
            }
            return null;
        }).when(client).get(any(), any());

        typeStr = "entity_info,init_progress";

        rawPath = "_opendistro/_anomaly_detection/detectors/T4c3dXUBj-2IZN7itix_/_profile";

        request = new GetAnomalyDetectorRequest(detectorId, 0L, false, false, typeStr, rawPath, false, entity);

        future = new PlainActionFuture<>();
        action.doExecute(null, request, future);
        assertException(future, OpenSearchStatusException.class, CommonMessages.FAIL_TO_FIND_CONFIG_MSG);
    }

    public void testGetTransportActionWithReturnTask() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            Consumer<List<ADTask>> consumer = (Consumer<List<ADTask>>) args[4];

            consumer.accept(createADTaskList());
            return null;
        })
            .when(adTaskManager)
            .getAndExecuteOnLatestADTasks(
                anyString(),
                eq(null),
                eq(null),
                anyList(),
                any(),
                eq(transportService),
                eq(true),
                anyInt(),
                any()
            );

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) args[1];

            listener.onResponse(createMultiGetResponse());
            return null;
        }).when(client).multiGet(any(), any());

        rawPath = "_opendistro/_anomaly_detection/detectors/T4c3dXUBj-2IZN7itix_";

        request = new GetAnomalyDetectorRequest(detectorId, 0L, false, true, typeStr, rawPath, false, entity);
        future = new PlainActionFuture<>();
        action.getExecute(request, future);

        verify(client).multiGet(any(), any());
    }

    private MultiGetResponse createMultiGetResponse() {
        MultiGetItemResponse[] items = new MultiGetItemResponse[2];
        ByteBuffer[] buffers = new ByteBuffer[0];
        items[0] = new MultiGetItemResponse(
            new GetResponse(
                new GetResult(CommonName.JOB_INDEX, "test_1", 1, 1, 0, true, BytesReference.fromByteBuffers(buffers), null, null)
            ),
            null
        );
        items[1] = new MultiGetItemResponse(
            new GetResponse(
                new GetResult(CommonName.JOB_INDEX, "test_2", 1, 1, 0, true, BytesReference.fromByteBuffers(buffers), null, null)
            ),
            null
        );
        return new MultiGetResponse(items);
    }

    private List<ADTask> createADTaskList() {
        ADTask adTask1 = new ADTask.Builder().taskId("test1").taskType(ADTaskType.REALTIME_SINGLE_ENTITY.name()).build();
        ADTask adTask2 = new ADTask.Builder().taskId("test2").taskType(ADTaskType.REALTIME_SINGLE_ENTITY.name()).build();
        ADTask adTask3 = new ADTask.Builder().taskId("test3").taskType(ADTaskType.REALTIME_HC_DETECTOR.name()).build();
        ADTask adTask4 = new ADTask.Builder().taskId("test4").taskType(ADTaskType.HISTORICAL_HC_DETECTOR.name()).build();
        ADTask adTask5 = new ADTask.Builder().taskId("test5").taskType(ADTaskType.HISTORICAL_SINGLE_ENTITY.name()).build();

        return Arrays.asList(adTask1, adTask2, adTask3, adTask4, adTask5);
    }
}
