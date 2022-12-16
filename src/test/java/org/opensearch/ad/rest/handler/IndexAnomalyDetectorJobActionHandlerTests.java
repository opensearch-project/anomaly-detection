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

package org.opensearch.ad.rest.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.ad.constant.CommonErrorMessages.CAN_NOT_FIND_LATEST_TASK;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.mock.model.MockSimpleLog;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.AnomalyDetectorJobResponse;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultResponse;
import org.opensearch.ad.transport.ProfileAction;
import org.opensearch.ad.transport.ProfileResponse;
import org.opensearch.ad.transport.handler.AnomalyIndexHandler;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;

public class IndexAnomalyDetectorJobActionHandlerTests extends OpenSearchTestCase {

    private static AnomalyDetectionIndices anomalyDetectionIndices;
    private static String detectorId;
    private static Long seqNo;
    private static Long primaryTerm;

    private static NamedXContentRegistry xContentRegistry;
    private static TransportService transportService;
    private static TimeValue requestTimeout;
    private static DiscoveryNodeFilterer nodeFilter;
    private static AnomalyDetector detector;

    private ADTaskManager adTaskManager;

    private ThreadPool threadPool;

    private ExecuteADResultResponseRecorder recorder;
    private Client client;
    private IndexAnomalyDetectorJobActionHandler handler;
    private AnomalyIndexHandler<AnomalyResult> anomalyResultHandler;

    @BeforeClass
    public static void setOnce() throws IOException {
        detectorId = "123";
        seqNo = 1L;
        primaryTerm = 2L;
        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        xContentRegistry = NamedXContentRegistry.EMPTY;
        transportService = mock(TransportService.class);

        requestTimeout = TimeValue.timeValueMinutes(60);
        when(anomalyDetectionIndices.doesAnomalyDetectorJobIndexExist()).thenReturn(true);

        nodeFilter = mock(DiscoveryNodeFilterer.class);
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a"));
    }

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            GetResponse response = mock(GetResponse.class);
            when(response.isExists()).thenReturn(false);
            listener.onResponse(response);

            return null;
        }).when(client).get(any(GetRequest.class), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<IndexResponse> listener = (ActionListener<IndexResponse>) args[1];

            IndexResponse response = mock(IndexResponse.class);
            when(response.getResult()).thenReturn(CREATED);
            listener.onResponse(response);

            return null;
        }).when(client).index(any(IndexRequest.class), any());

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<AnomalyResultResponse> listener = (ActionListener<AnomalyResultResponse>) args[2];

            AnomalyResultResponse response = new AnomalyResultResponse(null, "", 0L, 10L, true);
            listener.onResponse(response);

            return null;
        }).when(client).execute(any(AnomalyResultAction.class), any(), any());

        adTaskManager = mock(ADTaskManager.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<AnomalyDetectorJobResponse> listener = (ActionListener<AnomalyDetectorJobResponse>) args[4];

            AnomalyDetectorJobResponse response = mock(AnomalyDetectorJobResponse.class);
            listener.onResponse(response);

            return null;
        }).when(adTaskManager).startDetector(any(), any(), any(), any(), any());

        threadPool = mock(ThreadPool.class);

        anomalyResultHandler = mock(AnomalyIndexHandler.class);

        recorder = new ExecuteADResultResponseRecorder(
            anomalyDetectionIndices,
            anomalyResultHandler,
            adTaskManager,
            nodeFilter,
            threadPool,
            client
        );

        handler = new IndexAnomalyDetectorJobActionHandler(
            client,
            anomalyDetectionIndices,
            detectorId,
            seqNo,
            primaryTerm,
            requestTimeout,
            xContentRegistry,
            transportService,
            adTaskManager,
            recorder
        );
    }

    @SuppressWarnings("unchecked")
    public void testDelayHCProfile() {
        when(adTaskManager.isHCRealtimeTaskStartInitializing(anyString())).thenReturn(false);

        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);

        handler.startAnomalyDetectorJob(detector, listener);

        verify(client, times(1)).get(any(), any());
        verify(client, times(1)).execute(any(), any(), any());
        verify(adTaskManager, times(1)).startDetector(any(), any(), any(), any(), any());
        verify(adTaskManager, times(1)).isHCRealtimeTaskStartInitializing(anyString());
        verify(threadPool, times(1)).schedule(any(), any(), any());
        verify(listener, times(1)).onResponse(any());
    }

    @SuppressWarnings("unchecked")
    public void testNoDelayHCProfile() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<ProfileResponse> listener = (ActionListener<ProfileResponse>) args[2];

            ProfileResponse response = mock(ProfileResponse.class);
            when(response.getTotalUpdates()).thenReturn(3L);
            listener.onResponse(response);

            return null;
        }).when(client).execute(any(ProfileAction.class), any(), any());

        when(adTaskManager.isHCRealtimeTaskStartInitializing(anyString())).thenReturn(true);

        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);

        handler.startAnomalyDetectorJob(detector, listener);

        verify(client, times(1)).get(any(), any());
        verify(client, times(2)).execute(any(), any(), any());
        verify(adTaskManager, times(1)).startDetector(any(), any(), any(), any(), any());
        verify(adTaskManager, times(1)).isHCRealtimeTaskStartInitializing(anyString());
        verify(adTaskManager, times(1)).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(listener, times(1)).onResponse(any());
    }

    @SuppressWarnings("unchecked")
    public void testHCProfileException() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<ProfileResponse> listener = (ActionListener<ProfileResponse>) args[2];

            listener.onFailure(new RuntimeException());

            return null;
        }).when(client).execute(any(ProfileAction.class), any(), any());

        when(adTaskManager.isHCRealtimeTaskStartInitializing(anyString())).thenReturn(true);

        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);

        handler.startAnomalyDetectorJob(detector, listener);

        verify(client, times(1)).get(any(), any());
        verify(client, times(2)).execute(any(), any(), any());
        verify(adTaskManager, times(1)).startDetector(any(), any(), any(), any(), any());
        verify(adTaskManager, times(1)).isHCRealtimeTaskStartInitializing(anyString());
        verify(adTaskManager, never()).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(listener, times(1)).onResponse(any());
    }

    @SuppressWarnings("unchecked")
    public void testUpdateLatestRealtimeTaskOnCoordinatingNodeResourceNotFoundException() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<ProfileResponse> listener = (ActionListener<ProfileResponse>) args[2];

            ProfileResponse response = mock(ProfileResponse.class);
            when(response.getTotalUpdates()).thenReturn(3L);
            listener.onResponse(response);

            return null;
        }).when(client).execute(any(ProfileAction.class), any(), any());

        when(adTaskManager.isHCRealtimeTaskStartInitializing(anyString())).thenReturn(true);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<UpdateResponse> listener = (ActionListener<UpdateResponse>) args[5];

            listener.onFailure(new ResourceNotFoundException(CAN_NOT_FIND_LATEST_TASK));

            return null;
        }).when(adTaskManager).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());

        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);

        handler.startAnomalyDetectorJob(detector, listener);

        verify(client, times(1)).get(any(), any());
        verify(client, times(2)).execute(any(), any(), any());
        verify(adTaskManager, times(1)).startDetector(any(), any(), any(), any(), any());
        verify(adTaskManager, times(1)).isHCRealtimeTaskStartInitializing(anyString());
        verify(adTaskManager, times(1)).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
        verify(adTaskManager, times(1)).removeRealtimeTaskCache(anyString());
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(listener, times(1)).onResponse(any());
    }

    @SuppressWarnings("unchecked")
    public void testUpdateLatestRealtimeTaskOnCoordinatingException() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<ProfileResponse> listener = (ActionListener<ProfileResponse>) args[2];

            ProfileResponse response = mock(ProfileResponse.class);
            when(response.getTotalUpdates()).thenReturn(3L);
            listener.onResponse(response);

            return null;
        }).when(client).execute(any(ProfileAction.class), any(), any());

        when(adTaskManager.isHCRealtimeTaskStartInitializing(anyString())).thenReturn(true);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<UpdateResponse> listener = (ActionListener<UpdateResponse>) args[5];

            listener.onFailure(new RuntimeException());

            return null;
        }).when(adTaskManager).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());

        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);

        handler.startAnomalyDetectorJob(detector, listener);

        verify(client, times(1)).get(any(), any());
        verify(client, times(2)).execute(any(), any(), any());
        verify(adTaskManager, times(1)).startDetector(any(), any(), any(), any(), any());
        verify(adTaskManager, times(1)).isHCRealtimeTaskStartInitializing(anyString());
        verify(adTaskManager, times(1)).updateLatestRealtimeTaskOnCoordinatingNode(any(), any(), any(), any(), any(), any());
        verify(adTaskManager, never()).removeRealtimeTaskCache(anyString());
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(listener, times(1)).onResponse(any());
    }

    @SuppressWarnings("unchecked")
    public void testIndexException() throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<AnomalyResultResponse> listener = (ActionListener<AnomalyResultResponse>) args[2];

            listener.onFailure(new ResourceNotFoundException(detectorId, CommonErrorMessages.NO_CHECKPOINT_ERR_MSG));

            return null;
        }).when(client).execute(any(AnomalyResultAction.class), any(), any());

        ActionListener<AnomalyDetectorJobResponse> listener = mock(ActionListener.class);
        AggregationBuilder aggregationBuilder = TestHelpers
            .parseAggregation("{\"test\":{\"max\":{\"field\":\"" + MockSimpleLog.VALUE_FIELD + "\"}}}");
        Feature feature = new Feature(randomAlphaOfLength(5), randomAlphaOfLength(10), true, aggregationBuilder);
        detector = TestHelpers
            .randomDetector(
                ImmutableList.of(feature),
                "test",
                10,
                MockSimpleLog.TIME_FIELD,
                null,
                CommonName.CUSTOM_RESULT_INDEX_PREFIX + "index"
            );
        when(anomalyDetectionIndices.doesIndexExist(anyString())).thenReturn(false);
        handler.startAnomalyDetectorJob(detector, listener);
        verify(anomalyResultHandler, times(1)).index(any(), any(), eq(null));
        verify(threadPool, times(1)).schedule(any(), any(), any());
    }
}
