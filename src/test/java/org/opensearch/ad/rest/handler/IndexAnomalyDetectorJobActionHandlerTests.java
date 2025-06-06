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
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.action.DocWriteResponse.Result.CREATED;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;

import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.mock.model.MockSimpleLog;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultResponse;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.TransportService;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

public class IndexAnomalyDetectorJobActionHandlerTests extends OpenSearchTestCase {

    private static ADIndexManagement anomalyDetectionIndices;
    private static String detectorId;

    private static NamedXContentRegistry xContentRegistry;
    private static DiscoveryNodeFilterer nodeFilter;
    private static AnomalyDetector detector;

    private ADTaskManager adTaskManager;

    private ThreadPool threadPool;

    private ExecuteADResultResponseRecorder recorder;
    private Client client;
    private ADIndexJobActionHandler handler;
    private ResultBulkIndexingHandler<AnomalyResult, ADIndex, ADIndexManagement> anomalyResultHandler;
    private NodeStateManager nodeStateManager;
    private TransportService transportService;
    private Clock clock;

    @BeforeClass
    public static void setOnce() throws IOException {
        detectorId = "123";
        anomalyDetectionIndices = mock(ADIndexManagement.class);
        xContentRegistry = NamedXContentRegistry.EMPTY;
        when(anomalyDetectionIndices.doesJobIndexExist()).thenReturn(true);
        // make sure getAndExecuteOnLatestConfigLevelTask called in startConfig
        when(anomalyDetectionIndices.doesStateIndexExist()).thenReturn(true);

        nodeFilter = mock(DiscoveryNodeFilterer.class);
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

            AnomalyResultResponse response = new AnomalyResultResponse(null, "", 0L, 10L, true, null);
            listener.onResponse(response);

            return null;
        }).when(client).execute(any(AnomalyResultAction.class), any(), any());

        adTaskManager = mock(ADTaskManager.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<JobResponse> listener = (ActionListener<JobResponse>) args[5];

            JobResponse response = mock(JobResponse.class);
            listener.onResponse(response);

            return null;
        }).when(adTaskManager).getAndExecuteOnLatestConfigLevelTask(any(), any(), eq(false), any(), any(), any());

        threadPool = mock(ThreadPool.class);

        anomalyResultHandler = mock(ResultBulkIndexingHandler.class);

        nodeStateManager = mock(NodeStateManager.class);

        clock = mock(Clock.class);

        recorder = new ExecuteADResultResponseRecorder(
            anomalyDetectionIndices,
            anomalyResultHandler,
            adTaskManager,
            nodeFilter,
            threadPool,
            client,
            nodeStateManager,
            clock,
            32
        );

        handler = new ADIndexJobActionHandler(
            client,
            anomalyDetectionIndices,
            xContentRegistry,
            adTaskManager,
            recorder,
            nodeStateManager,
            Settings.EMPTY
        );

        transportService = mock(TransportService.class);

        // Since we modify detector in testIndexException, it is better to recreate the detector for each test
        // instead of doing it in setUpOnce.
        // Otherwise, we may have race condition: if testIndexException executes first, other tests will get
        // its recreated detector; otherwise, other tests get detector defined in setUpOnce.
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList("a"));
    }

    @SuppressWarnings("unchecked")
    public void testDelayHCProfile() {
        when(adTaskManager.isRealtimeTaskStartInitializing(anyString())).thenReturn(false);

        ActionListener<JobResponse> listener = mock(ActionListener.class);

        handler.startJob(detector, transportService, clock, listener);

        verify(client, times(1)).get(any(), any());
        verify(client, times(1)).execute(any(), any(), any());
        verify(adTaskManager, times(1)).getAndExecuteOnLatestConfigLevelTask(any(), any(), eq(false), any(), any(), any());
        verify(adTaskManager, times(1)).isRealtimeTaskStartInitializing(anyString());
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
        }).when(client).execute(any(ADProfileAction.class), any(), any());

        when(adTaskManager.isRealtimeTaskStartInitializing(anyString())).thenReturn(true);

        ActionListener<JobResponse> listener = mock(ActionListener.class);

        handler.startJob(detector, transportService, clock, listener);

        verify(client, times(1)).get(any(), any());
        verify(client, times(2)).execute(any(), any(), any());
        verify(adTaskManager, times(1)).getAndExecuteOnLatestConfigLevelTask(any(), any(), eq(false), any(), any(), any());
        verify(adTaskManager, times(1)).isRealtimeTaskStartInitializing(anyString());
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
        }).when(client).execute(any(ADProfileAction.class), any(), any());

        when(adTaskManager.isRealtimeTaskStartInitializing(anyString())).thenReturn(true);

        ActionListener<JobResponse> listener = mock(ActionListener.class);

        handler.startJob(detector, transportService, clock, listener);

        verify(client, times(1)).get(any(), any());
        verify(client, times(2)).execute(any(), any(), any());
        verify(adTaskManager, times(1)).getAndExecuteOnLatestConfigLevelTask(any(), any(), eq(false), any(), any(), any());
        verify(adTaskManager, times(1)).isRealtimeTaskStartInitializing(anyString());
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
        }).when(client).execute(any(ADProfileAction.class), any(), any());

        when(adTaskManager.isRealtimeTaskStartInitializing(anyString())).thenReturn(true);

        doAnswer(invocation -> {
            ActionListener<UpdateResponse> cb = (ActionListener<UpdateResponse>) invocation.getArgument(6);
            cb.onFailure(new ResourceNotFoundException(CommonMessages.CAN_NOT_FIND_LATEST_TASK));
            return null;
        })
            .when(adTaskManager)
            .updateLatestRealtimeTaskOnCoordinatingNode(
                eq("123"),                // we know the configId
                isNull(),                 // taskState must be null
                eq(3L),                   // rcfTotalUpdates
                eq(10L),                  // configIntervalInMinutes
                eq(""),                   // error
                isNull(),                 // hasRecentResult must be null
                any(ActionListener.class) // listener
            );

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Optional<AnomalyDetector>> listener = (ActionListener<Optional<AnomalyDetector>>) args[3];
            listener.onResponse(Optional.of(detector));

            return null;
        }).when(nodeStateManager).getConfig(any(), any(), anyBoolean(), any());

        ActionListener<JobResponse> listener = mock(ActionListener.class);

        handler.startJob(detector, transportService, clock, listener);

        verify(client, times(1)).get(any(), any());
        verify(client, times(2)).execute(any(), any(), any());
        verify(nodeStateManager, times(1)).getConfig(any(), any(), anyBoolean(), any());
        verify(adTaskManager, times(1))
            .updateLatestRealtimeTaskOnCoordinatingNode(
                eq("123"),                // we know the configId
                isNull(),                 // taskState must be null
                eq(3L),                   // rcfTotalUpdates
                eq(10L),                  // configIntervalInMinutes
                eq(""),                   // error
                isNull(),                 // hasRecentResult must be null
                any(ActionListener.class) // listener
            );
        verify(adTaskManager, times(1)).getAndExecuteOnLatestConfigLevelTask(any(), any(), eq(false), any(), any(), any());
        verify(adTaskManager, times(1)).isRealtimeTaskStartInitializing(anyString());
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
        }).when(client).execute(any(ADProfileAction.class), any(), any());

        when(adTaskManager.isRealtimeTaskStartInitializing(anyString())).thenReturn(true);

        ActionListener<JobResponse> listener = mock(ActionListener.class);

        handler.startJob(detector, transportService, clock, listener);

        verify(client, times(1)).get(any(), any());
        verify(client, times(2)).execute(any(), any(), any());
        verify(adTaskManager, times(1)).getAndExecuteOnLatestConfigLevelTask(any(), any(), eq(false), any(), any(), any());
        verify(adTaskManager, times(1)).isRealtimeTaskStartInitializing(anyString());
        verify(adTaskManager, never()).removeRealtimeTaskCache(anyString());
        verify(adTaskManager, times(1)).skipUpdateRealtimeTask(anyString(), anyString());
        verify(threadPool, never()).schedule(any(), any(), any());
        verify(listener, times(1)).onResponse(any());
    }

    @SuppressWarnings("unchecked")
    public void testIndexException() throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<AnomalyResultResponse> listener = (ActionListener<AnomalyResultResponse>) args[2];

            listener.onFailure(new InternalFailure(detectorId, ADCommonMessages.NO_MODEL_ERR_MSG));

            return null;
        }).when(client).execute(any(AnomalyResultAction.class), any(), any());

        ActionListener<JobResponse> listener = mock(ActionListener.class);
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
                ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "index"
            );
        when(anomalyDetectionIndices.doesIndexExist(anyString())).thenReturn(false);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Optional<AnomalyDetector>> listener2 = (ActionListener<Optional<AnomalyDetector>>) args[3];
            listener2.onResponse(Optional.of(detector));

            return null;
        }).when(nodeStateManager).getConfig(any(), any(), anyBoolean(), any());
        handler.startJob(detector, transportService, clock, listener);
        verify(anomalyResultHandler, times(1)).index(any(), any(), eq(ADCommonName.CUSTOM_RESULT_INDEX_PREFIX + "index"));
        // we only schedule delayed update when there is no index exception (recorder.indexResultException in startJob
        // method won't schedule delayed update)
        verify(threadPool, never()).schedule(any(), any(), any());
    }
}
