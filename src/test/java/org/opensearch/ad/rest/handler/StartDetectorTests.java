/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.rest.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.constant.ADCommonName.ANOMALY_RESULT_INDEX_ALIAS;
import static org.opensearch.ad.constant.ADCommonName.DETECTION_STATE_INDEX;
import static org.opensearch.timeseries.TestHelpers.randomAnomalyDetector;
import static org.opensearch.timeseries.TestHelpers.randomDetectionDateRange;
import static org.opensearch.timeseries.TestHelpers.randomDetector;
import static org.opensearch.timeseries.TestHelpers.randomFeature;
import static org.opensearch.timeseries.TestHelpers.randomUser;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.junit.BeforeClass;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import com.google.common.collect.ImmutableList;

public class StartDetectorTests extends AbstractTimeSeriesTest {
    private static ADIndexManagement anomalyDetectionIndices;
    private static NamedXContentRegistry xContentRegistry;

    private DateRange detectionDateRange;
    private ADIndexJobActionHandler handler;
    private Client client;
    private ADTaskManager adTaskManager;
    private ExecuteADResultResponseRecorder recorder;
    private NodeStateManager nodeStateManager;
    private ActionListener<JobResponse> listener;
    private TransportService transportService;

    @BeforeClass
    public static void setOnce() throws IOException {
        anomalyDetectionIndices = mock(ADIndexManagement.class);
        xContentRegistry = NamedXContentRegistry.EMPTY;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Instant now = Instant.now();
        Instant startTime = now.minus(10, ChronoUnit.DAYS);
        Instant endTime = now.minus(1, ChronoUnit.DAYS);
        detectionDateRange = new DateRange(startTime, endTime);

        client = mock(Client.class);

        adTaskManager = mock(ADTaskManager.class);
        recorder = mock(ExecuteADResultResponseRecorder.class);
        nodeStateManager = mock(NodeStateManager.class);

        handler = new ADIndexJobActionHandler(
            client,
            anomalyDetectionIndices,
            xContentRegistry,
            adTaskManager,
            recorder,
            nodeStateManager,
            Settings.EMPTY
        );

        listener = spy(new ActionListener<JobResponse>() {
            @Override
            public void onResponse(JobResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {}
        });

        transportService = mock(TransportService.class);
    }

    public void testCreateTaskIndexWithException() throws IOException {
        String error = randomAlphaOfLength(5);
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException(error));
            return null;
        }).when(anomalyDetectionIndices).initStateIndex(any());
        doReturn(false).when(anomalyDetectionIndices).doesStateIndexExist();
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector, client);

        handler.startConfig(detector, detectionDateRange, randomUser(), transportService, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(error, exceptionCaptor.getValue().getMessage());
    }

    public void testCreateTaskIndexNotAcknowledged() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(false, false, ANOMALY_RESULT_INDEX_ALIAS));
            return null;
        }).when(anomalyDetectionIndices).initStateIndex(any());
        doReturn(false).when(anomalyDetectionIndices).doesStateIndexExist();
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector, client);

        handler.startConfig(detector, detectionDateRange, randomUser(), transportService, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        String error = String.format(Locale.ROOT, CommonMessages.CREATE_INDEX_NOT_ACKNOWLEDGED, DETECTION_STATE_INDEX);
        assertEquals(error, exceptionCaptor.getValue().getMessage());
    }

    public void testCreateTaskIndexWithResourceAlreadyExistsException() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException("index created"));
            return null;
        }).when(anomalyDetectionIndices).initStateIndex(any());
        doReturn(false).when(anomalyDetectionIndices).doesStateIndexExist();
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));
        setupGetDetector(detector, client);

        handler.startConfig(detector, detectionDateRange, randomUser(), transportService, listener);
        verify(listener, never()).onFailure(any());
    }

    @SuppressWarnings("unchecked")
    public void testgetAndExecuteOnLatestConfigLevelTaskWithException() throws IOException {
        AnomalyDetector detector = randomAnomalyDetector(ImmutableList.of(randomFeature(true)));
        DateRange detectionDateRange = randomDetectionDateRange();
        User user = null;
        ActionListener<JobResponse> listener = mock(ActionListener.class);
        when(anomalyDetectionIndices.doesStateIndexExist()).thenReturn(false);
        doThrow(new RuntimeException("test")).when(anomalyDetectionIndices).initStateIndex(any());
        handler.startConfig(detector, detectionDateRange, user, transportService, listener);
        verify(listener, times(1)).onFailure(any());
    }
}
