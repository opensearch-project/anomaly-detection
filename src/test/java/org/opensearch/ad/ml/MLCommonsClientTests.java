/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import static org.mockito.Mockito.*;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ad.model.DetectorMetadata;
import org.opensearch.ad.model.MLMetricsCorrelationInput;
import org.opensearch.ad.model.MLMetricsCorrelationOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.ml.common.transport.execute.MLExecuteTaskRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

public class MLCommonsClientTests extends OpenSearchTestCase {

    @Mock
    private Client client;

    @Mock
    private NamedXContentRegistry xContentRegistry;

    private MLCommonsClient mlCommonsClient;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);

        // Create client for ML Commons transport layer
        mlCommonsClient = new MLCommonsClient(client, xContentRegistry);

        // In test environment, ML Commons transport is not available.
        // Stub client.execute to fail immediately so the client degrades gracefully.
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<Object> listener = (ActionListener<Object>) invocation.getArgument(2);
            listener.onFailure(new Exception("ml commons not installed"));
            return null;
        }).when(client).execute(any(), any(MLExecuteTaskRequest.class), any());
    }

    public void testConstructor() {
        assertNotNull(mlCommonsClient);

        // Verify client is initialized for transport layer
        MLCommonsClient anotherClient = new MLCommonsClient(client, xContentRegistry);
        assertNotNull(anotherClient);
    }

    public void testExecuteMetricsCorrelationWithEmptyMatrix() {
        // Create empty input
        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        // Execute
        mlCommonsClient.executeMetricsCorrelation(input, listener);

        // Verify empty output is returned
        ArgumentCaptor<MLMetricsCorrelationOutput> outputCaptor = ArgumentCaptor.forClass(MLMetricsCorrelationOutput.class);
        verify(listener, times(1)).onResponse(outputCaptor.capture());

        MLMetricsCorrelationOutput output = outputCaptor.getValue();
        assertNotNull(output);
        assertNotNull(output.getInferenceResults());
        assertEquals(0, output.getInferenceResults().size());
    }

    public void testExecuteMetricsCorrelationWithZeroMetrics() {
        // Create input with 0 metrics
        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        mlCommonsClient.executeMetricsCorrelation(input, listener);

        verify(listener, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    public void testExecuteMetricsCorrelationWithZeroBuckets() {
        // Create input with metrics but 0 buckets
        List<List<Double>> matrix = Arrays.asList(Collections.emptyList(), Collections.emptyList());

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1", "m2"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        mlCommonsClient.executeMetricsCorrelation(input, listener);

        verify(listener, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    public void testExecuteMetricsCorrelationWithoutMLCommons() {
        // Create valid input
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0, 3.0), Arrays.asList(4.0, 5.0, 6.0));

        Map<String, DetectorMetadata> metadata = new HashMap<>();
        metadata.put("detector-1", new DetectorMetadata("detector-1", "Detector 1", Arrays.asList("index-1")));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("detector-1", "detector-2"),
            metadata,
            Instant.now(),
            Instant.now(),
            60000L,
            Arrays.asList(Instant.now(), Instant.now().plusSeconds(60), Instant.now().plusSeconds(120))
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        // Execute - should return empty output since ML Commons is not installed
        mlCommonsClient.executeMetricsCorrelation(input, listener);

        // Verify empty output is returned
        ArgumentCaptor<MLMetricsCorrelationOutput> outputCaptor = ArgumentCaptor.forClass(MLMetricsCorrelationOutput.class);
        verify(listener, times(1)).onResponse(outputCaptor.capture());

        MLMetricsCorrelationOutput output = outputCaptor.getValue();
        assertNotNull(output);
        assertNotNull(output.getInferenceResults());
    }

    public void testExecuteMetricsCorrelationWithLargeMatrix() {
        // Create realistic 125-bucket x 3-metric input
        List<List<Double>> matrix = Arrays.asList(generateTimeSeries(125), generateTimeSeries(125), generateTimeSeries(125));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("detector-1", "detector-2", "detector-3"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        mlCommonsClient.executeMetricsCorrelation(input, listener);

        // Verify listener is called (either success or graceful degradation)
        verify(listener, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    public void testExecuteMetricsCorrelationWithSingleMetric() {
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("single-detector"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        mlCommonsClient.executeMetricsCorrelation(input, listener);

        verify(listener, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    public void testExecuteMetricsCorrelationWithManyMetrics() {
        // Test with 50 metrics
        List<List<Double>> matrix = new java.util.ArrayList<>();
        List<String> metricKeys = new java.util.ArrayList<>();

        for (int i = 0; i < 50; i++) {
            matrix.add(Arrays.asList(1.0, 2.0, 3.0));
            metricKeys.add("detector-" + i);
        }

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            metricKeys,
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        mlCommonsClient.executeMetricsCorrelation(input, listener);

        verify(listener, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    public void testGracefulDegradationWhenMLCommonsUnavailable() {
        // This test verifies the client handles ML Commons unavailability gracefully
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0), Arrays.asList(3.0, 4.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1", "m2"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        // Execute - should not throw exception even without ML Commons
        try {
            mlCommonsClient.executeMetricsCorrelation(input, listener);

            // Verify listener was called
            verify(listener, atLeastOnce()).onResponse(any(MLMetricsCorrelationOutput.class));
        } catch (Exception e) {
            fail("Should not throw exception when ML Commons is unavailable: " + e.getMessage());
        }
    }

    public void testMultipleSequentialCalls() {
        // Test that multiple calls work correctly
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener1 = mock(ActionListener.class);
        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener2 = mock(ActionListener.class);
        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener3 = mock(ActionListener.class);

        // Execute multiple times
        mlCommonsClient.executeMetricsCorrelation(input, listener1);
        mlCommonsClient.executeMetricsCorrelation(input, listener2);
        mlCommonsClient.executeMetricsCorrelation(input, listener3);

        // All should succeed
        verify(listener1, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
        verify(listener2, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
        verify(listener3, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    public void testMLCommonsAvailabilityCheck() {
        // Test that client is initialized for transport layer
        MLCommonsClient testClient = new MLCommonsClient(client, xContentRegistry);
        assertNotNull(testClient);

        // Should work even without ML Commons
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0));
        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        testClient.executeMetricsCorrelation(input, listener);
        verify(listener, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    public void testCreateEmptyOutput() {
        // Test the createEmptyOutput() method indirectly
        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        mlCommonsClient.executeMetricsCorrelation(input, listener);

        ArgumentCaptor<MLMetricsCorrelationOutput> captor = ArgumentCaptor.forClass(MLMetricsCorrelationOutput.class);
        verify(listener, times(1)).onResponse(captor.capture());

        MLMetricsCorrelationOutput output = captor.getValue();
        assertNotNull(output);
        assertNotNull(output.getInferenceResults());
        assertEquals(0, output.getInferenceResults().size());
    }

    public void testBuildMLInputParams() {
        // Test the buildMLInputParams() method indirectly
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0, 3.0), Arrays.asList(4.0, 5.0, 6.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1", "m2"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        mlCommonsClient.executeMetricsCorrelation(input, listener);

        // Should create params with metrics field
        verify(listener, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    public void testExecutionWithComplexMetadata() {
        // Test with complex detector metadata
        Map<String, DetectorMetadata> metadata = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            metadata.put("detector-" + i, new DetectorMetadata("detector-" + i, "Detector " + i, Arrays.asList("index-" + i)));
        }

        List<List<Double>> matrix = new java.util.ArrayList<>();
        List<String> metricKeys = new java.util.ArrayList<>();
        for (int i = 0; i < 10; i++) {
            matrix.add(Arrays.asList(1.0, 2.0, 3.0));
            metricKeys.add("detector-" + i);
        }

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            metricKeys,
            metadata,
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        mlCommonsClient.executeMetricsCorrelation(input, listener);
        verify(listener, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    public void testExecutionWithVariableTimestamps() {
        // Test with different timestamp patterns
        Instant start = Instant.parse("2025-01-01T00:00:00Z");
        List<Instant> timestamps = new java.util.ArrayList<>();

        // Variable intervals
        timestamps.add(start);
        timestamps.add(start.plusSeconds(60));
        timestamps.add(start.plusSeconds(150)); // Different interval
        timestamps.add(start.plusSeconds(210));

        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, 2.0, 3.0, 4.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1"),
            Collections.emptyMap(),
            start,
            start.plusSeconds(240),
            60000L,
            timestamps
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        mlCommonsClient.executeMetricsCorrelation(input, listener);
        verify(listener, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    public void testUnexpectedErrorDuringConversionGracefullyHandled() {
        // Matrix containing a null value to trigger NPE during float conversion
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0, null, 3.0));

        MLMetricsCorrelationInput input = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener = mock(ActionListener.class);

        // Should catch exception and degrade to empty output
        mlCommonsClient.executeMetricsCorrelation(input, listener);
        verify(listener, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    public void testClientReuseability() {
        // Test that the same client can be used multiple times
        List<List<Double>> matrix = Arrays.asList(Arrays.asList(1.0));

        MLMetricsCorrelationInput input1 = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m1"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        MLMetricsCorrelationInput input2 = new MLMetricsCorrelationInput(
            matrix,
            Arrays.asList("m2"),
            Collections.emptyMap(),
            Instant.now(),
            Instant.now(),
            60000L,
            Collections.emptyList()
        );

        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener1 = mock(ActionListener.class);
        @SuppressWarnings("unchecked")
        ActionListener<MLMetricsCorrelationOutput> listener2 = mock(ActionListener.class);

        // Use same client for both
        mlCommonsClient.executeMetricsCorrelation(input1, listener1);
        mlCommonsClient.executeMetricsCorrelation(input2, listener2);

        verify(listener1, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
        verify(listener2, times(1)).onResponse(any(MLMetricsCorrelationOutput.class));
    }

    // Helper method to generate time series data
    private List<Double> generateTimeSeries(int length) {
        List<Double> series = new java.util.ArrayList<>();
        for (int i = 0; i < length; i++) {
            series.add(Math.sin(i * 0.1) * 10 + randomDouble() * 2);
        }
        return series;
    }
}
