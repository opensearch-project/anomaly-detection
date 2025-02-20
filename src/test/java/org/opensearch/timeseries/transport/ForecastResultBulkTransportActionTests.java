/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.timeseries.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastResultWriteRequest;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.transport.ForecastResultBulkRequest;
import org.opensearch.forecast.transport.ForecastResultBulkTransportAction;
import org.opensearch.index.IndexingPressure;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class ForecastResultBulkTransportActionTests extends AbstractTimeSeriesTest {

    private ForecastResultBulkTransportAction resultBulk;
    private TransportService transportService;
    private ClusterService clusterService;
    private IndexingPressure indexingPressure;
    private Client client;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(ForecastResultBulkTransportActionTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        Settings settings = Settings
            .builder()
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), "1KB")
            .put("forecast.index_pressure.soft_limit", 0.8)
            .build();

        // Setup test nodes and services
        setupTestNodes(ForecastSettings.FORECAST_INDEX_PRESSURE_SOFT_LIMIT, ForecastSettings.FORECAST_INDEX_PRESSURE_HARD_LIMIT);
        transportService = testNodes[0].transportService;
        clusterService = testNodes[0].clusterService;

        ActionFilters actionFilters = mock(ActionFilters.class);
        indexingPressure = mock(IndexingPressure.class);

        client = mock(Client.class);

        resultBulk = new ForecastResultBulkTransportAction(
            transportService,
            actionFilters,
            indexingPressure,
            settings,
            clusterService,
            client
        );
    }

    @Override
    @After
    public final void tearDown() throws Exception {
        tearDownTestNodes();
        super.tearDown();
    }

    @SuppressWarnings("unchecked")
    public void testBulkIndexingFailure() throws IOException {
        // Set indexing pressure below soft limit to ensure requests are processed
        when(indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes()).thenReturn(0L);
        when(indexingPressure.getCurrentReplicaBytes()).thenReturn(0L);

        // Create a ForecastResultBulkRequest with some results
        ForecastResultBulkRequest originalRequest = new ForecastResultBulkRequest();
        originalRequest.add(TestHelpers.randomForecastResultWriteRequest());
        originalRequest.add(TestHelpers.randomForecastResultWriteRequest());

        // Mock client.execute to throw an exception
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) args[2];
            listener.onFailure(new RuntimeException("Simulated bulk indexing failure"));
            return null;
        }).when(client).execute(any(), any(), any());

        // Execute the action
        PlainActionFuture<ResultBulkResponse> future = PlainActionFuture.newFuture();
        resultBulk.doExecute(null, originalRequest, future);

        // Verify that the exception is propagated to the listener
        Exception exception = expectThrows(Exception.class, () -> future.actionGet());
        assertTrue(exception.getMessage().contains("Simulated bulk indexing failure"));
    }

    public void testPrepareBulkRequestFailure() throws IOException {
        // Set indexing pressure below soft limit to ensure requests are processed
        when(indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes()).thenReturn(0L);
        when(indexingPressure.getCurrentReplicaBytes()).thenReturn(0L);

        // Create a ForecastResultWriteRequest with a result that throws IOException when toXContent is called
        ForecastResultWriteRequest faultyWriteRequest = mock(ForecastResultWriteRequest.class);
        ForecastResult faultyResult = mock(ForecastResult.class);

        when(faultyWriteRequest.getResult()).thenReturn(faultyResult);
        when(faultyWriteRequest.getResultIndex()).thenReturn(null);

        // Mock the toXContent method to throw IOException
        doThrow(new IOException("Simulated IOException in toXContent")).when(faultyResult).toXContent(any(XContentBuilder.class), any());

        // Create a ForecastResultBulkRequest with the faulty write request
        ForecastResultBulkRequest originalRequest = new ForecastResultBulkRequest();
        originalRequest.add(faultyWriteRequest);

        // Execute the prepareBulkRequest method directly
        BulkRequest bulkRequest = resultBulk.prepareBulkRequest(0.5f, originalRequest);

        // Since the exception is caught inside addResult, bulkRequest should have zero actions
        assertEquals(0, bulkRequest.numberOfActions());
    }
}
