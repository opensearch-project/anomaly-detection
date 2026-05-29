/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors.
 */

package org.opensearch.forecast.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.ForecasterRunner;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class PreviewForecasterTransportActionTests extends OpenSearchTestCase {

    private PreviewForecasterTransportAction action;
    private Client client;
    private Task task;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        task = mock(Task.class);
        ForecasterRunner forecasterRunner = new ForecasterRunner(mock(ForecastColdStart.class), mock(FeatureManager.class));
        action = new PreviewForecasterTransportAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            forecasterRunner,
            client,
            NamedXContentRegistry.EMPTY
        );
    }

    @Test
    public void testMissingForecasterIdentityReturnsBadRequest() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        PreviewForecasterRequest request = new PreviewForecasterRequest(null, null, Instant.ofEpochMilli(1), Instant.ofEpochMilli(2));

        action.doExecute(task, request, new ActionListener<PreviewForecasterResponse>() {
            @Override
            public void onResponse(PreviewForecasterResponse response) {
                fail("Expected failure for missing forecaster identity");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof OpenSearchStatusException);
                OpenSearchStatusException statusException = (OpenSearchStatusException) e;
                assertEquals(RestStatus.BAD_REQUEST, statusException.status());
                assertEquals(PreviewForecasterTransportAction.MISSING_REQUEST_MESSAGE, statusException.getMessage());
                latch.countDown();
            }
        });

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMissingConfigIndexReturnsNotFound() throws InterruptedException {
        String forecasterId = "missing-forecaster";
        CountDownLatch latch = new CountDownLatch(1);
        PreviewForecasterRequest request = new PreviewForecasterRequest(
            null,
            forecasterId,
            Instant.ofEpochMilli(1),
            Instant.ofEpochMilli(2)
        );

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new IndexNotFoundException(ForecastCommonName.CONFIG_INDEX));
            return null;
        }).when(client).get(any(GetRequest.class), any());

        action.doExecute(task, request, new ActionListener<PreviewForecasterResponse>() {
            @Override
            public void onResponse(PreviewForecasterResponse response) {
                fail("Expected not found when forecaster config index is missing");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof OpenSearchStatusException);
                OpenSearchStatusException statusException = (OpenSearchStatusException) e;
                assertEquals(RestStatus.NOT_FOUND, statusException.status());
                assertTrue(statusException.getMessage().contains("Can't find forecaster with id:" + forecasterId));
                latch.countDown();
            }
        });

        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

}
