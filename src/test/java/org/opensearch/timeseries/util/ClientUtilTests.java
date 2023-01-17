/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultRequest;
import org.opensearch.ad.transport.AnomalyResultResponse;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.model.FeatureData;

public class ClientUtilTests extends OpenSearchTestCase {
    private AnomalyResultRequest asyncRequest;

    private ClientUtil clientUtil;

    private Client client;
    private CountDownLatch latch;
    private ActionListener<AnomalyResultResponse> latchListener;
    private AnomalyResultResponse actualResponse;
    private Exception exception;
    private ActionListener<AnomalyResultResponse> listener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        asyncRequest = new AnomalyResultRequest("abc123", 100, 200);

        listener = new ActionListener<>() {
            @Override
            public void onResponse(AnomalyResultResponse resultResponse) {
                actualResponse = resultResponse;
            }

            @Override
            public void onFailure(Exception e) {
                exception = e;
            }
        };
        actualResponse = null;
        exception = null;

        latch = new CountDownLatch(1);
        latchListener = new LatchedActionListener<>(listener, latch);

        client = mock(Client.class);
        clientUtil = new ClientUtil(client);
    }

    public void testAsyncRequestOnSuccess() throws InterruptedException {
        AnomalyResultResponse expected = new AnomalyResultResponse(
            4d,
            0.993,
            1.01,
            Collections.singletonList(new FeatureData("xyz", "foo", 0d)),
            randomAlphaOfLength(4),
            randomLong(),
            randomLong(),
            randomBoolean(),
            randomInt(),
            new double[] { randomDoubleBetween(0, 1.0, true), randomDoubleBetween(0, 1.0, true) },
            new double[] { randomDouble(), randomDouble() },
            new double[][] { new double[] { randomDouble(), randomDouble() } },
            new double[] { randomDouble() },
            randomDoubleBetween(1.1, 10.0, true),
            null
        );
        BiConsumer<AnomalyResultRequest, ActionListener<AnomalyResultResponse>> consumer = (request, actionListener) -> {
            // simulate successful operation
            // actionListener.onResponse();
            latchListener.onResponse(expected);
        };
        clientUtil.asyncRequest(asyncRequest, consumer, listener);

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
        assertNotNull(actualResponse);
        assertNull(exception);
        org.hamcrest.MatcherAssert.assertThat(actualResponse, equalTo(expected));
    }

    public void testAsyncRequestOnFailure() {
        Exception testException = new Exception("Test exception");
        BiConsumer<AnomalyResultRequest, ActionListener<AnomalyResultResponse>> consumer = (request, actionListener) -> {
            // simulate successful operation
            // actionListener.onResponse();
            latchListener.onFailure(testException);
        };
        clientUtil.asyncRequest(asyncRequest, consumer, listener);
        assertNull(actualResponse);
        assertNotNull(exception);
        assertEquals("Test exception", exception.getMessage());
    }

    @SuppressWarnings("unchecked")
    public void testExecuteOnSuccess() throws InterruptedException {
        AnomalyResultResponse expected = new AnomalyResultResponse(
            4d,
            0.993,
            1.01,
            Collections.singletonList(new FeatureData("xyz", "foo", 0d)),
            randomAlphaOfLength(4),
            randomLong(),
            randomLong(),
            randomBoolean(),
            randomInt(),
            new double[] { randomDoubleBetween(0, 1.0, true), randomDoubleBetween(0, 1.0, true) },
            new double[] { randomDouble(), randomDouble() },
            new double[][] { new double[] { randomDouble(), randomDouble() } },
            new double[] { randomDouble() },
            randomDoubleBetween(1.1, 10.0, true),
            null
        );
        doAnswer(invocationOnMock -> {
            ((ActionListener<AnomalyResultResponse>) invocationOnMock.getArguments()[2]).onResponse(expected);
            latch.countDown();
            return null;
        }).when(client).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
        clientUtil.execute(AnomalyResultAction.INSTANCE, asyncRequest, latchListener);

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
        assertNotNull(actualResponse);
        assertNull(exception);
        org.hamcrest.MatcherAssert.assertThat(actualResponse, equalTo(expected));
    }

    @SuppressWarnings("unchecked")
    public void testExecuteOnFailure() {
        Exception testException = new Exception("Test exception");
        doAnswer(invocationOnMock -> {
            ((ActionListener<AnomalyResultResponse>) invocationOnMock.getArguments()[2]).onFailure(testException);
            latch.countDown();
            return null;
        }).when(client).execute(eq(AnomalyResultAction.INSTANCE), any(), any());
        clientUtil.execute(AnomalyResultAction.INSTANCE, asyncRequest, latchListener);
        assertNull(actualResponse);
        assertNotNull(exception);
        assertEquals("Test exception", exception.getMessage());
    }
}
