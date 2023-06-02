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

package org.opensearch.ad.transport.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.ratelimit.RequestPriority;
import org.opensearch.ad.ratelimit.ResultWriteRequest;
import org.opensearch.ad.transport.ADResultBulkAction;
import org.opensearch.ad.transport.ADResultBulkRequest;
import org.opensearch.ad.transport.ADResultBulkResponse;
import org.opensearch.timeseries.common.exception.TimeSeriesException;

public class MultiEntityResultHandlerTests extends AbstractIndexHandlerTest {
    private MultiEntityResultHandler handler;
    private ADResultBulkRequest request;
    private ADResultBulkResponse response;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        handler = new MultiEntityResultHandler(
            client,
            settings,
            threadPool,
            anomalyDetectionIndices,
            clientUtil,
            indexUtil,
            clusterService
        );

        request = new ADResultBulkRequest();
        ResultWriteRequest resultWriteRequest = new ResultWriteRequest(
            Instant.now().plus(10, ChronoUnit.MINUTES).toEpochMilli(),
            detectorId,
            RequestPriority.MEDIUM,
            TestHelpers.randomAnomalyDetectResult(),
            null
        );
        request.add(resultWriteRequest);

        response = new ADResultBulkResponse();

        super.setUpLog4jForJUnit(MultiEntityResultHandler.class);

        doAnswer(invocation -> {
            ActionListener<ADResultBulkResponse> listener = invocation.getArgument(2);
            listener.onResponse(response);
            return null;
        }).when(client).execute(eq(ADResultBulkAction.INSTANCE), any(), ArgumentMatchers.<ActionListener<ADResultBulkResponse>>any());
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
    }

    @Test
    public void testIndexWriteBlock() throws InterruptedException {
        setWriteBlockAdResultIndex(true);

        CountDownLatch verified = new CountDownLatch(1);

        handler.flush(request, ActionListener.wrap(response -> {
            assertTrue("Should not reach here ", false);
            verified.countDown();
        }, exception -> {
            assertTrue(exception instanceof TimeSeriesException);
            assertTrue(
                "actual: " + exception.getMessage(),
                exception.getMessage().contains(MultiEntityResultHandler.CANNOT_SAVE_RESULT_ERR_MSG)
            );
            verified.countDown();
        }));

        assertTrue(verified.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testSavingAdResult() throws IOException, InterruptedException {
        setUpSavingAnomalyResultIndex(false);

        CountDownLatch verified = new CountDownLatch(1);
        handler.flush(request, ActionListener.wrap(response -> { verified.countDown(); }, exception -> {
            assertTrue("Should not reach here ", false);
            verified.countDown();
        }));
        assertTrue(verified.await(100, TimeUnit.SECONDS));
        assertEquals(1, testAppender.countMessage(MultiEntityResultHandler.SUCCESS_SAVING_RESULT_MSG, false));
    }

    @Test
    public void testSavingFailure() throws IOException, InterruptedException {
        setUpSavingAnomalyResultIndex(false);
        doAnswer(invocation -> {
            ActionListener<ADResultBulkResponse> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(client).execute(eq(ADResultBulkAction.INSTANCE), any(), ArgumentMatchers.<ActionListener<ADResultBulkResponse>>any());

        CountDownLatch verified = new CountDownLatch(1);
        handler.flush(request, ActionListener.wrap(response -> {
            assertTrue("Should not reach here ", false);
            verified.countDown();
        }, exception -> {
            assertTrue(exception instanceof RuntimeException);
            verified.countDown();
        }));
        assertTrue(verified.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testAdResultIndexExists() throws IOException, InterruptedException {
        setUpSavingAnomalyResultIndex(true);

        CountDownLatch verified = new CountDownLatch(1);
        handler.flush(request, ActionListener.wrap(response -> { verified.countDown(); }, exception -> {
            assertTrue("Should not reach here ", false);
            verified.countDown();
        }));
        assertTrue(verified.await(100, TimeUnit.SECONDS));
        assertEquals(1, testAppender.countMessage(MultiEntityResultHandler.SUCCESS_SAVING_RESULT_MSG, false));
    }

    @Test
    public void testNothingToSave() throws IOException, InterruptedException {
        setUpSavingAnomalyResultIndex(false);

        CountDownLatch verified = new CountDownLatch(1);
        handler.flush(new ADResultBulkRequest(), ActionListener.wrap(response -> {
            assertTrue("Should not reach here ", false);
            verified.countDown();
        }, exception -> {
            assertTrue(exception instanceof TimeSeriesException);
            verified.countDown();
        }));
        assertTrue(verified.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testCreateUnAcked() throws IOException, InterruptedException {
        setUpSavingAnomalyResultIndex(false, IndexCreation.NOT_ACKED);

        CountDownLatch verified = new CountDownLatch(1);
        handler.flush(request, ActionListener.wrap(response -> {
            assertTrue("Should not reach here ", false);
            verified.countDown();
        }, exception -> {
            assertTrue(exception instanceof TimeSeriesException);
            verified.countDown();
        }));
        assertTrue(verified.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testCreateRuntimeException() throws IOException, InterruptedException {
        setUpSavingAnomalyResultIndex(false, IndexCreation.RUNTIME_EXCEPTION);

        CountDownLatch verified = new CountDownLatch(1);
        handler.flush(request, ActionListener.wrap(response -> {
            assertTrue("Should not reach here ", false);
            verified.countDown();
        }, exception -> {
            assertTrue(exception instanceof RuntimeException);
            verified.countDown();
        }));
        assertTrue(verified.await(100, TimeUnit.SECONDS));
    }

    @Test
    public void testCreateResourcExistsException() throws IOException, InterruptedException {
        setUpSavingAnomalyResultIndex(false, IndexCreation.RESOURCE_EXISTS_EXCEPTION);

        CountDownLatch verified = new CountDownLatch(1);
        handler.flush(request, ActionListener.wrap(response -> { verified.countDown(); }, exception -> {
            assertTrue("Should not reach here ", false);
            verified.countDown();
        }));
        assertTrue(verified.await(100, TimeUnit.SECONDS));
        assertEquals(1, testAppender.countMessage(MultiEntityResultHandler.SUCCESS_SAVING_RESULT_MSG, false));
    }
}
