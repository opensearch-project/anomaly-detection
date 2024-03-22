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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexingPressure;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.transport.TransportService;

public class ADResultBulkTransportActionTests extends AbstractTimeSeriesTest {
    private ADResultBulkTransportAction resultBulk;
    private TransportService transportService;
    private ClusterService clusterService;
    private IndexingPressure indexingPressure;
    private Client client;
    private String detectorId;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
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
            .put(AnomalyDetectorSettings.AD_INDEX_PRESSURE_SOFT_LIMIT.getKey(), 0.8)
            .build();

        // without register these settings, the constructor of ADResultBulkTransportAction cannot invoke update consumer
        setupTestNodes(AnomalyDetectorSettings.AD_INDEX_PRESSURE_SOFT_LIMIT, AnomalyDetectorSettings.AD_INDEX_PRESSURE_HARD_LIMIT);
        transportService = testNodes[0].transportService;
        clusterService = testNodes[0].clusterService;

        ActionFilters actionFilters = mock(ActionFilters.class);
        indexingPressure = mock(IndexingPressure.class);

        client = mock(Client.class);
        detectorId = randomAlphaOfLength(5);

        resultBulk = new ADResultBulkTransportAction(transportService, actionFilters, indexingPressure, settings, clusterService, client);
    }

    @Override
    @After
    public final void tearDown() throws Exception {
        tearDownTestNodes();
        super.tearDown();
    }

    @SuppressWarnings("unchecked")
    public void testSendAll() {
        when(indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes()).thenReturn(0L);
        when(indexingPressure.getCurrentReplicaBytes()).thenReturn(0L);

        ADResultBulkRequest originalRequest = new ADResultBulkRequest();
        originalRequest.add(TestHelpers.randomResultWriteRequest(detectorId, 0.8d, 0d));
        originalRequest.add(TestHelpers.randomResultWriteRequest(detectorId, 8d, 0.2d));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 3
            );

            assertTrue(args[1] instanceof BulkRequest);
            assertTrue(args[2] instanceof ActionListener);
            BulkRequest request = (BulkRequest) args[1];
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) args[2];

            assertEquals(2, request.requests().size());
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        PlainActionFuture<ADResultBulkResponse> future = PlainActionFuture.newFuture();
        resultBulk.doExecute(null, originalRequest, future);

        future.actionGet();
    }

    @SuppressWarnings("unchecked")
    public void testSendPartial() {
        // the limit is 1024 Bytes
        when(indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes()).thenReturn(1000L);
        when(indexingPressure.getCurrentReplicaBytes()).thenReturn(24L);

        ADResultBulkRequest originalRequest = new ADResultBulkRequest();
        originalRequest.add(TestHelpers.randomResultWriteRequest(detectorId, 0.8d, 0d));
        originalRequest.add(TestHelpers.randomResultWriteRequest(detectorId, 8d, 0.2d));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 3
            );

            assertTrue(args[1] instanceof BulkRequest);
            assertTrue(args[2] instanceof ActionListener);
            BulkRequest request = (BulkRequest) args[1];
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) args[2];

            assertEquals(1, request.requests().size());
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        PlainActionFuture<ADResultBulkResponse> future = PlainActionFuture.newFuture();
        resultBulk.doExecute(null, originalRequest, future);

        future.actionGet();
    }

    @SuppressWarnings("unchecked")
    public void testSendRandomPartial() {
        // 1024 * 0.9 > 400 + 421 > 1024 * 0.6. 1024 is 1KB, our INDEX_PRESSURE_SOFT_LIMIT
        when(indexingPressure.getCurrentCombinedCoordinatingAndPrimaryBytes()).thenReturn(400L);
        when(indexingPressure.getCurrentReplicaBytes()).thenReturn(421L);

        ADResultBulkRequest originalRequest = new ADResultBulkRequest();
        for (int i = 0; i < 1000; i++) {
            originalRequest.add(TestHelpers.randomResultWriteRequest(detectorId, 0.8d, 0d));
        }

        originalRequest.add(TestHelpers.randomResultWriteRequest(detectorId, 8d, 0.2d));

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 3
            );

            assertTrue(args[1] instanceof BulkRequest);
            assertTrue(args[2] instanceof ActionListener);
            BulkRequest request = (BulkRequest) args[1];
            ActionListener<BulkResponse> listener = (ActionListener<BulkResponse>) args[2];

            int size = request.requests().size();
            assertTrue(1 < size);
            // at least 1 half should be removed
            assertTrue(String.format(Locale.ROOT, "size is actually %d", size), size < 500);
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        PlainActionFuture<ADResultBulkResponse> future = PlainActionFuture.newFuture();
        resultBulk.doExecute(null, originalRequest, future);

        future.actionGet();
    }

    public void testSerialzationRequest() throws IOException {
        ADResultBulkRequest request = new ADResultBulkRequest();
        request.add(TestHelpers.randomResultWriteRequest(detectorId, 0.8d, 0d));
        request.add(TestHelpers.randomResultWriteRequest(detectorId, 8d, 0.2d));
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        ADResultBulkRequest readRequest = new ADResultBulkRequest(streamInput);
        assertThat(2, equalTo(readRequest.numberOfActions()));
    }

    public void testValidateRequest() {
        ActionRequestValidationException e = new ADResultBulkRequest().validate();
        assertThat(e.validationErrors(), hasItem(ADResultBulkRequest.NO_REQUESTS_ADDED_ERR));
    }
}
