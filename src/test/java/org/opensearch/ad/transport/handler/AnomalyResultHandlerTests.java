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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.transport.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.TestHelpers.createIndexBlockedState;

import java.io.IOException;
import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.transport.AnomalyResultTests;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.IndexUtils;
import org.opensearch.ad.util.Throttler;
import org.opensearch.ad.util.ThrowingConsumerWrapper;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;
import org.opensearch.threadpool.ThreadPool;

public class AnomalyResultHandlerTests extends AbstractADTest {
    private static Settings settings;
    @Mock
    private ClusterService clusterService;

    @Mock
    private Client client;

    private ClientUtil clientUtil;

    @Mock
    private IndexNameExpressionResolver indexNameResolver;

    @Mock
    private AnomalyDetectionIndices anomalyDetectionIndices;

    private String detectorId = "123";

    @Mock
    private Throttler throttler;

    private ThreadPool context;

    private IndexUtils indexUtil;

    @Mock
    private NodeStateManager nodeStateManager;

    @Mock
    private Clock clock;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
        settings = Settings.EMPTY;
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
        settings = null;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(AnomalyIndexHandler.class);
        MockitoAnnotations.initMocks(this);
        setWriteBlockAdResultIndex(false);
        context = TestHelpers.createThreadPool();
        clientUtil = new ClientUtil(settings, client, throttler, context);
        indexUtil = new IndexUtils(client, clientUtil, clusterService, indexNameResolver);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testSavingAdResult() throws IOException {
        setUpSavingAnomalyResultIndex(false);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 2);
            IndexRequest request = invocation.getArgument(0);
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            assertTrue(request != null && listener != null);
            listener.onResponse(mock(IndexResponse.class));
            return null;
        }).when(client).index(any(IndexRequest.class), ArgumentMatchers.<ActionListener<IndexResponse>>any());
        AnomalyIndexHandler<AnomalyResult> handler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            settings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            clientUtil,
            indexUtil,
            clusterService
        );
        handler.index(TestHelpers.randomAnomalyDetectResult(), detectorId);
        assertEquals(1, testAppender.countMessage(AnomalyIndexHandler.SUCCESS_SAVING_MSG, true));
    }

    @Test
    public void testSavingFailureNotRetry() throws InterruptedException, IOException {
        savingFailureTemplate(false, 1, true);

        assertEquals(1, testAppender.countMessage(AnomalyIndexHandler.FAIL_TO_SAVE_ERR_MSG, true));
        assertTrue(!testAppender.containsMessage(AnomalyIndexHandler.SUCCESS_SAVING_MSG, true));
        assertTrue(!testAppender.containsMessage(AnomalyIndexHandler.RETRY_SAVING_ERR_MSG, true));
    }

    @Test
    public void testSavingFailureRetry() throws InterruptedException, IOException {
        setWriteBlockAdResultIndex(false);
        savingFailureTemplate(true, 3, true);

        assertEquals(2, testAppender.countMessage(AnomalyIndexHandler.RETRY_SAVING_ERR_MSG, true));
        assertEquals(1, testAppender.countMessage(AnomalyIndexHandler.FAIL_TO_SAVE_ERR_MSG, true));
        assertTrue(!testAppender.containsMessage(AnomalyIndexHandler.SUCCESS_SAVING_MSG, true));
    }

    @Test
    public void testIndexWriteBlock() {
        setWriteBlockAdResultIndex(true);
        AnomalyIndexHandler<AnomalyResult> handler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            settings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            clientUtil,
            indexUtil,
            clusterService
        );
        handler.index(TestHelpers.randomAnomalyDetectResult(), detectorId);

        assertTrue(testAppender.containsMessage(AnomalyIndexHandler.CANNOT_SAVE_ERR_MSG, true));
    }

    @Test
    public void testAdResultIndexExist() throws IOException {
        setInitAnomalyResultIndexException(true);
        AnomalyIndexHandler<AnomalyResult> handler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            settings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            clientUtil,
            indexUtil,
            clusterService
        );
        handler.index(TestHelpers.randomAnomalyDetectResult(), detectorId);
        verify(client, times(1)).index(any(), any());
    }

    @Test
    public void testAdResultIndexOtherException() throws IOException {
        expectedEx.expect(AnomalyDetectionException.class);
        expectedEx.expectMessage("Error in saving .opendistro-anomaly-results for detector " + detectorId);

        setInitAnomalyResultIndexException(false);
        AnomalyIndexHandler<AnomalyResult> handler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            settings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            clientUtil,
            indexUtil,
            clusterService
        );
        handler.index(TestHelpers.randomAnomalyDetectResult(), detectorId);
        verify(client, never()).index(any(), any());
    }

    private void setInitAnomalyResultIndexException(boolean indexExistException) throws IOException {
        Exception e = indexExistException ? mock(ResourceAlreadyExistsException.class) : mock(RuntimeException.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 1);
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            assertTrue(listener != null);
            listener.onFailure(e);
            return null;
        }).when(anomalyDetectionIndices).initAnomalyResultIndexDirectly(any());
    }

    private void setWriteBlockAdResultIndex(boolean blocked) {
        String indexName = randomAlphaOfLength(10);
        Settings settings = blocked
            ? Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build()
            : Settings.EMPTY;
        ClusterState blockedClusterState = createIndexBlockedState(indexName, settings, CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        when(clusterService.state()).thenReturn(blockedClusterState);
        when(indexNameResolver.concreteIndexNames(any(), any(), any(String.class))).thenReturn(new String[] { indexName });
    }

    /**
     * Template to test exponential backoff retry during saving anomaly result.
     *
     * @param throwOpenSearchRejectedExecutionException whether to throw
     *                                          OpenSearchRejectedExecutionException in the
     *                                          client::index mock or not
     * @param latchCount                        used for coordinating. Equal to
     *                                          number of expected retries plus 1.
     * @throws InterruptedException if thread execution is interrupted
     * @throws IOException          if IO failures
     */
    @SuppressWarnings("unchecked")
    private void savingFailureTemplate(boolean throwOpenSearchRejectedExecutionException, int latchCount, boolean adResultIndexExists)
        throws InterruptedException,
        IOException {
        setUpSavingAnomalyResultIndex(adResultIndexExists);

        final CountDownLatch backoffLatch = new CountDownLatch(latchCount);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 2);
            IndexRequest request = invocation.getArgument(0);
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            assertTrue(request != null && listener != null);
            if (throwOpenSearchRejectedExecutionException) {
                listener.onFailure(new OpenSearchRejectedExecutionException(""));
            } else {
                listener.onFailure(new IllegalArgumentException());
            }

            backoffLatch.countDown();
            return null;
        }).when(client).index(any(IndexRequest.class), ArgumentMatchers.<ActionListener<IndexResponse>>any());

        Settings backoffSettings = Settings
            .builder()
            .put("plugins.anomaly_detection.max_retry_for_backoff", 2)
            .put("plugins.anomaly_detection.backoff_initial_delay", TimeValue.timeValueMillis(1))
            .build();

        AnomalyIndexHandler<AnomalyResult> handler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            backoffSettings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            clientUtil,
            indexUtil,
            clusterService
        );

        handler.index(TestHelpers.randomAnomalyDetectResult(), detectorId);

        backoffLatch.await(1, TimeUnit.MINUTES);
    }

    @SuppressWarnings("unchecked")
    private void setUpSavingAnomalyResultIndex(boolean anomalyResultIndexExists) throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length >= 1);
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            assertTrue(listener != null);
            listener.onResponse(new CreateIndexResponse(true, true, CommonName.ANOMALY_RESULT_INDEX_ALIAS) {
            });
            return null;
        }).when(anomalyDetectionIndices).initAnomalyResultIndexDirectly(any());
        when(anomalyDetectionIndices.doesAnomalyResultIndexExist()).thenReturn(anomalyResultIndexExists);
    }
}
