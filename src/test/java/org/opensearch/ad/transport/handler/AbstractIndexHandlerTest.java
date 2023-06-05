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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.opensearch.timeseries.TestHelpers.createIndexBlockedState;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.transport.AnomalyResultTests;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.IndexUtils;
import org.opensearch.ad.util.Throttler;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.TestHelpers;

public abstract class AbstractIndexHandlerTest extends AbstractTimeSeriesTest {
    enum IndexCreation {
        RUNTIME_EXCEPTION,
        RESOURCE_EXISTS_EXCEPTION,
        ACKED,
        NOT_ACKED
    }

    protected static Settings settings;
    protected ClientUtil clientUtil;
    protected ThreadPool context;
    protected IndexUtils indexUtil;
    protected String detectorId = "123";

    @Mock
    protected Client client;

    @Mock
    protected AnomalyDetectionIndices anomalyDetectionIndices;

    @Mock
    protected Throttler throttler;

    @Mock
    protected ClusterService clusterService;

    @Mock
    protected IndexNameExpressionResolver indexNameResolver;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyResultTests.class.getSimpleName());
        settings = Settings
            .builder()
            .put("plugins.anomaly_detection.max_retry_for_backoff", 2)
            .put("plugins.anomaly_detection.backoff_initial_delay", TimeValue.timeValueMillis(1))
            .build();
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
        settings = null;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.initMocks(this);
        setWriteBlockAdResultIndex(false);
        context = TestHelpers.createThreadPool();
        clientUtil = new ClientUtil(settings, client, throttler, context);
        indexUtil = new IndexUtils(client, clientUtil, clusterService, indexNameResolver);
    }

    protected void setWriteBlockAdResultIndex(boolean blocked) {
        String indexName = randomAlphaOfLength(10);
        Settings settings = blocked
            ? Settings.builder().put(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.getKey(), true).build()
            : Settings.EMPTY;
        ClusterState blockedClusterState = createIndexBlockedState(indexName, settings, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS);
        when(clusterService.state()).thenReturn(blockedClusterState);
        when(indexNameResolver.concreteIndexNames(any(), any(), any(String.class))).thenReturn(new String[] { indexName });
    }

    @SuppressWarnings("unchecked")
    protected void setUpSavingAnomalyResultIndex(boolean anomalyResultIndexExists, IndexCreation creationResult) throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length >= 1
            );
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            assertTrue(listener != null);
            switch (creationResult) {
                case RUNTIME_EXCEPTION:
                    listener.onFailure(new RuntimeException());
                    break;
                case RESOURCE_EXISTS_EXCEPTION:
                    listener.onFailure(new ResourceAlreadyExistsException(ADCommonName.ANOMALY_RESULT_INDEX_ALIAS));
                    break;
                case ACKED:
                    listener.onResponse(new CreateIndexResponse(true, true, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS));
                    break;
                case NOT_ACKED:
                    listener.onResponse(new CreateIndexResponse(false, false, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS));
                    break;
                default:
                    assertTrue("should not reach here", false);
                    break;
            }
            return null;
        }).when(anomalyDetectionIndices).initDefaultAnomalyResultIndexDirectly(any());
        when(anomalyDetectionIndices.doesDefaultAnomalyResultIndexExist()).thenReturn(anomalyResultIndexExists);
    }

    protected void setUpSavingAnomalyResultIndex(boolean anomalyResultIndexExists) throws IOException {
        setUpSavingAnomalyResultIndex(anomalyResultIndexExists, IndexCreation.ACKED);
    }
}
