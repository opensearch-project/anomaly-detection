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

package org.opensearch.action.admin.indices.mapping.get;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;
import org.opensearch.ad.transport.IndexAnomalyDetectorResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.AbstractAnomalyDetectorActionHandler;
import org.opensearch.ad.task.ADTaskManager;

public class AnomalyDetectorActionHandlerTestsBase<T extends ActionResponse> extends AbstractADTest {
    static ThreadPool threadPool;
    private String TEXT_FIELD_TYPE = "text";
    private IndexAnomalyDetectorActionHandler handler;
    private ClusterService clusterService;
    private NodeClient clientMock;
    private TransportService transportService;
    private ActionListener<IndexAnomalyDetectorResponse> channel;
    private AnomalyDetectionIndices anomalyDetectionIndices;
    private String detectorId;
    private Long seqNo;
    private Long primaryTerm;
    private AnomalyDetector detector;
    private WriteRequest.RefreshPolicy refreshPolicy;
    private TimeValue requestTimeout;
    private Integer maxSingleEntityAnomalyDetectors;
    private Integer maxMultiEntityAnomalyDetectors;
    private Integer maxAnomalyFeatures;
    private Settings settings;
    private RestRequest.Method method;
    private ADTaskManager adTaskManager;
    private SearchFeatureDao searchFeatureDao;


    /**
     * Mockito does not allow mock final methods.  Make my own delegates and mock them.
     *
     */
//    class NodeClientDelegate extends NodeClient {
//
//        NodeClientDelegate(Settings settings, ThreadPool threadPool) {
//            super(settings, threadPool);
//        }
//
//        public <Request extends ActionRequest, Response extends ActionResponse> void execute2(
//                ActionType<Response> action,
//                Request request,
//                ActionListener<Response> listener
//        ) {
//            super.execute(action, request, listener);
//        }
//    }

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool("IndexAnomalyDetectorJobActionHandlerTests");
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        settings = Settings.EMPTY;
        clusterService = mock(ClusterService.class);
        clientMock = spy(new NodeClient(settings, null));
        transportService = mock(TransportService.class);

        channel = mock(ActionListener.class);

        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        when(anomalyDetectionIndices.doesAnomalyDetectorIndexExist()).thenReturn(true);

        detectorId = "123";
        seqNo = 0L;
        primaryTerm = 0L;

        WriteRequest.RefreshPolicy refreshPolicy = WriteRequest.RefreshPolicy.IMMEDIATE;

        String field = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));

        requestTimeout = new TimeValue(1000L);

        maxSingleEntityAnomalyDetectors = 1000;

        maxMultiEntityAnomalyDetectors = 10;

        maxAnomalyFeatures = 5;

        method = RestRequest.Method.POST;

        adTaskManager = mock(ADTaskManager.class);
    }
}


