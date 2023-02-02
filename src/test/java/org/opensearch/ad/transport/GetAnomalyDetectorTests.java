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

package org.opensearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.ad.util.SecurityClientUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

public class GetAnomalyDetectorTests extends AbstractADTest {
    private GetAnomalyDetectorTransportAction action;
    private TransportService transportService;
    private DiscoveryNodeFilterer nodeFilter;
    private ActionFilters actionFilters;
    private Client client;
    private SecurityClientUtil clientUtil;
    private GetAnomalyDetectorRequest request;
    private String detectorId = "yecrdnUBqurvo9uKU_d8";
    private String entityValue = "app_0";
    private String typeStr;
    private String rawPath;
    private PlainActionFuture<GetAnomalyDetectorResponse> future;
    private ADTaskManager adTaskManager;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(EntityProfileTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            null,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> null,
            null,
            Collections.emptySet()
        );

        nodeFilter = mock(DiscoveryNodeFilterer.class);

        actionFilters = mock(ActionFilters.class);

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        clientUtil = new SecurityClientUtil(nodeStateManager, Settings.EMPTY);

        adTaskManager = mock(ADTaskManager.class);

        action = new GetAnomalyDetectorTransportAction(
            transportService,
            nodeFilter,
            actionFilters,
            clusterService,
            client,
            clientUtil,
            Settings.EMPTY,
            xContentRegistry(),
            adTaskManager
        );
    }

    public void testInvalidRequest() throws IOException {
        typeStr = "entity_info2,init_progress2";

        rawPath = "_opendistro/_anomaly_detection/detectors/T4c3dXUBj-2IZN7itix_/_profile";

        request = new GetAnomalyDetectorRequest(detectorId, 0L, false, false, typeStr, rawPath, false, entityValue);

        future = new PlainActionFuture<>();
        action.doExecute(null, request, future);
        assertException(future, InvalidParameterException.class, CommonErrorMessages.EMPTY_PROFILES_COLLECT);
    }

    @SuppressWarnings("unchecked")
    public void testValidRequest() throws IOException {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            GetRequest request = (GetRequest) args[0];
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];

            String indexName = request.index();
            if (indexName.equals(ANOMALY_DETECTORS_INDEX)) {
                listener.onResponse(null);
            }
            return null;
        }).when(client).get(any(), any());

        typeStr = "entity_info,init_progress";

        rawPath = "_opendistro/_anomaly_detection/detectors/T4c3dXUBj-2IZN7itix_/_profile";

        request = new GetAnomalyDetectorRequest(detectorId, 0L, false, false, typeStr, rawPath, false, entityValue);

        future = new PlainActionFuture<>();
        action.doExecute(null, request, future);
        assertException(future, InvalidParameterException.class, CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG);
    }
}
