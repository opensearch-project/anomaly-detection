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

package com.amazon.opendistroforelasticsearch.ad.transport;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.caching.CacheProvider;
import com.amazon.opendistroforelasticsearch.ad.caching.EntityCache;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorProfileName;

public class ProfileTransportActionTests extends OpenSearchIntegTestCase {
    private ProfileTransportAction action;
    private String detectorId = "Pl536HEBnXkDrah03glg";
    String node1, nodeName1;
    DiscoveryNode discoveryNode1;
    Set<DetectorProfileName> profilesToRetrieve = new HashSet<DetectorProfileName>();
    private int shingleSize = 6;
    private long modelSize = 4456448L;
    private String modelId = "Pl536HEBnXkDrah03glg_model_rcf_1";
    private CacheProvider cacheProvider;
    private int activeEntities = 10;
    private long totalUpdates = 127;
    private long multiEntityModelSize = 712480L;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        ModelManager modelManager = mock(ModelManager.class);
        FeatureManager featureManager = mock(FeatureManager.class);

        when(featureManager.getShingleSize(any(String.class))).thenReturn(shingleSize);

        EntityCache cache = mock(EntityCache.class);
        cacheProvider = mock(CacheProvider.class);
        when(cacheProvider.get()).thenReturn(cache);
        when(cache.getActiveEntities(anyString())).thenReturn(activeEntities);
        when(cache.getTotalUpdates(anyString())).thenReturn(totalUpdates);
        Map<String, Long> multiEntityModelSizeMap = new HashMap<>();
        multiEntityModelSizeMap.put("T4c3dXUBj-2IZN7itix__entity_app_3", multiEntityModelSize);
        multiEntityModelSizeMap.put("T4c3dXUBj-2IZN7itix__entity_app_2", multiEntityModelSize);
        when(cache.getModelSize(anyString())).thenReturn(multiEntityModelSizeMap);

        Map<String, Long> modelSizes = new HashMap<>();
        modelSizes.put(modelId, modelSize);
        when(modelManager.getModelSize(any(String.class))).thenReturn(modelSizes);

        action = new ProfileTransportAction(
            client().threadPool(),
            clusterService(),
            mock(TransportService.class),
            mock(ActionFilters.class),
            modelManager,
            featureManager,
            cacheProvider
        );

        profilesToRetrieve = new HashSet<DetectorProfileName>();
        profilesToRetrieve.add(DetectorProfileName.COORDINATING_NODE);
    }

    @Test
    public void testNewResponse() {
        DiscoveryNode node = clusterService().localNode();
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, false, node);

        ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(node, new HashMap<>(), shingleSize, 0, 0);
        List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1);
        List<FailedNodeException> failures = new ArrayList<>();

        ProfileResponse profileResponse = action.newResponse(profileRequest, profileNodeResponses, failures);
        assertEquals(node.getId(), profileResponse.getCoordinatingNode());
    }

    @Test
    public void testNewNodeRequest() {

        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, false);

        ProfileNodeRequest profileNodeRequest1 = new ProfileNodeRequest(profileRequest);
        ProfileNodeRequest profileNodeRequest2 = action.newNodeRequest(profileRequest);

        assertEquals(profileNodeRequest1.getDetectorId(), profileNodeRequest2.getDetectorId());
        assertEquals(profileNodeRequest2.getProfilesToBeRetrieved(), profileNodeRequest2.getProfilesToBeRetrieved());
    }

    @Test
    public void testNodeOperation() {

        DiscoveryNode nodeId = clusterService().localNode();
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, false, nodeId);

        ProfileNodeResponse response = action.nodeOperation(new ProfileNodeRequest(profileRequest));

        assertEquals(shingleSize, response.getShingleSize());
        assertEquals(null, response.getModelSize());

        profilesToRetrieve = new HashSet<DetectorProfileName>();
        profilesToRetrieve.add(DetectorProfileName.TOTAL_SIZE_IN_BYTES);

        profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, false, nodeId);
        response = action.nodeOperation(new ProfileNodeRequest(profileRequest));

        assertEquals(-1, response.getShingleSize());
        assertEquals(1, response.getModelSize().size());
        assertEquals(modelSize, response.getModelSize().get(modelId).longValue());
    }

    @Test
    public void testMultiEntityNodeOperation() {

        DiscoveryNode nodeId = clusterService().localNode();
        profilesToRetrieve = new HashSet<DetectorProfileName>();
        profilesToRetrieve.add(DetectorProfileName.ACTIVE_ENTITIES);
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, true, nodeId);

        ProfileNodeResponse response = action.nodeOperation(new ProfileNodeRequest(profileRequest));

        assertEquals(activeEntities, response.getActiveEntities());
        assertEquals(null, response.getModelSize());

        profilesToRetrieve.add(DetectorProfileName.INIT_PROGRESS);

        profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, true, nodeId);
        response = action.nodeOperation(new ProfileNodeRequest(profileRequest));

        assertEquals(activeEntities, response.getActiveEntities());
        assertEquals(null, response.getModelSize());
        assertEquals(totalUpdates, response.getTotalUpdates());

        profilesToRetrieve.add(DetectorProfileName.MODELS);
        profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, true, nodeId);
        response = action.nodeOperation(new ProfileNodeRequest(profileRequest));

        assertEquals(activeEntities, response.getActiveEntities());
        assertEquals(2, response.getModelSize().size());
        assertEquals(totalUpdates, response.getTotalUpdates());
    }
}
