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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.model.DetectorProfileName;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.ModelProfile;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.TransportService;

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
    private ModelManager modelManager;
    private FeatureManager featureManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        modelManager = mock(ModelManager.class);
        featureManager = mock(FeatureManager.class);

        when(featureManager.getShingleSize(any(String.class))).thenReturn(shingleSize);

        EntityCache cache = mock(EntityCache.class);
        cacheProvider = mock(CacheProvider.class);
        when(cacheProvider.get()).thenReturn(cache);
        when(cache.getActiveEntities(anyString())).thenReturn(activeEntities);
        when(cache.getTotalUpdates(anyString())).thenReturn(totalUpdates);
        Map<String, Long> multiEntityModelSizeMap = new HashMap<>();
        String modelId1 = "T4c3dXUBj-2IZN7itix__entity_app_3";
        String modelId2 = "T4c3dXUBj-2IZN7itix__entity_app_2";
        multiEntityModelSizeMap.put(modelId1, multiEntityModelSize);
        multiEntityModelSizeMap.put(modelId2, multiEntityModelSize);
        when(cache.getModelSize(anyString())).thenReturn(multiEntityModelSizeMap);

        List<ModelProfile> modelProfiles = new ArrayList<>();
        String field = "field";
        String fieldVal1 = "value1";
        String fieldVal2 = "value2";
        Entity entity1 = Entity.createSingleAttributeEntity(field, fieldVal1);
        Entity entity2 = Entity.createSingleAttributeEntity(field, fieldVal2);
        modelProfiles.add(new ModelProfile(modelId1, entity1, multiEntityModelSize));
        modelProfiles.add(new ModelProfile(modelId1, entity2, multiEntityModelSize));
        when(cache.getAllModelProfile(anyString())).thenReturn(modelProfiles);

        Map<String, Long> modelSizes = new HashMap<>();
        modelSizes.put(modelId, modelSize);
        when(modelManager.getModelSize(any(String.class))).thenReturn(modelSizes);

        Settings settings = Settings.builder().put("plugins.anomaly_detection.max_model_size_per_node", 100).build();

        action = new ProfileTransportAction(
            client().threadPool(),
            clusterService(),
            mock(TransportService.class),
            mock(ActionFilters.class),
            modelManager,
            featureManager,
            cacheProvider,
            settings
        );

        profilesToRetrieve = new HashSet<DetectorProfileName>();
        profilesToRetrieve.add(DetectorProfileName.COORDINATING_NODE);
    }

    private void setUpModelSize(int maxModel) {
        Settings nodeSettings = Settings.builder().put(AnomalyDetectorSettings.MAX_MODEL_SZIE.getKey(), maxModel).build();
        internalCluster().startNode(nodeSettings);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(AnomalyDetectorPlugin.class);
    }

    @Test
    public void testNewResponse() {
        setUpModelSize(100);
        DiscoveryNode node = clusterService().localNode();
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, false, node);

        ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(node, new HashMap<>(), shingleSize, 0, 0, new ArrayList<>(), 0);
        List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1);
        List<FailedNodeException> failures = new ArrayList<>();

        ProfileResponse profileResponse = action.newResponse(profileRequest, profileNodeResponses, failures);
        assertEquals(node.getId(), profileResponse.getCoordinatingNode());
    }

    @Test
    public void testNewNodeRequest() {
        setUpModelSize(100);
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, false);

        ProfileNodeRequest profileNodeRequest1 = new ProfileNodeRequest(profileRequest);
        ProfileNodeRequest profileNodeRequest2 = action.newNodeRequest(profileRequest);

        assertEquals(profileNodeRequest1.getDetectorId(), profileNodeRequest2.getDetectorId());
        assertEquals(profileNodeRequest2.getProfilesToBeRetrieved(), profileNodeRequest2.getProfilesToBeRetrieved());
    }

    @Test
    public void testNodeOperation() {
        setUpModelSize(100);
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
        setUpModelSize(100);
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
        assertEquals(null, response.getModelSize());
        assertEquals(2, response.getModelProfiles().size());
        assertEquals(totalUpdates, response.getTotalUpdates());
        assertEquals(2, response.getModelCount());
    }

    @Test
    public void testModelCount() {
        setUpModelSize(1);

        Settings settings = Settings.builder().put("plugins.anomaly_detection.max_model_size_per_node", 1).build();

        action = new ProfileTransportAction(
            client().threadPool(),
            clusterService(),
            mock(TransportService.class),
            mock(ActionFilters.class),
            modelManager,
            featureManager,
            cacheProvider,
            settings
        );

        DiscoveryNode nodeId = clusterService().localNode();
        profilesToRetrieve = new HashSet<DetectorProfileName>();
        profilesToRetrieve.add(DetectorProfileName.MODELS);
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, true, nodeId);
        ProfileNodeResponse response = action.nodeOperation(new ProfileNodeRequest(profileRequest));
        assertEquals(2, response.getModelCount());
        assertEquals(1, response.getModelProfiles().size());
    }
}
