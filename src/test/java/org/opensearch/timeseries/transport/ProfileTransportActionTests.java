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

package org.opensearch.timeseries.transport;

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
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.transport.DelegateADProfileTransportAction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.ModelProfile;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.transport.TransportService;

public class ProfileTransportActionTests extends OpenSearchIntegTestCase {
    private DelegateADProfileTransportAction action;
    private String detectorId = "Pl536HEBnXkDrah03glg";
    String node1, nodeName1;
    DiscoveryNode discoveryNode1;
    Set<ProfileName> profilesToRetrieve = new HashSet<ProfileName>();
    private long modelSize = 4456448L;
    private String modelId = "Pl536HEBnXkDrah03glg_model_rcf_1";
    private ADCacheProvider cacheProvider;
    private int activeEntities = 10;
    private long totalUpdates = 127;
    private long multiEntityModelSize = 712480L;
    // private ADModelManager modelManager;
    private ADTaskCacheManager taskCacheManager;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        // modelManager = mock(ADModelManager.class);
        taskCacheManager = mock(ADTaskCacheManager.class);

        ADPriorityCache cache = mock(ADPriorityCache.class);
        cacheProvider = mock(ADCacheProvider.class);
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

        // Map<String, Long> modelSizes = new HashMap<>();
        // modelSizes.put(modelId, modelSize);
        // when(modelManager.getModelSize(any(String.class))).thenReturn(modelSizes);

        Settings settings = Settings.builder().put("plugins.anomaly_detection.max_model_size_per_node", 100).build();

        action = new DelegateADProfileTransportAction(
            client().threadPool(),
            clusterService(),
            mock(TransportService.class),
            mock(ActionFilters.class),
            taskCacheManager,
            cacheProvider,
            settings
        );

        profilesToRetrieve = new HashSet<ProfileName>();
        profilesToRetrieve.add(ProfileName.COORDINATING_NODE);
    }

    private void setUpModelSize(int maxModel) {
        Settings nodeSettings = Settings.builder().put(AnomalyDetectorSettings.AD_MAX_MODEL_SIZE_PER_NODE.getKey(), maxModel).build();
        internalCluster().startNode(nodeSettings);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TimeSeriesAnalyticsPlugin.class);
    }

    @Test
    public void testNewResponse() {
        setUpModelSize(100);
        DiscoveryNode node = clusterService().localNode();
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, node);

        ProfileNodeResponse profileNodeResponse1 = new ProfileNodeResponse(node, new HashMap<>(), 0, 0, new ArrayList<>(), 0, true);
        List<ProfileNodeResponse> profileNodeResponses = Arrays.asList(profileNodeResponse1);
        List<FailedNodeException> failures = new ArrayList<>();

        ProfileResponse profileResponse = action.newResponse(profileRequest, profileNodeResponses, failures);
        assertEquals(node.getId(), profileResponse.getCoordinatingNode());
    }

    @Test
    public void testNewNodeRequest() {
        setUpModelSize(100);
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve);

        ProfileNodeRequest profileNodeRequest1 = new ProfileNodeRequest(profileRequest);
        ProfileNodeRequest profileNodeRequest2 = action.newNodeRequest(profileRequest);

        assertEquals(profileNodeRequest1.getConfigId(), profileNodeRequest2.getConfigId());
        assertEquals(profileNodeRequest2.getProfilesToBeRetrieved(), profileNodeRequest2.getProfilesToBeRetrieved());
    }

    @Test
    public void testMultiEntityNodeOperation() {
        setUpModelSize(100);
        DiscoveryNode nodeId = clusterService().localNode();
        profilesToRetrieve = new HashSet<ProfileName>();
        profilesToRetrieve.add(ProfileName.ACTIVE_ENTITIES);
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, nodeId);

        ProfileNodeResponse response = action.nodeOperation(new ProfileNodeRequest(profileRequest));

        assertEquals(activeEntities, response.getActiveEntities());
        assertEquals(null, response.getModelSize());

        profilesToRetrieve.add(ProfileName.INIT_PROGRESS);

        profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, nodeId);
        response = action.nodeOperation(new ProfileNodeRequest(profileRequest));

        assertEquals(activeEntities, response.getActiveEntities());
        assertEquals(null, response.getModelSize());
        assertEquals(totalUpdates, response.getTotalUpdates());

        profilesToRetrieve.add(ProfileName.MODELS);
        profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, nodeId);
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

        action = new DelegateADProfileTransportAction(
            client().threadPool(),
            clusterService(),
            mock(TransportService.class),
            mock(ActionFilters.class),
            taskCacheManager,
            cacheProvider,
            settings
        );

        DiscoveryNode nodeId = clusterService().localNode();
        profilesToRetrieve = new HashSet<ProfileName>();
        profilesToRetrieve.add(ProfileName.MODELS);
        ProfileRequest profileRequest = new ProfileRequest(detectorId, profilesToRetrieve, nodeId);
        ProfileNodeResponse response = action.nodeOperation(new ProfileNodeRequest(profileRequest));
        assertEquals(2, response.getModelCount());
        assertEquals(1, response.getModelProfiles().size());
    }
}
