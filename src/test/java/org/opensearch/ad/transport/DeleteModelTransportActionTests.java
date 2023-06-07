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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.ml.EntityColdStarter;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.extensions.DiscoveryExtensionNode;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.ThreadPool;

import com.google.gson.JsonElement;

import test.org.opensearch.ad.util.JsonDeserializer;

public class DeleteModelTransportActionTests extends AbstractADTest {
    private DeleteModelTransportAction action;
    private String localNodeID;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ThreadPool threadPool = mock(ThreadPool.class);

        SDKClusterService clusterService = mock(SDKClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.getClusterName()).thenReturn(new ClusterName("clustername"));
        localNodeID = "foo";
        DiscoveryExtensionNode discoveryExtensionNode = new DiscoveryExtensionNode(
            "name",
            localNodeID,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Version.CURRENT,
            Version.CURRENT,
            Collections.emptyList()
        );
        when(clusterService.localNode()).thenReturn(discoveryExtensionNode);

        TaskManager taskManager = mock(TaskManager.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        NodeStateManager nodeStateManager = mock(NodeStateManager.class);
        ModelManager modelManager = mock(ModelManager.class);
        FeatureManager featureManager = mock(FeatureManager.class);
        CacheProvider cacheProvider = mock(CacheProvider.class);
        EntityCache entityCache = mock(EntityCache.class);
        when(cacheProvider.get()).thenReturn(entityCache);
        ADTaskCacheManager adTaskCacheManager = mock(ADTaskCacheManager.class);
        EntityColdStarter coldStarter = mock(EntityColdStarter.class);
        ExtensionsRunner extensionsRunner = mock(ExtensionsRunner.class);
        Settings settings = Settings.builder().put("cluster.name", "clusterName").build();
        when(extensionsRunner.getEnvironmentSettings()).thenReturn(settings);

        action = new DeleteModelTransportAction(
            threadPool,
            clusterService,
            taskManager,
            actionFilters,
            nodeStateManager,
            modelManager,
            featureManager,
            cacheProvider,
            adTaskCacheManager,
            coldStarter,
            extensionsRunner
        );
    }

    public void testNormal() throws IOException, JsonPathNotFoundException {
        DeleteModelRequest request = new DeleteModelRequest("123");
        assertThat(request.validate(), is(nullValue()));

        DeleteModelNodeRequest nodeRequest = new DeleteModelNodeRequest(request);
        BytesStreamOutput nodeRequestOut = new BytesStreamOutput();
        nodeRequestOut.setVersion(Version.CURRENT);
        nodeRequest.writeTo(nodeRequestOut);
        StreamInput siNode = nodeRequestOut.bytes().streamInput();

        DeleteModelNodeRequest nodeResponseRead = new DeleteModelNodeRequest(siNode);

        List<DeleteModelNodeResponse> nodeResponses = new ArrayList<>();

        action
            .doExecute(
                mock(Task.class),
                request,
                ActionListener.wrap(response -> nodeResponses.add(response.getNodes().get(0)), ex -> assertTrue(false))
            );
        action
            .doExecute(
                mock(Task.class),
                request,
                ActionListener.wrap(response -> nodeResponses.add(response.getNodes().get(0)), ex -> assertTrue(false))
            );

        DeleteModelResponse response = action.newResponse(request, nodeResponses, Collections.emptyList());

        assertEquals(2, response.getNodes().size());
        assertTrue(!response.hasFailures());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = Strings.toString(builder);
        Function<JsonElement, String> function = (s) -> {
            try {
                return JsonDeserializer.getTextValue(s, CronNodeResponse.NODE_ID);
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
            return null;
        };
        assertArrayEquals(
            JsonDeserializer.getArrayValue(json, function, CronResponse.NODES_JSON_KEY),
            new String[] { localNodeID, localNodeID }
        );
    }

    public void testEmptyDetectorID() {
        ActionRequestValidationException e = new DeleteModelRequest().validate();
        assertThat(e.validationErrors(), Matchers.hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }
}
