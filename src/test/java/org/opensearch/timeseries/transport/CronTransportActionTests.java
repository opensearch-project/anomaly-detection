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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.transport.TransportService;

import com.google.gson.JsonElement;

import test.org.opensearch.ad.util.JsonDeserializer;

public class CronTransportActionTests extends AbstractTimeSeriesTest {
    private CronTransportAction action;
    private String localNodeID;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        ThreadPool threadPool = mock(ThreadPool.class);

        ClusterService clusterService = mock(ClusterService.class);
        localNodeID = "foo";
        when(clusterService.localNode()).thenReturn(new DiscoveryNode(localNodeID, buildNewFakeTransportAddress(), Version.CURRENT));
        when(clusterService.getClusterName()).thenReturn(new ClusterName("test"));

        TransportService transportService = mock(TransportService.class);
        ActionFilters actionFilters = mock(ActionFilters.class);
        NodeStateManager tarnsportStatemanager = mock(NodeStateManager.class);
        ADModelManager modelManager = mock(ADModelManager.class);
        FeatureManager featureManager = mock(FeatureManager.class);
        ADCacheProvider cacheProvider = mock(ADCacheProvider.class);
        ADPriorityCache entityCache = mock(ADPriorityCache.class);
        ADColdStart entityColdStarter = mock(ADColdStart.class);
        when(cacheProvider.get()).thenReturn(entityCache);
        ADTaskManager adTaskManager = mock(ADTaskManager.class);

        ForecastCacheProvider forecastCacheProvider = mock(ForecastCacheProvider.class);
        ForecastPriorityCache forecastCache = mock(ForecastPriorityCache.class);
        ForecastColdStart forecastColdStarter = mock(ForecastColdStart.class);
        when(forecastCacheProvider.get()).thenReturn(forecastCache);
        ForecastTaskManager forecastTaskManager = mock(ForecastTaskManager.class);

        action = new CronTransportAction(
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            tarnsportStatemanager,
            modelManager,
            featureManager,
            cacheProvider,
            forecastCacheProvider,
            entityColdStarter,
            forecastColdStarter,
            adTaskManager,
            forecastTaskManager
        );
    }

    public void testNormal() throws IOException, JsonPathNotFoundException {
        CronRequest request = new CronRequest();

        CronNodeRequest nodeRequest = new CronNodeRequest();
        BytesStreamOutput nodeRequestOut = new BytesStreamOutput();
        nodeRequestOut.setVersion(Version.V_2_0_0);
        nodeRequest.writeTo(nodeRequestOut);
        StreamInput siNode = nodeRequestOut.bytes().streamInput();
        siNode.setVersion(Version.V_2_0_0);

        CronNodeRequest nodeResponseRead = new CronNodeRequest(siNode);

        CronNodeResponse nodeResponse1 = action.nodeOperation(nodeResponseRead);
        CronNodeResponse nodeResponse2 = action.nodeOperation(new CronNodeRequest());

        CronResponse response = action.newResponse(request, Arrays.asList(nodeResponse1, nodeResponse2), Collections.emptyList());

        assertEquals(2, response.getNodes().size());
        assertTrue(!response.hasFailures());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        String json = builder.toString();
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
}
