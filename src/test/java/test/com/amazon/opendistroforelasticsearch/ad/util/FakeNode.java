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

package test.com.amazon.opendistroforelasticsearch.ad.util;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.lucene.util.SetOnce;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.opensearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.BoundTransportAddress;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.tasks.TaskManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.tasks.MockTaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;

public class FakeNode implements Releasable {
    public FakeNode(String name, ThreadPool threadPool, Settings settings, TransportInterceptor transportInterceptor) {
        final Function<BoundTransportAddress, DiscoveryNode> boundTransportAddressDiscoveryNodeFunction = address -> {
            discoveryNode.set(new DiscoveryNode(name, address.publishAddress(), emptyMap(), emptySet(), Version.CURRENT));
            return discoveryNode.get();
        };
        transportService = new TransportService(
            settings,
            new MockNioTransport(
                settings,
                Version.CURRENT,
                threadPool,
                new NetworkService(Collections.emptyList()),
                PageCacheRecycler.NON_RECYCLING_INSTANCE,
                new NamedWriteableRegistry(ClusterModule.getNamedWriteables()),
                new NoneCircuitBreakerService()
            ) {
                @Override
                public TransportAddress[] addressesFromString(String address) {
                    return new TransportAddress[] { dns.getOrDefault(address, OpenSearchTestCase.buildNewFakeTransportAddress()) };
                }
            },
            threadPool,
            transportInterceptor,
            boundTransportAddressDiscoveryNodeFunction,
            null,
            Collections.emptySet()
        ) {
            @Override
            protected TaskManager createTaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
                if (MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.get(settings)) {
                    return new MockTaskManager(settings, threadPool, taskHeaders);
                } else {
                    return super.createTaskManager(settings, threadPool, taskHeaders);
                }
            }
        };

        transportService.start();
        clusterService = createClusterService(threadPool, discoveryNode.get());
        clusterService.addStateApplier(transportService.getTaskManager());
        ActionFilters actionFilters = new ActionFilters(emptySet());
        transportListTasksAction = new TransportListTasksAction(clusterService, transportService, actionFilters);
        transportCancelTasksAction = new TransportCancelTasksAction(clusterService, transportService, actionFilters);
        transportService.acceptIncomingRequests();
    }

    public FakeNode(String name, ThreadPool threadPool, Settings settings) {
        this(name, threadPool, settings, TransportService.NOOP_TRANSPORT_INTERCEPTOR);
    }

    public final ClusterService clusterService;
    public final TransportService transportService;
    private final SetOnce<DiscoveryNode> discoveryNode = new SetOnce<>();
    public final TransportListTasksAction transportListTasksAction;
    public final TransportCancelTasksAction transportCancelTasksAction;
    private final Map<String, TransportAddress> dns = new ConcurrentHashMap<>();

    @Override
    public void close() {
        clusterService.close();
        transportService.close();
    }

    public String getNodeId() {
        return discoveryNode().getId();
    }

    public DiscoveryNode discoveryNode() {
        return discoveryNode.get();
    }

    public static void connectNodes(FakeNode... nodes) {
        List<DiscoveryNode> discoveryNodes = new ArrayList<DiscoveryNode>(nodes.length);
        DiscoveryNode master = nodes[0].discoveryNode();
        for (int i = 0; i < nodes.length; i++) {
            discoveryNodes.add(nodes[i].discoveryNode());
        }

        for (FakeNode node : nodes) {
            setState(node.clusterService, ClusterCreation.state(new ClusterName("test"), node.discoveryNode(), master, discoveryNodes));
        }
        for (FakeNode nodeA : nodes) {
            for (FakeNode nodeB : nodes) {
                nodeA.transportService.connectToNode(nodeB.discoveryNode());
            }
        }
    }
}
