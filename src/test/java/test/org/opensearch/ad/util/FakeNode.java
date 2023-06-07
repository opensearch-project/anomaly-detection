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

package test.org.opensearch.ad.util;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.test.ClusterServiceUtils.createClusterService;
import static org.opensearch.test.ClusterServiceUtils.setState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
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
import org.opensearch.common.network.NetworkService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.BoundTransportAddress;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.lease.Releasable;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.tasks.TaskManager;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.tasks.MockTaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.nio.MockNioTransport;

public class FakeNode implements Releasable {
    protected static final Logger LOG = (Logger) LogManager.getLogger(FakeNode.class);

    public FakeNode(
        String name,
        ThreadPool threadPool,
        final Settings nodeSettings,
        final Set<Setting<?>> settingsSet,
        TransportInterceptor transportInterceptor,
        Version version
    ) {
        final Function<BoundTransportAddress, DiscoveryNode> boundTransportAddressDiscoveryNodeFunction = address -> {
            discoveryNode.set(new DiscoveryNode(name, address.publishAddress(), emptyMap(), emptySet(), Version.CURRENT));
            return discoveryNode.get();
        };
        transportService = new TransportService(
            Settings.EMPTY,
            new MockNioTransport(
                Settings.EMPTY,
                Version.V_2_1_0,
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
            protected TaskManager createTaskManager(
                Settings settings,
                ClusterSettings clusterSettings,
                ThreadPool threadPool,
                Set<String> taskHeaders
            ) {
                if (MockTaskManager.USE_MOCK_TASK_MANAGER_SETTING.get(settings)) {
                    return new MockTaskManager(settings, threadPool, taskHeaders);
                } else {
                    return super.createTaskManager(settings, clusterSettings, threadPool, taskHeaders);
                }
            }
        };

        transportService.start();
        Set<Setting<?>> internalSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        internalSettings.addAll(settingsSet);
        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, internalSettings);
        clusterService = createClusterService(threadPool, discoveryNode.get(), clusterSettings);
        clusterService.addStateApplier(transportService.getTaskManager());
        ActionFilters actionFilters = new ActionFilters(emptySet());
        taskResourceTrackingService = new TaskResourceTrackingService(nodeSettings, clusterService.getClusterSettings(), threadPool);
        transportListTasksAction = new TransportListTasksAction(
            clusterService,
            transportService,
            actionFilters,
            taskResourceTrackingService
        );
        transportCancelTasksAction = new TransportCancelTasksAction(clusterService, transportService, actionFilters);
        transportService.acceptIncomingRequests();
    }

    public FakeNode(String name, ThreadPool threadPool, Set<Setting<?>> settings) {
        this(name, threadPool, Settings.EMPTY, settings, TransportService.NOOP_TRANSPORT_INTERCEPTOR, Version.CURRENT);
    }

    public final ClusterService clusterService;
    public final TransportService transportService;
    public final TaskResourceTrackingService taskResourceTrackingService;
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
        DiscoveryNode clusterManager = nodes[0].discoveryNode();
        for (int i = 0; i < nodes.length; i++) {
            discoveryNodes.add(nodes[i].discoveryNode());
        }

        for (FakeNode node : nodes) {
            setState(
                node.clusterService,
                ClusterCreation.state(new ClusterName("test"), node.discoveryNode(), clusterManager, discoveryNodes)
            );
        }
        for (FakeNode nodeA : nodes) {
            for (FakeNode nodeB : nodes) {
                nodeA.transportService.connectToNode(nodeB.discoveryNode());
            }
        }
    }
}
