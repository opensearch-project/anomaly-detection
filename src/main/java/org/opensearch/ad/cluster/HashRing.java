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

package org.opensearch.ad.cluster;

import static org.opensearch.ad.constant.CommonName.AD_PLUGIN_NAME;
import static org.opensearch.ad.constant.CommonName.AD_PLUGIN_NAME_FOR_TEST;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.COOLDOWN_MINUTES;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.PluginInfo;

import com.google.common.collect.Sets;

public class HashRing {
    private static final Logger LOG = LogManager.getLogger(HashRing.class);
    static final String REBUILD_MSG = "Rebuild hash ring";
    // In case of frequent node join/leave, hash ring has a cooldown period say 5 minute.
    // Hash ring doesn't respond to more than 1 cluster membership changes within the
    // cool-down period.
    static final String COOLDOWN_MSG = "Hash ring doesn't respond to cluster state change within the cooldown period.";
    private static final String DEFAULT_HASH_RING_MODEL_ID = "DEFAULT_HASHRING_MODEL_ID";

    private final int VIRTUAL_NODE_COUNT = 100;

    // Semaphore to control only 1 thread can build AD hash ring.
    private Semaphore buildHashRingSemaphore;
    // This field is to track AD version of data node. Historical analysis will use this hash ring.
    // Key: node id; Value: AD version
    private Map<String, Version> nodeAdVersions;
    // This field records AD version hash ring in realtime way. Historical detection will use this hash ring.
    // Key: AD version; Value: hash ring
    private TreeMap<Version, TreeMap<Integer, DiscoveryNode>> circles;
    // Track if hash ring inited or not. If not inited, the first master event will try to init it.
    private AtomicBoolean hashRingInited;

    // the UTC epoch milliseconds of the most recent successful update of AD circles for realtime AD.
    private long lastUpdateForRealtimeAD;
    // Cool down period before next hash ring rebuild. We need this as realtime AD needs stable hash ring.
    private volatile TimeValue coolDownPeriodForRealtimeAD;
    // This field records AD version hash ring with cooldown period. Realtime job will use this hash ring.
    // Key: AD version; Value: hash ring
    private TreeMap<Version, TreeMap<Integer, DiscoveryNode>> circlesForRealtimeAD;

    // Record node change event. Will check if there is node change event when rebuild AD hash ring with
    // cooldown for realtime job.
    private ConcurrentLinkedQueue<Boolean> nodeChangeEvents;

    private final DiscoveryNodeFilterer nodeFilter;
    private final ClusterService clusterService;
    private final ADDataMigrator dataMigrator;
    private final Clock clock;
    private final Client client;

    public HashRing(
        DiscoveryNodeFilterer nodeFilter,
        Clock clock,
        Settings settings,
        Client client,
        ClusterService clusterService,
        ADDataMigrator dataMigrator
    ) {
        this.nodeFilter = nodeFilter;
        this.buildHashRingSemaphore = new Semaphore(1);
        this.clock = clock;
        this.coolDownPeriodForRealtimeAD = COOLDOWN_MINUTES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(COOLDOWN_MINUTES, it -> coolDownPeriodForRealtimeAD = it);

        this.lastUpdateForRealtimeAD = 0;
        this.client = client;
        this.clusterService = clusterService;
        this.dataMigrator = dataMigrator;
        this.nodeAdVersions = new ConcurrentHashMap<>();
        this.circles = new TreeMap<>();
        this.circlesForRealtimeAD = new TreeMap<>();
        this.hashRingInited = new AtomicBoolean(false);
        this.nodeChangeEvents = new ConcurrentLinkedQueue<>();
    }

    public boolean isHashRingInited() {
        return hashRingInited.get();
    }

    /**
     * Build AD version based circles with discovery node delta change. Listen to master event in
     * {@link ADClusterEventListener#clusterChanged(ClusterChangedEvent)}.
     * Will remove the removed nodes from cache and send request to newly added nodes to get their
     * plugin information; then add new nodes to AD version hash ring.
     *
     * @param delta discovery node delta change
     * @param listener action listener
     */
    public void buildCircles(DiscoveryNodes.Delta delta, ActionListener<Boolean> listener) {
        if (!buildHashRingSemaphore.tryAcquire()) {
            LOG.info("AD version hash ring change is in progress. Can't build hash ring for node delta event.");
            listener.onResponse(false);
            return;
        }
        Set<String> removedNodeIds = delta.removed()
            ? delta.removedNodes().stream().filter(nodeFilter::isEligibleDataNode).map(DiscoveryNode::getId).collect(Collectors.toSet())
            : null;
        Set<String> addedNodeIds = delta.added()
            ? delta.addedNodes().stream().filter(nodeFilter::isEligibleDataNode).map(DiscoveryNode::getId).collect(Collectors.toSet())
            : null;
        buildCircles(removedNodeIds, addedNodeIds, listener);
    }

    /**
     * Build AD version based circles by comparing with all eligible data nodes.
     * 1. Remove nodes which are not eligible now;
     * 2. Add nodes which are not in AD version circles.
     * @param actionListener action listener
     */
    public void buildCircles(ActionListener<Boolean> actionListener) {
        if (!buildHashRingSemaphore.tryAcquire()) {
            LOG.info("AD version hash ring change is in progress. Can't rebuild hash ring.");
            actionListener.onResponse(false);
            return;
        }
        DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
        Set<String> eligibleNodeIds = new HashSet<>();
        for (DiscoveryNode node : eligibleDataNodes) {
            eligibleNodeIds.add(node.getId());
        }
        Set<String> currentNodeIds = nodeAdVersions.keySet();
        Set<String> removedNodeIds = Sets.difference(currentNodeIds, eligibleNodeIds);
        Set<String> addedNodeIds = Sets.difference(eligibleNodeIds, currentNodeIds);
        buildCircles(removedNodeIds, addedNodeIds, actionListener);
    }

    public void buildCirclesForRealtimeAD() {
        if (nodeChangeEvents.isEmpty()) {
            return;
        }
        buildCircles(
            ActionListener
                .wrap(
                    r -> { LOG.debug("build circles on AD versions successfully"); },
                    e -> { LOG.error("Failed to build circles on AD versions", e); }
                )
        );
    }

    /**
     * Build AD version hash ring.
     * 1. Delete removed nodes from AD version hash ring.
     * 2. Add new nodes to AD version hash ring
     *
     * If fail to acquire semaphore to update AD version hash ring, will return false to
     * action listener; otherwise will return true. The "true" response just mean we got
     * semaphore and finished rebuilding hash ring, but the hash ring may stay the same.
     * Hash ring changed or not depends on if "removedNodeIds" or "addedNodeIds" is empty.
     *
     * We use different way to build hash ring for realtime job and historical analysis
     * 1. For historical analysis,if node removed, we remove it immediately from adVersionCircles
     *    to avoid new AD task routes to it. If new node added, we add it immediately to adVersionCircles
     *    to make load more balanced and speed up AD task running.
     * 2. For realtime job, we don't record which node running detector's model partition. We just
     *    use hash ring to get owning node. If we rebuild hash ring frequently, realtime job may get
     *    different owning node and need to restore model on new owning node. If that happens a lot,
     *    it may bring heavy load to cluster. So we prefer to wait for some time before next hash ring
     *    rebuild, we call it cooldown period. The cons is we may have stale hash ring during cooldown
     *    period. Some node may already been removed from hash ring, then realtime job won't know this
     *    and still send RCF request to it. If new node added during cooldown period, realtime job won't
     *    choose it as model partition owning node, thus we may have skewed load on data nodes.
     *
     * [Important!]: When you call this function, make sure you TRY ACQUIRE adVersionCircleInProgress first.
     *               Check {@link HashRing#buildCircles(ActionListener)} and
     *               {@link HashRing#buildCircles(DiscoveryNodes.Delta, ActionListener)}
     *
     * @param removedNodeIds removed node ids
     * @param addedNodeIds added node ids
     * @param actionListener action listener
     */
    private void buildCircles(Set<String> removedNodeIds, Set<String> addedNodeIds, ActionListener<Boolean> actionListener) {
        if (buildHashRingSemaphore.availablePermits() != 0) {
            throw new AnomalyDetectionException("Must get update hash ring semaphore before building AD hash ring");
        }
        try {
            DiscoveryNode localNode = clusterService.localNode();
            if (removedNodeIds != null && removedNodeIds.size() > 0) {
                LOG.info("Remove nodes from AD version hash ring: {}", Arrays.toString(removedNodeIds.toArray(new String[0])));
                for (String nodeId : removedNodeIds) {
                    removeNodeFromCircles(nodeId);
                }
            }
            Set<String> allAddedNodes = new HashSet<>();

            if (addedNodeIds != null) {
                allAddedNodes.addAll(addedNodeIds);
            }
            if (nodeFilter.isEligibleNode(localNode) && !nodeAdVersions.containsKey(localNode.getId())) {
                allAddedNodes.add(localNode.getId());
            }
            if (allAddedNodes.size() == 0) {
                actionListener.onResponse(true);
                LOG.info("No newly added nodes, return");
                // rebuild AD version hash ring with cooldown.
                rebuildCirclesForRealtimeAD();
                buildHashRingSemaphore.release();
                return;
            }

            LOG.info("Add nodes to AD version hash ring: {}", Arrays.toString(allAddedNodes.toArray(new String[0])));
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
            nodesInfoRequest.nodesIds(allAddedNodes.toArray(new String[0]));
            nodesInfoRequest.clear().addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());

            AdminClient admin = client.admin();
            ClusterAdminClient cluster = admin.cluster();
            cluster.nodesInfo(nodesInfoRequest, ActionListener.wrap(r -> {
                Map<String, NodeInfo> nodesMap = r.getNodesMap();
                if (nodesMap != null && nodesMap.size() > 0) {
                    for (Map.Entry<String, NodeInfo> entry : nodesMap.entrySet()) {
                        NodeInfo nodeInfo = entry.getValue();
                        PluginsAndModules plugins = nodeInfo.getInfo(PluginsAndModules.class);
                        if (plugins == null) {
                            continue;
                        }
                        DiscoveryNode curNode = nodeInfo.getNode();
                        if (!nodeFilter.isEligibleDataNode(curNode)) {
                            continue;
                        }
                        TreeMap<Integer, DiscoveryNode> circle = null;
                        for (PluginInfo pluginInfo : plugins.getPluginInfos()) {
                            if (AD_PLUGIN_NAME.equals(pluginInfo.getName()) || AD_PLUGIN_NAME_FOR_TEST.equals(pluginInfo.getName())) {
                                Version version = ADVersionUtil.fromString(pluginInfo.getVersion());
                                circle = circles.computeIfAbsent(version, key -> new TreeMap<>());
                                nodeAdVersions.put(curNode.getId(), ADVersionUtil.fromString(pluginInfo.getVersion()));
                                break;
                            }
                        }
                        if (circle != null) {
                            for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                                circle.put(Murmur3HashFunction.hash(curNode.getId() + i), curNode);
                            }
                        }
                    }
                }
                LOG.info("All nodes in AD version hash ring: {}", nodeAdVersions);

                // rebuild AD version hash ring with cooldown after all new node added.
                rebuildCirclesForRealtimeAD();

                if (!dataMigrator.isMigrated() && circles.size() > 0 && circles.lastEntry().getKey().after(Version.V_1_0_0)) {
                    // Find owning node with highest AD version to make sure the data migration logic be compatible to
                    // latest AD version when upgrade.
                    Optional<DiscoveryNode> owningNode = getOwningNodeWithHighestAdVersion(DEFAULT_HASH_RING_MODEL_ID);
                    String localNodeId = localNode.getId();
                    if (owningNode.isPresent() && localNodeId.equals(owningNode.get().getId())) {
                        dataMigrator.migrateData();
                    } else {
                        dataMigrator.skipMigration();
                    }
                }
                buildHashRingSemaphore.release();
                hashRingInited.set(true);
                actionListener.onResponse(true);
            }, e -> {
                buildHashRingSemaphore.release();
                actionListener.onFailure(e);
                LOG.error("Fail to get node info to build AD version hash ring", e);
            }));
        } catch (Exception e) {
            LOG.error("Failed to build AD version circles", e);
            buildHashRingSemaphore.release();
            actionListener.onFailure(e);
        }
    }

    private void removeNodeFromCircles(String nodeId) {
        Version adVersion = this.nodeAdVersions.remove(nodeId);
        if (adVersion != null) {
            TreeMap<Integer, DiscoveryNode> circle = this.circles.get(adVersion);
            List<Integer> deleted = new ArrayList<>();
            for (Map.Entry<Integer, DiscoveryNode> entry : circle.entrySet()) {
                if (entry.getValue().getId().equals(nodeId)) {
                    deleted.add(entry.getKey());
                }
            }
            if (deleted.size() == circle.size()) {
                circles.remove(adVersion);
            } else {
                for (Integer key : deleted) {
                    circle.remove(key);
                }
            }
        }
    }

    private void rebuildCirclesForRealtimeAD() {
        // Check if it's eligible to rebuild hash ring with cooldown
        if (eligibleToRebuildCirclesForRealtimeAD()) {
            LOG.info("Rebuild AD hash ring for realtime AD with cooldown, nodeChangeEvents size {}", nodeChangeEvents.size());
            int size = nodeChangeEvents.size();
            TreeMap<Version, TreeMap<Integer, DiscoveryNode>> newCircles = new TreeMap<>();
            for (Map.Entry<Version, TreeMap<Integer, DiscoveryNode>> entry : circles.entrySet()) {
                newCircles.put(entry.getKey(), new TreeMap<>(entry.getValue()));
            }
            circlesForRealtimeAD = newCircles;
            lastUpdateForRealtimeAD = clock.millis();
            // It's possible that multiple threads add new event to nodeChangeEvents,
            // but this is the only place to consume/poll the event and there is only
            // one thread poll it as we are using adVersionCircleInProgress semaphore(1)
            // to control only 1 thread build hash ring.
            while (size-- > 0) {
                Boolean poll = nodeChangeEvents.poll();
                if (poll == null) {
                    break;
                }
            }
        }
    }

    /**
     * Check if it's eligible to rebuilt hash ring now.
     * It's eligible if:
     * 1. There is node change event not consumed, and
     * 2. Have passed cool down period from last hash ring update time.
     *
     * Check {@link org.opensearch.ad.settings.AnomalyDetectorSettings#COOLDOWN_MINUTES} about
     * cool down settings.
     *
     * Why we need to wait for some cooldown period before rebuilding hash ring?
     *    This is for realtime detection. In realtime detection, we rely on hash ring to get
     *    owning node for RCF model partitions. It's stateless, that means we don't record
     *    which node is running RCF partition for the detector. That requires a stable hash
     *    ring. If hash ring changes, it's possible that the next job run will use a different
     *    node to run RCF partition. Then we need to restore model on the new node and clean up
     *    old model partitions on old node. That model migration between nodes may bring heavy
     *    load to cluster.
     *
     * @return true if it's eligible to rebuild hash ring
     */
    protected boolean eligibleToRebuildCirclesForRealtimeAD() {
        // Check if there is any node change event
        if (nodeChangeEvents.isEmpty() && !circlesForRealtimeAD.isEmpty()) {
            return false;
        }

        // Check cooldown period
        if (clock.millis() - lastUpdateForRealtimeAD <= coolDownPeriodForRealtimeAD.getMillis()) {
            LOG.debug(COOLDOWN_MSG);
            return false;
        }
        return true;
    }

    /**
     * Get owning node with highest AD version circle.
     * @param modelId model id
     * @return owning node
     */
    public Optional<DiscoveryNode> getOwningNodeWithHighestAdVersion(String modelId) {
        int modelHash = Murmur3HashFunction.hash(modelId);
        Map.Entry<Version, TreeMap<Integer, DiscoveryNode>> versionTreeMapEntry = circles.lastEntry();
        if (versionTreeMapEntry == null) {
            return Optional.empty();
        }
        TreeMap<Integer, DiscoveryNode> adVersionCircle = versionTreeMapEntry.getValue();
        Map.Entry<Integer, DiscoveryNode> entry = adVersionCircle.higherEntry(modelHash);
        return Optional.ofNullable(Optional.ofNullable(entry).orElse(adVersionCircle.firstEntry())).map(x -> x.getValue());
    }

    /**
     * Get owning node with same AD version of local node.
     * @param modelId model id
     * @param function consumer function
     * @param listener action listener
     * @param <T> listener response type
     */
    public <T> void buildAndGetOwningNodeWithSameLocalAdVersion(
        String modelId,
        Consumer<Optional<DiscoveryNode>> function,
        ActionListener<T> listener
    ) {
        buildCircles(ActionListener.wrap(r -> {
            DiscoveryNode localNode = clusterService.localNode();
            Version adVersion = nodeAdVersions.containsKey(localNode.getId()) ? getAdVersion(localNode.getId()) : Version.CURRENT;
            Optional<DiscoveryNode> owningNode = getOwningNodeWithSameAdVersionDirectly(modelId, adVersion, false);
            function.accept(owningNode);
        }, e -> listener.onFailure(e)));
    }

    public Optional<DiscoveryNode> getOwningNodeWithSameLocalAdVersionForRealtimeAD(String modelId) {
        try {
            DiscoveryNode localNode = clusterService.localNode();
            Version adVersion = nodeAdVersions.containsKey(localNode.getId()) ? getAdVersion(localNode.getId()) : Version.CURRENT;
            Optional<DiscoveryNode> owningNode = getOwningNodeWithSameAdVersionDirectly(modelId, adVersion, true);
            // rebuild hash ring
            buildCirclesForRealtimeAD();
            return owningNode;
        } catch (Exception e) {
            LOG.error("Failed to get owning node with same local AD version", e);
            return Optional.empty();
        }
    }

    private Optional<DiscoveryNode> getOwningNodeWithSameAdVersionDirectly(String modelId, Version adVersion, boolean forRealtime) {
        int modelHash = Murmur3HashFunction.hash(modelId);
        TreeMap<Integer, DiscoveryNode> adVersionCircle = forRealtime ? circlesForRealtimeAD.get(adVersion) : circles.get(adVersion);
        if (adVersionCircle != null) {
            Map.Entry<Integer, DiscoveryNode> entry = adVersionCircle.higherEntry(modelHash);
            return Optional.ofNullable(Optional.ofNullable(entry).orElse(adVersionCircle.firstEntry())).map(x -> x.getValue());
        }
        return Optional.empty();
    }

    public <T> void getNodesWithSameLocalAdVersion(Consumer<DiscoveryNode[]> function, ActionListener<T> listener) {
        buildCircles(ActionListener.wrap(updated -> {
            DiscoveryNode localNode = clusterService.localNode();
            Version adVersion = nodeAdVersions.containsKey(localNode.getId()) ? getAdVersion(localNode.getId()) : Version.CURRENT;
            Set<DiscoveryNode> nodes = getNodesWithSameAdVersion(adVersion, false);
            if (!nodeAdVersions.containsKey(localNode.getId())) {
                nodes.add(localNode);
            }
            // Make sure listener return in function
            function.accept(nodes.toArray(new DiscoveryNode[0]));
        }, e -> listener.onFailure(e)));
    }

    public DiscoveryNode[] getNodesWithSameLocalAdVersion() {
        DiscoveryNode localNode = clusterService.localNode();
        Version adVersion = nodeAdVersions.containsKey(localNode.getId()) ? getAdVersion(localNode.getId()) : Version.CURRENT;
        Set<DiscoveryNode> nodes = getNodesWithSameAdVersion(adVersion, false);
        // rebuild hash ring
        buildCirclesForRealtimeAD();
        return nodes.toArray(new DiscoveryNode[0]);
    }

    protected Set<DiscoveryNode> getNodesWithSameAdVersion(Version adVersion, boolean forRealtime) {
        TreeMap<Integer, DiscoveryNode> circle = forRealtime ? circlesForRealtimeAD.get(adVersion) : circles.get(adVersion);
        Set<String> nodeIds = new HashSet<>();
        Set<DiscoveryNode> nodes = new HashSet<>();
        if (circle == null) {
            return nodes;
        }
        circle.entrySet().stream().forEach(e -> {
            DiscoveryNode discoveryNode = e.getValue();
            if (!nodeIds.contains(discoveryNode.getId())) {
                nodeIds.add(discoveryNode.getId());
                nodes.add(discoveryNode);
            }
        });
        return nodes;
    }

    /**
     * Get AD version.
     * @param nodeId node id
     * @return AD version
     */
    public Version getAdVersion(String nodeId) {
        return nodeAdVersions.get(nodeId);
    }

    /**
     * Get node by transport address.
     * If transport address is null, return local node; otherwise, filter current eligible data nodes
     * with IP address. If no node found, will return Optional.empty()
     *
     * @param address transport address
     * @return discovery node
     */
    public Optional<DiscoveryNode> getNodeByAddress(TransportAddress address) {
        if (address == null) {
            // If remote address of transport request is null, that means remote node is local node.
            return Optional.of(clusterService.localNode());
        }
        String ipAddress = getIpAddress(address);
        DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
        for (DiscoveryNode node : eligibleDataNodes) {
            if (getIpAddress(node.getAddress()).equals(ipAddress)) {
                return Optional.ofNullable(node);
            }
        }
        return Optional.empty();
    }

    /**
     * Get IP address from transport address.
     * TransportAddress.toString() example: 100.200.100.200:12345
     * @param address transport address
     * @return IP address
     */
    private String getIpAddress(TransportAddress address) {
        return address.toString().split(":")[0];
    }

    /**
     * Get all eligible data nodes whose AD versions are known in AD version based hash ring.
     * @param function consumer function
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getAllEligibleDataNodesWithKnownAdVersion(Consumer<DiscoveryNode[]> function, ActionListener<T> listener) {
        buildCircles(ActionListener.wrap(r -> {
            DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
            List<DiscoveryNode> allNodes = new ArrayList<>();
            for (DiscoveryNode node : eligibleDataNodes) {
                if (nodeAdVersions.containsKey(node.getId())) {
                    allNodes.add(node);
                }
            }
            // Make sure listener return in function
            function.accept(allNodes.toArray(new DiscoveryNode[0]));
        }, e -> listener.onFailure(e)));
    }

    /**
     * Put node change events in node change event queue. Will poll event from this queue when rebuild hash ring
     * for realtime task.
     * We track all node change events in case some race condition happen and we miss adding some node to hash
     * ring.
     */
    public void addNodeChangeEvent() {
        this.nodeChangeEvents.add(true);
    }
}
