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
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
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
    private final DiscoveryNodeFilterer nodeFilter;
    private TreeMap<Integer, DiscoveryNode> circle;
    private Semaphore inProgress;
    private Semaphore adVersionCircleInProgress;
    // the UTC epoch milliseconds of the most recent successful update
    private long lastUpdate;
    private final TimeValue coolDownPeriod;
    private final Clock clock;
    private AtomicBoolean membershipChangeRequied;
    private final Client client;
    private Map<String, Version> nodeAdVersions;
    private TreeMap<Version, TreeMap<Integer, DiscoveryNode>> adVersionCircles;
    private ClusterService clusterService;
    private ADDataMigrator dataMigrator;
    private AtomicBoolean adVersionHashRingInited;

    public HashRing(
        DiscoveryNodeFilterer nodeFilter,
        Clock clock,
        Settings settings,
        Client client,
        ClusterService clusterService,
        ADDataMigrator dataMigrator
    ) {
        this.circle = new TreeMap<>();
        this.nodeFilter = nodeFilter;
        this.inProgress = new Semaphore(1);
        this.adVersionCircleInProgress = new Semaphore(1);
        this.clock = clock;
        this.coolDownPeriod = COOLDOWN_MINUTES.get(settings);
        this.lastUpdate = 0;
        this.membershipChangeRequied = new AtomicBoolean(false);
        this.client = client;
        this.clusterService = clusterService;
        this.dataMigrator = dataMigrator;
        this.nodeAdVersions = new ConcurrentHashMap<>();
        this.adVersionCircles = new TreeMap<>();
        this.adVersionHashRingInited = new AtomicBoolean(false);
    }

    public boolean isAdVersionHashRingInited() {
        return adVersionHashRingInited.get();
    }

    /**
     * Rebuilds the hash ring when cluster membership change is required.
     * The build method skips a rebuilding if it has already rebuilt the hash ring within the
     *  cooldown period or the rebuilding is already in progress.
     * @return whether hash ring is rebuilt or not.
     */
    public boolean build() {
        // Check this conjunct first since most of time this conjunct evaluates
        // to false and we can skip of the following checks.
        // Hash ring can be empty because we cannot build the ring in constructor. The constructor
        // is called when the plugin is being loaded. At that time, cluster state is empty.
        if (!membershipChangeRequied.get() && !circle.isEmpty()) {
            return false;
        }

        // TODO: Remove cooldown period? If nodes start up quickly, we may miss some data node.
        // Check cooldown period
        if (clock.millis() - lastUpdate <= coolDownPeriod.getMillis()) {
            LOG.debug(COOLDOWN_MSG);
            return false;
        }

        // When the condition check passes, we start hash ring rebuilding.
        if (!inProgress.tryAcquire()) {
            LOG.info("Hash ring change in progress, return.");
            return false;
        }

        LOG.info(REBUILD_MSG);
        TreeMap<Integer, DiscoveryNode> newCircle = new TreeMap<>();

        try {
            for (DiscoveryNode curNode : nodeFilter.getEligibleDataNodes()) {
                for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                    newCircle.put(Murmur3HashFunction.hash(curNode.getId() + i), curNode);
                }
            }
            circle = newCircle;
            lastUpdate = clock.millis();
            membershipChangeRequied.set(false);
        } catch (Exception ex) {
            LOG.error("Hash ring cannot be rebuilt", ex);
            return false;
        } finally {
            inProgress.release();
        }
        return true;
    }

    /**
     * Compute the owning node of modelID using consistent hashing
     * @param modelId example: http-latency-rcf-1
     * @return the owning node of the modeID
     */
    public Optional<DiscoveryNode> getOwningNode(String modelId) {
        build();

        int modelHash = Murmur3HashFunction.hash(modelId);
        Map.Entry<Integer, DiscoveryNode> entry = circle.higherEntry(modelHash);

        // The method can return an empty Optional. Say two concurrent getOwningNode requests to
        // the hash ring before it's been built. The first one starts building it,
        // turning on inProgress. The second one returns from build and continues on to
        // the rest of hashing and look up while the ring is still being built and thus empty.
        // The second getOwningNode request returns an empty Optional in this case.
        return Optional.ofNullable(Optional.ofNullable(entry).orElse(circle.firstEntry())).map(x -> x.getValue());
    }

    public void recordMembershipChange() {
        membershipChangeRequied.set(true);
    }

    /**
     * Build AD version based circles with discovery node delta change. Listen to master event in
     * {@link ADClusterEventListener#clusterChanged(ClusterChangedEvent)}.
     * Will remove the removed nodes from cache and send request to newly added nodes to get their plugin information,and.
     *
     * @param delta discovery node delta change
     */
    public void buildCirclesOnAdVersions(DiscoveryNodes.Delta delta) {
        Set<String> removedNodeIds = delta.removed()
            ? delta.removedNodes().stream().filter(nodeFilter::isEligibleDataNode).map(DiscoveryNode::getId).collect(Collectors.toSet())
            : null;
        Set<String> addedNodeIds = delta.added()
            ? delta.addedNodes().stream().filter(nodeFilter::isEligibleDataNode).map(DiscoveryNode::getId).collect(Collectors.toSet())
            : null;
        buildCirclesOnAdVersions(removedNodeIds, addedNodeIds, ActionListener.wrap(updated -> {
            if (updated) {
                LOG.info("Build circles on AD versions successfully");
            } else {
                LOG.debug("Skip updating circles on AD versions");
            }
        }, e -> { LOG.error("Failed updating circles on AD versions", e); }));
    }

    /**
     * Build AD version based circles by comparing with all eligible data nodes.
     * 1. Remove nodes which are not eligible now;
     * 2. Add nodes which are not in AD version circles.
     * @param actionListener action listener
     */
    public void buildCirclesOnAdVersions(ActionListener<Boolean> actionListener) {
        DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
        Set<String> eligibleNodeIds = new HashSet<>();
        for (DiscoveryNode node : eligibleDataNodes) {
            eligibleNodeIds.add(node.getId());
        }
        Set<String> currentNodeIds = nodeAdVersions.keySet();
        Set<String> removedNodeIds = Sets.difference(currentNodeIds, eligibleNodeIds);
        Set<String> addedNodeIds = Sets.difference(eligibleNodeIds, currentNodeIds);
        buildCirclesOnAdVersions(removedNodeIds, addedNodeIds, actionListener);
    }

    public void buildCirclesOnAdVersions(Set<String> removedNodeIds, Set<String> addedNodeIds, ActionListener<Boolean> actionListener) {
        if (!adVersionCircleInProgress.tryAcquire()) {
            LOG.info("AD version hash ring change in progress, return.");
            actionListener.onResponse(false);
            return;
        }
        try {
            DiscoveryNode localNode = clusterService.localNode();
            if (removedNodeIds != null && removedNodeIds.size() > 0) {
                LOG.info("Remove nodes from AD version hash ring: {}", Arrays.toString(removedNodeIds.toArray(new String[0])));
                for (String nodeId : removedNodeIds) {
                    removeNodeFromAdVersionCircles(nodeId);
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
                adVersionCircleInProgress.release();
                return;
            }

            LOG.info("Add nodes to AD version hash ring: {}", Arrays.toString(allAddedNodes.toArray(new String[0])));
            NodesInfoRequest nodesInfoRequest = new NodesInfoRequest();
            nodesInfoRequest.nodesIds(allAddedNodes.toArray(new String[0]));
            nodesInfoRequest.clear().addMetric(NodesInfoRequest.Metric.PLUGINS.metricName());

            client.admin().cluster().nodesInfo(nodesInfoRequest, ActionListener.wrap(r -> {
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
                                circle = adVersionCircles.computeIfAbsent(version, key -> new TreeMap<>());
                                nodeAdVersions.computeIfAbsent(curNode.getId(), key -> ADVersionUtil.fromString(pluginInfo.getVersion()));
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

                if (adVersionCircles.size() > 0 && adVersionCircles.lastEntry().getKey().after(Version.V_1_0_0)) {
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
                adVersionCircleInProgress.release();
                adVersionHashRingInited.set(true);
                actionListener.onResponse(true);
            }, e -> {
                adVersionCircleInProgress.release();
                actionListener.onFailure(e);
                LOG.error("Fail to get node info to build AD version hash ring", e);
            }));
        } catch (Exception e) {
            LOG.error("Failed to build AD version circles", e);
            adVersionCircleInProgress.release();
        }
    }

    private void removeNodeFromAdVersionCircles(String nodeId) {
        Version adVersion = this.nodeAdVersions.remove(nodeId);
        if (adVersion != null) {
            TreeMap<Integer, DiscoveryNode> circle = this.adVersionCircles.get(adVersion);
            List<Integer> deleted = new ArrayList<>();
            for (Map.Entry<Integer, DiscoveryNode> entry : circle.entrySet()) {
                if (entry.getValue().getId().equals(nodeId)) {
                    deleted.add(entry.getKey());
                }
            }
            if (deleted.size() == circle.size()) {
                adVersionCircles.remove(adVersion);
            } else {
                for (Integer key : deleted) {
                    circle.remove(key);
                }
            }
        }
    }

    /**
     * Get owning node with highest AD version circle.
     * @param modelId model id
     * @return owning node
     */
    public Optional<DiscoveryNode> getOwningNodeWithHighestAdVersion(String modelId) {
        int modelHash = Murmur3HashFunction.hash(modelId);
        Map.Entry<Version, TreeMap<Integer, DiscoveryNode>> versionTreeMapEntry = adVersionCircles.lastEntry();
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
     */
    /**
     * Get owning node with same AD version of local node.
     * @param modelId model id
     * @param function consumer function
     * @param listener action listener
     * @param <T> listener response type
     */
    public <T> void getOwningNodeWithSameLocalAdVersion(
        String modelId,
        Consumer<Optional<DiscoveryNode>> function,
        ActionListener<T> listener
    ) {
        buildCirclesOnAdVersions(ActionListener.wrap(r -> {
            DiscoveryNode localNode = clusterService.localNode();
            Version adVersion = nodeAdVersions.containsKey(localNode.getId()) ? getAdVersion(localNode.getId()) : Version.CURRENT;
            Optional<DiscoveryNode> owningNode = getOwningNodeWithSameAdVersion(modelId, adVersion);
            function.accept(owningNode);
        }, e -> listener.onFailure(e)));
    }

    private Optional<DiscoveryNode> getOwningNodeWithSameAdVersion(String modelId, Version adVersion) {
        int modelHash = Murmur3HashFunction.hash(modelId);
        Map.Entry<Integer, DiscoveryNode> entry = adVersionCircles.get(adVersion).higherEntry(modelHash);
        return Optional.ofNullable(Optional.ofNullable(entry).orElse(circle.firstEntry())).map(x -> x.getValue());
    }

    public <T> void getNodesWithSameLocalAdVersion(Consumer<DiscoveryNode[]> function, ActionListener<T> listener) {
        buildCirclesOnAdVersions(ActionListener.wrap(updated -> {
            DiscoveryNode localNode = clusterService.localNode();
            Version adVersion = nodeAdVersions.containsKey(localNode.getId()) ? getAdVersion(localNode.getId()) : Version.CURRENT;
            Set<DiscoveryNode> nodes = getNodesWithSameAdVersion(adVersion);
            if (!nodeAdVersions.containsKey(localNode.getId())) {
                nodes.add(localNode);
            }
            function.accept(nodes.toArray(new DiscoveryNode[0]));
        }, e -> listener.onFailure(e)));
    }

    private Set<DiscoveryNode> getNodesWithSameAdVersion(Version adVersion) {
        TreeMap<Integer, DiscoveryNode> circle = adVersionCircles.get(adVersion);
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
     * Get all eligible data nodes which AD version known in AD version based hash ring.
     * @param function consumer function
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getAllEligibleDataNodesWithKnownAdVersion(Consumer<DiscoveryNode[]> function, ActionListener<T> listener) {
        buildCirclesOnAdVersions(ActionListener.wrap(r -> {
            DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
            List<DiscoveryNode> allNodes = new ArrayList<>();
            for (DiscoveryNode node : eligibleDataNodes) {
                if (nodeAdVersions.containsKey(node.getId())) {
                    allNodes.add(node);
                }
            }
            function.accept(allNodes.toArray(new DiscoveryNode[0]));
        }, e -> listener.onFailure(e)));
    }
}
