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

package org.opensearch.timeseries.cluster;

import static org.opensearch.ad.constant.ADCommonName.AD_PLUGIN_NAME;
import static org.opensearch.ad.constant.ADCommonName.AD_PLUGIN_NAME_FOR_TEST;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_COOLDOWN_MINUTES;

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
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

import com.google.common.collect.Sets;

public class HashRing {
    private static final Logger LOG = LogManager.getLogger(HashRing.class);
    // In case of frequent node join/leave, hash ring has a cooldown period say 5 minute.
    // Hash ring doesn't respond to more than 1 cluster membership changes within the
    // cool-down period.
    static final String COOLDOWN_MSG = "Hash ring doesn't respond to cluster state change within the cooldown period.";
    private static final String DEFAULT_HASH_RING_MODEL_ID = "DEFAULT_HASHRING_MODEL_ID";
    static final String REMOVE_MODEL_MSG = "Remove model";

    private final int VIRTUAL_NODE_COUNT = 100;

    // Semaphore to control only 1 thread can build AD hash ring.
    private Semaphore buildHashRingSemaphore;
    // This field is to track time series plugin version of all nodes.
    // Key: node id; Value: node info
    private Map<String, TimeSeriesNodeInfo> nodeVersions;
    // This field records time series version hash ring in realtime way. Historical detection will use this hash ring.
    // Key: time series version; Value: hash ring which only contains eligible data nodes
    private TreeMap<Version, TreeMap<Integer, DiscoveryNode>> circles;
    // Track if hash ring inited or not. If not inited, the first clusterManager event will try to init it.
    private AtomicBoolean hashRingInited;

    // the UTC epoch milliseconds of the most recent successful update of AD circles for realtime AD.
    private long lastUpdateForRealtimeAD;
    // Cool down period before next hash ring rebuild. We need this as realtime AD needs stable hash ring.
    private volatile TimeValue coolDownPeriodForRealtimeAD;
    // This field records time series version hash ring with cooldown period. Realtime job will use this hash ring.
    // Key: time series version; Value: hash ring which only contains eligible data nodes
    private TreeMap<Version, TreeMap<Integer, DiscoveryNode>> circlesForRealtimeAD;

    // Record node change event. Will check if there is node change event when rebuild AD hash ring with
    // cooldown for realtime job.
    private ConcurrentLinkedQueue<Boolean> nodeChangeEvents;

    private final DiscoveryNodeFilterer nodeFilter;
    private final ClusterService clusterService;
    private final ADDataMigrator dataMigrator;
    private final Clock clock;
    private final Client client;
    private final ADModelManager modelManager;

    public HashRing(
        DiscoveryNodeFilterer nodeFilter,
        Clock clock,
        Settings settings,
        Client client,
        ClusterService clusterService,
        ADDataMigrator dataMigrator,
        ADModelManager modelManager
    ) {
        this.nodeFilter = nodeFilter;
        this.buildHashRingSemaphore = new Semaphore(1);
        this.clock = clock;
        this.coolDownPeriodForRealtimeAD = AD_COOLDOWN_MINUTES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_COOLDOWN_MINUTES, it -> coolDownPeriodForRealtimeAD = it);

        this.lastUpdateForRealtimeAD = 0;
        this.client = client;
        this.clusterService = clusterService;
        this.dataMigrator = dataMigrator;
        this.nodeVersions = new ConcurrentHashMap<>();
        this.circles = new TreeMap<>();
        this.circlesForRealtimeAD = new TreeMap<>();
        this.hashRingInited = new AtomicBoolean(false);
        this.nodeChangeEvents = new ConcurrentLinkedQueue<>();
        this.modelManager = modelManager;
    }

    public boolean isHashRingInited() {
        return hashRingInited.get();
    }

    /**
     * Build version based circles with discovery node delta change. Listen to clusterManager event in
     * {@link ClusterEventListener#clusterChanged(ClusterChangedEvent)}.
     * Will remove the removed nodes from cache and send request to newly added nodes to get their
     * plugin information; then add new nodes to version hash ring.
     *
     * @param delta discovery node delta change
     * @param listener action listener
     */
    public void buildCircles(DiscoveryNodes.Delta delta, ActionListener<Boolean> listener) {
        if (!buildHashRingSemaphore.tryAcquire()) {
            LOG.info("hash ring change is in progress. Can't build hash ring for node delta event.");
            listener.onResponse(false);
            return;
        }
        Set<String> removedNodeIds = delta.removed()
            ? delta.removedNodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet())
            : null;
        Set<String> addedNodeIds = delta.added() ? delta.addedNodes().stream().map(DiscoveryNode::getId).collect(Collectors.toSet()) : null;
        buildCircles(removedNodeIds, addedNodeIds, listener);
    }

    /**
     * Build version based circles by comparing with all eligible data nodes.
     * 1. Remove nodes which are not eligible now;
     * 2. Add nodes which are not in version circles.
     * @param actionListener action listener
     */
    public void buildCircles(ActionListener<Boolean> actionListener) {
        if (!buildHashRingSemaphore.tryAcquire()) {
            LOG.info("hash ring change is in progress. Can't rebuild hash ring.");
            actionListener.onResponse(false);
            return;
        }
        DiscoveryNode[] allNodes = nodeFilter.getAllNodes();
        Set<String> nodeIds = new HashSet<>();
        for (DiscoveryNode node : allNodes) {
            nodeIds.add(node.getId());
        }
        Set<String> currentNodeIds = nodeVersions.keySet();
        Set<String> removedNodeIds = Sets.difference(currentNodeIds, nodeIds);
        Set<String> addedNodeIds = Sets.difference(nodeIds, currentNodeIds);
        buildCircles(removedNodeIds, addedNodeIds, actionListener);
    }

    public void buildCirclesForRealtime() {
        if (nodeChangeEvents.isEmpty()) {
            return;
        }
        buildCircles(
            ActionListener.wrap(r -> { LOG.debug("build circles successfully"); }, e -> { LOG.error("Failed to build circles", e); })
        );
    }

    /**
     * Build version hash ring.
     * 1. Delete removed nodes from version hash ring.
     * 2. Add new nodes to version hash ring
     *
     * If fail to acquire semaphore to update version hash ring, will return false to
     * action listener; otherwise will return true. The "true" response just mean we got
     * semaphore and finished rebuilding hash ring, but the hash ring may stay the same.
     * Hash ring changed or not depends on if "removedNodeIds" or "addedNodeIds" is empty.
     *
     * We use different way to build hash ring for realtime job and historical analysis
     * 1. For historical analysis,if node removed, we remove it immediately from version circles
     *    to avoid new task routes to it. If new node added, we add it immediately to version circles
     *    to make load more balanced and speed up task running.
     * 2. For realtime job, we don't record which node running detector's model partition. We just
     *    use hash ring to get owning node. If we rebuild hash ring frequently, realtime job may get
     *    different owning node and need to restore model on new owning node. If that happens a lot,
     *    it may bring heavy load to cluster. So we prefer to wait for some time before next hash ring
     *    rebuild, we call it cooldown period. The cons is we may have stale hash ring during cooldown
     *    period. Some node may already been removed from hash ring, then realtime job won't know this
     *    and still send RCF request to it. If new node added during cooldown period, realtime job won't
     *    choose it as model partition owning node, thus we may have skewed load on data nodes.
     *
     * [Important!]: When you call this function, make sure you TRY ACQUIRE buildHashRingSemaphore first.
     *               Check {@link HashRing#buildCircles(ActionListener)} and
     *               {@link HashRing#buildCircles(DiscoveryNodes.Delta, ActionListener)}
     *
     * @param removedNodeIds removed node ids
     * @param addedNodeIds added node ids
     * @param actionListener action listener
     */
    private void buildCircles(Set<String> removedNodeIds, Set<String> addedNodeIds, ActionListener<Boolean> actionListener) {
        if (buildHashRingSemaphore.availablePermits() != 0) {
            throw new TimeSeriesException("Must get update hash ring semaphore before building AD hash ring");
        }
        try {
            DiscoveryNode localNode = clusterService.localNode();
            if (removedNodeIds != null && removedNodeIds.size() > 0) {
                LOG.info("Node removed: {}", Arrays.toString(removedNodeIds.toArray(new String[0])));
                for (String nodeId : removedNodeIds) {
                    TimeSeriesNodeInfo nodeInfo = nodeVersions.remove(nodeId);
                    if (nodeInfo != null && nodeInfo.isEligibleDataNode()) {
                        removeNodeFromCircles(nodeId, nodeInfo.getVersion());
                        LOG.info("Remove data node from version hash ring: {}", nodeId);
                    }
                }
            }
            Set<String> allAddedNodes = new HashSet<>();

            if (addedNodeIds != null) {
                allAddedNodes.addAll(addedNodeIds);
            }
            if (!nodeVersions.containsKey(localNode.getId())) {
                allAddedNodes.add(localNode.getId());
            }
            if (allAddedNodes.size() == 0) {
                actionListener.onResponse(true);
                // rebuild version hash ring with cooldown.
                rebuildCirclesForRealtimeAD();
                buildHashRingSemaphore.release();
                return;
            }

            LOG.info("Node added: {}", Arrays.toString(allAddedNodes.toArray(new String[0])));
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
                        DiscoveryNode curNode = nodeInfo.getNode();
                        if (plugins == null) {
                            continue;
                        }
                        TreeMap<Integer, DiscoveryNode> circle = null;
                        for (PluginInfo pluginInfo : plugins.getPluginInfos()) {
                            // bwc: Need to include both.
                            if (AD_PLUGIN_NAME.equals(pluginInfo.getName())
                                || AD_PLUGIN_NAME_FOR_TEST.equals(pluginInfo.getName())
                                || CommonName.TIME_SERIES_PLUGIN_NAME.equals(pluginInfo.getName())
                                || CommonName.TIME_SERIES_PLUGIN_NAME_FOR_TEST.equals(pluginInfo.getName())) {
                                Version version = VersionUtil.fromString(pluginInfo.getVersion());
                                boolean eligibleNode = nodeFilter.isEligibleNode(curNode);
                                if (eligibleNode) {
                                    circle = circles.computeIfAbsent(version, key -> new TreeMap<>());
                                    LOG.info("Add data node to version hash ring: {}", curNode.getId());
                                }
                                nodeVersions.put(curNode.getId(), new TimeSeriesNodeInfo(version, eligibleNode));
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
                LOG.info("All nodes with known version: {}", nodeVersions);

                // rebuild version hash ring with cooldown after all new node added.
                rebuildCirclesForRealtimeAD();

                if (!dataMigrator.isMigrated() && circles.size() > 0) {
                    // Find owning node with highest version to make sure the data migration logic be compatible to
                    // latest version when upgrade.
                    Optional<DiscoveryNode> owningNode = getOwningNodeWithHighestVersion(DEFAULT_HASH_RING_MODEL_ID);
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
                LOG.error("Fail to get node info to build hash ring", e);
            }));
        } catch (Exception e) {
            LOG.error("Failed to build circles", e);
            buildHashRingSemaphore.release();
            actionListener.onFailure(e);
        }
    }

    private void removeNodeFromCircles(String nodeId, Version version) {
        if (version != null) {
            TreeMap<Integer, DiscoveryNode> circle = this.circles.get(version);
            List<Integer> deleted = new ArrayList<>();
            for (Map.Entry<Integer, DiscoveryNode> entry : circle.entrySet()) {
                if (entry.getValue().getId().equals(nodeId)) {
                    deleted.add(entry.getKey());
                }
            }
            if (deleted.size() == circle.size()) {
                circles.remove(version);
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
            LOG.info("Rebuild hash ring for realtime with cooldown, nodeChangeEvents size {}", nodeChangeEvents.size());
            int size = nodeChangeEvents.size();
            TreeMap<Version, TreeMap<Integer, DiscoveryNode>> newCircles = new TreeMap<>();
            for (Map.Entry<Version, TreeMap<Integer, DiscoveryNode>> entry : circles.entrySet()) {
                newCircles.put(entry.getKey(), new TreeMap<>(entry.getValue()));
            }
            circlesForRealtimeAD = newCircles;
            lastUpdateForRealtimeAD = clock.millis();
            LOG.info("Build version hash ring successfully");
            String localNodeId = clusterService.localNode().getId();
            Set<String> modelIds = modelManager.getAllModelIds();
            for (String modelId : modelIds) {
                Optional<DiscoveryNode> node = getOwningNodeWithSameLocalVersionForRealtime(modelId);
                if (node.isPresent() && !node.get().getId().equals(localNodeId)) {
                    LOG.info(REMOVE_MODEL_MSG + " {}", modelId);
                    modelManager
                        .stopModel(
                            // stopModel will clear model cache
                            SingleStreamModelIdMapper.getConfigIdForModelId(modelId),
                            modelId,
                            ActionListener
                                .wrap(
                                    r -> LOG.info("Stopped model [{}] with response [{}]", modelId, r),
                                    e -> LOG.error("Fail to stop model " + modelId, e)
                                )
                        );
                }
            }
            // It's possible that multiple threads add new event to nodeChangeEvents,
            // but this is the only place to consume/poll the event and there is only
            // one thread poll it as we are using buildHashRingSemaphore
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
     * Check {@link org.opensearch.ad.settings.AnomalyDetectorSettings#AD_COOLDOWN_MINUTES} about
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
    public boolean eligibleToRebuildCirclesForRealtimeAD() {
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
     * Get owning node with highest version circle.
     * @param modelId model id
     * @return owning node
     */
    public Optional<DiscoveryNode> getOwningNodeWithHighestVersion(String modelId) {
        int modelHash = Murmur3HashFunction.hash(modelId);
        Map.Entry<Version, TreeMap<Integer, DiscoveryNode>> versionTreeMapEntry = circles.lastEntry();
        if (versionTreeMapEntry == null) {
            return Optional.empty();
        }
        TreeMap<Integer, DiscoveryNode> versionCircle = versionTreeMapEntry.getValue();
        Map.Entry<Integer, DiscoveryNode> entry = versionCircle.higherEntry(modelHash);
        return Optional.ofNullable(Optional.ofNullable(entry).orElse(versionCircle.firstEntry())).map(x -> x.getValue());
    }

    /**
     * Get owning node with same version of local node.
     * @param modelId model id
     * @param function consumer function
     * @param listener action listener
     * @param <T> listener response type
     */
    public <T> void buildAndGetOwningNodeWithSameLocalVersion(
        String modelId,
        Consumer<Optional<DiscoveryNode>> function,
        ActionListener<T> listener
    ) {
        buildCircles(ActionListener.wrap(r -> {
            DiscoveryNode localNode = clusterService.localNode();
            Version version = nodeVersions.containsKey(localNode.getId()) ? getVersion(localNode.getId()) : Version.CURRENT;
            Optional<DiscoveryNode> owningNode = getOwningNodeWithSameVersionDirectly(modelId, version, false);
            function.accept(owningNode);
        }, e -> listener.onFailure(e)));
    }

    public Optional<DiscoveryNode> getOwningNodeWithSameLocalVersionForRealtime(String modelId) {
        try {
            DiscoveryNode localNode = clusterService.localNode();
            Version version = nodeVersions.containsKey(localNode.getId()) ? getVersion(localNode.getId()) : Version.CURRENT;
            Optional<DiscoveryNode> owningNode = getOwningNodeWithSameVersionDirectly(modelId, version, true);
            // rebuild hash ring
            buildCirclesForRealtime();
            return owningNode;
        } catch (Exception e) {
            LOG.error("Failed to get owning node with same local time series version", e);
            return Optional.empty();
        }
    }

    private Optional<DiscoveryNode> getOwningNodeWithSameVersionDirectly(String modelId, Version version, boolean forRealtime) {
        int modelHash = Murmur3HashFunction.hash(modelId);
        TreeMap<Integer, DiscoveryNode> versionCircle = forRealtime ? circlesForRealtimeAD.get(version) : circles.get(version);
        if (versionCircle != null) {
            Map.Entry<Integer, DiscoveryNode> entry = versionCircle.higherEntry(modelHash);
            return Optional.ofNullable(Optional.ofNullable(entry).orElse(versionCircle.firstEntry())).map(x -> x.getValue());
        }
        return Optional.empty();
    }

    public <T> void getNodesWithSameLocalVersion(Consumer<DiscoveryNode[]> function, ActionListener<T> listener) {
        buildCircles(ActionListener.wrap(updated -> {
            DiscoveryNode localNode = clusterService.localNode();
            Version version = nodeVersions.containsKey(localNode.getId()) ? getVersion(localNode.getId()) : Version.CURRENT;
            Set<DiscoveryNode> nodes = getNodesWithSameVersion(version, false);
            if (!nodeVersions.containsKey(localNode.getId())) {
                nodes.add(localNode);
            }
            // Make sure listener return in function
            function.accept(nodes.toArray(new DiscoveryNode[0]));
        }, e -> listener.onFailure(e)));
    }

    public DiscoveryNode[] getNodesWithSameLocalVersion() {
        DiscoveryNode localNode = clusterService.localNode();
        Version version = nodeVersions.containsKey(localNode.getId()) ? getVersion(localNode.getId()) : Version.CURRENT;
        Set<DiscoveryNode> nodes = getNodesWithSameVersion(version, false);
        // rebuild hash ring
        buildCirclesForRealtime();
        return nodes.toArray(new DiscoveryNode[0]);
    }

    public Set<DiscoveryNode> getNodesWithSameVersion(Version version, boolean forRealtime) {
        TreeMap<Integer, DiscoveryNode> circle = forRealtime ? circlesForRealtimeAD.get(version) : circles.get(version);
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
     * Get time series version.
     * @param nodeId node id
     * @return version
     */
    public Version getVersion(String nodeId) {
        TimeSeriesNodeInfo nodeInfo = nodeVersions.get(nodeId);
        return nodeInfo == null ? null : nodeInfo.getVersion();
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
        DiscoveryNode[] allNodes = nodeFilter.getAllNodes();

        // Can't handle this edge case for BWC of AD1.0: mixed cluster with AD1.0 and Version after 1.1.
        // Start multiple OpenSearch processes on same IP, some run AD 1.0, some run new AD
        // on or after 1.1. As we ignore port number in transport address, just look for node
        // with IP like "127.0.0.1", so it's possible that we get wrong node as all nodes have
        // same IP.
        for (DiscoveryNode node : allNodes) {
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
        // Ignore port number as it may change, just use ip to look for node
        return address.toString().split(":")[0];
    }

    /**
     * Get all eligible data nodes whose time series versions are known in hash ring.
     * @param function consumer function
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getAllEligibleDataNodesWithKnownVersion(Consumer<DiscoveryNode[]> function, ActionListener<T> listener) {
        buildCircles(ActionListener.wrap(r -> {
            DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
            List<DiscoveryNode> allNodes = new ArrayList<>();
            for (DiscoveryNode node : eligibleDataNodes) {
                if (nodeVersions.containsKey(node.getId())) {
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
