/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;

public class CrossClusterConfigUtils {
    private static final Logger logger = LogManager.getLogger(ParseUtils.class);

    /**
     * Uses the clusterName to determine whether the target client is the local or a remote client,
     * and returns the appropriate client.
     * @param clusterName The name of the cluster to evaluate.
     * @param client The local {@link NodeClient}.
     * @param localClusterName The name of the local cluster.
     * @return The local {@link NodeClient} for the local cluster, or a remote client for a remote cluster.
     */
    public static Client getClientForCluster(String clusterName, Client client, String localClusterName) {
        return clusterName.equals(localClusterName) ? client : client.getRemoteClusterClient(clusterName);
    }

    /**
     * Uses the clusterName to determine whether the target client is the local or a remote client,
     * and returns the appropriate client.
     * @param clusterName The name of the cluster to evaluate.
     * @param client The local {@link NodeClient}.
     * @param clusterService Used to retrieve the name of the local cluster.
     * @return The local {@link NodeClient} for the local cluster, or a remote client for a remote cluster.
     */
    public static Client getClientForCluster(String clusterName, Client client, ClusterService clusterService) {
        logger.info("clusterName1: " + clusterName);
        logger.info("clusterService.getClusterName().value(): " + clusterService.getClusterName().value());

        return getClientForCluster(clusterName, client, clusterService.getClusterName().value());
    }

    /**
     * Parses the list of indexes into a map of cluster_name to List of index names
     * @param indexes A list of index names in cluster_name:index_name format.
     *      Local indexes can also be in index_name format.
     * @param clusterService Used to retrieve the name of the local cluster.
     * @return A map of cluster_name:index names
     */
    public static HashMap<String, List<String>> separateClusterIndexes(List<String> indexes, ClusterService clusterService) {
        return separateClusterIndexes(indexes, clusterService.getClusterName().value());
    }

    /**
     * Parses the list of indexes into a map of cluster_name to list of index_name
     * @param indexes A list of index names in cluster_name:index_name format.
     * @param localClusterName The name of the local cluster.
     * @return A map of cluster_name to List index_name
     */
    public static HashMap<String, List<String>> separateClusterIndexes(List<String> indexes, String localClusterName) {
        HashMap<String, List<String>> output = new HashMap<>();
        for (String index : indexes) {
            String clusterName = parseClusterName(index);
            String indexName = parseIndexName(index);

            // If the index entry does not have a cluster_name, it indicates the index is on the local cluster.
            if (clusterName.isEmpty()) {
                clusterName = localClusterName;
            }
            output.computeIfAbsent(clusterName, k -> new ArrayList<>()).add(indexName);
        }
        return output;
    }

    /**
     * @param index The name of the index to evaluate.
     *      Can be in either cluster_name:index_name or index_name format.
     * @return The index name.
     */
    public static String parseIndexName(String index) {
        if (index.contains(":")) {
            String[] parts = index.split(":");
            return parts.length > 1 ? parts[1] : index;
        } else {
            return index;
        }
    }

    /**
     * @param index The name of the index to evaluate.
     *      Can be in either cluster_name:index_name or index_name format.
     * @return The index name.
     */
    public static String parseClusterName(String index) {
        return index.contains(":") ? index.substring(0, index.indexOf(':')) : "";
    }
}
