/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;

public class CrossClusterConfigUtils {
    private static final Logger logger = LogManager.getLogger(CrossClusterConfigUtils.class);

    private static final String WILDCARD = "*";

    /**
     * Uses the clusterName to determine whether the target client is the local or a remote client,
     * and returns the appropriate client.
     * @param clusterName The name of the cluster to evaluate.
     * @param client The local {@link NodeClient}.
     * @param localClusterName The name of the local cluster.
     * @return The local {@link NodeClient} for the local cluster, or a remote client for a remote cluster.
     */
    public static Client getClientForCluster(String clusterName, Client client, String localClusterName) {
        return clusterName.contains("#local") && clusterName.split("#")[0].equals(localClusterName)
            ? client
            : client.getRemoteClusterClient(clusterName);
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
            // Use the refactored method to get both cluster and index names in one call
            Pair<String, String> clusterAndIndex = parseClusterAndIndexName(index);
            String clusterName = clusterAndIndex.getKey();
            String indexName = clusterAndIndex.getValue();

            if (clusterName.isEmpty()) {
                // Use #local marker to indicate local cluster to avoid clashing if local cluster has the same name as remote cluster
                clusterName = localClusterName + "#local";
            }
            output.computeIfAbsent(clusterName, k -> new ArrayList<>()).add(indexName);
        }
        return output;
    }

    /**
     * Parses the cluster and index names from the given input string.
     * The input can be in either "cluster_name:index_name" format or just "index_name".
     * @param index The name of the index to evaluate.
     * @return A Pair where the left is the cluster name (or empty if not present), and the right is the index name.
     */
    public static Pair<String, String> parseClusterAndIndexName(String index) {
        if (index.contains(":")) {
            String[] parts = index.split(":", 2);
            String clusterName = parts[0];
            String indexName = parts.length > 1 ? parts[1] : "";
            return Pair.of(clusterName, indexName);
        } else {
            return Pair.of("", index);
        }
    }

    /**
     * Returns true if the cluster prefix contains a glob {@code *} wildcard.
     */
    public static boolean isWildcardClusterPattern(String clusterPattern) {
        return clusterPattern != null && clusterPattern.contains(WILDCARD);
    }

    /**
     * Returns true if any index in the list uses a wildcard cluster prefix (e.g. {@code *:foo} or
     * {@code cluster*:foo}). Local-only entries and entries with a fully qualified cluster name
     * return false.
     */
    public static boolean containsWildcardClusterPattern(List<String> indices) {
        if (indices == null) {
            return false;
        }
        for (String idx : indices) {
            if (idx == null || !idx.contains(":")) {
                continue;
            }
            String clusterPart = idx.split(":", 2)[0];
            if (isWildcardClusterPattern(clusterPart)) {
                return true;
            }
        }
        return false;
    }

    /**
     * If {@code indices} contains a wildcard cluster prefix, relax cross-cluster expansion on
     * {@code searchRequest} so a remote that lacks the resolved index is treated as "no docs"
     * rather than failing the whole request with {@code IndexNotFoundException}. Call only after
     * an upstream mapping check has verified that at least one expanded cluster has the index.
     */
    public static void applyLenientIfWildcard(SearchRequest searchRequest, List<String> indices) {
        if (containsWildcardClusterPattern(indices)) {
            searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen());
        }
    }

    /**
     * Compiles a glob (with {@code *} wildcards) to a regex {@link Pattern} that matches the entire
     * cluster name. Any other regex metacharacters in the glob are escaped literally.
     */
    public static Pattern globToRegex(String glob) {
        StringBuilder sb = new StringBuilder();
        sb.append('^');
        StringBuilder literal = new StringBuilder();
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            if (c == '*') {
                if (literal.length() > 0) {
                    sb.append(Pattern.quote(literal.toString()));
                    literal.setLength(0);
                }
                sb.append(".*");
            } else {
                literal.append(c);
            }
        }
        if (literal.length() > 0) {
            sb.append(Pattern.quote(literal.toString()));
        }
        sb.append('$');
        return Pattern.compile(sb.toString());
    }

    /**
     * Returns the subset of {@code candidates} that matches the given glob {@code pattern}. The
     * returned set preserves iteration order of {@code candidates}.
     */
    public static Set<String> matchRemoteClusters(String pattern, Set<String> candidates) {
        if (candidates == null || candidates.isEmpty()) {
            return Collections.emptySet();
        }
        Pattern regex = globToRegex(pattern);
        Set<String> matches = new LinkedHashSet<>();
        for (String name : candidates) {
            if (name != null && regex.matcher(name).matches()) {
                matches.add(name);
            }
        }
        return matches;
    }

    /**
     * Splits {@code indices} into strict per-cluster entries and wildcard cluster groups. Wildcard
     * cluster prefixes match only registered remote clusters — the local cluster must be referenced
     * with an explicit entry (no cluster prefix, or its real name).
     *
     * @throws ValidationException for empty inputs, empty index parts after a cluster prefix, or
     *                             a wildcard cluster prefix that matches no registered remote.
     */
    public static ResolvedIndices resolveIndices(
        List<String> indices,
        ClusterService clusterService,
        Set<String> remoteClusterNames,
        ValidationAspect aspect
    ) {
        return resolveIndices(indices, clusterService.getClusterName().value(), remoteClusterNames, aspect);
    }

    public static ResolvedIndices resolveIndices(
        List<String> indices,
        String localClusterName,
        Set<String> remoteClusterNames,
        ValidationAspect aspect
    ) {
        if (indices == null || indices.isEmpty()) {
            throw new ValidationException("No indices specified for validation.", ValidationIssueType.INDICES, aspect);
        }

        Map<String, List<String>> strict = new LinkedHashMap<>();
        List<WildcardPatternGroup> wildcardGroups = new ArrayList<>();

        for (String raw : indices) {
            if (raw == null || raw.isEmpty()) {
                throw new ValidationException(
                    String.format(Locale.ROOT, CommonMessages.EMPTY_INDEX_NAME_ERR_MSG, indices),
                    ValidationIssueType.INDICES,
                    aspect
                );
            }
            Pair<String, String> parts = parseClusterAndIndexName(raw);
            String clusterPart = parts.getKey();
            String indexPart = parts.getValue();

            if (indexPart.isEmpty()) {
                throw new ValidationException(
                    String.format(Locale.ROOT, CommonMessages.EMPTY_INDEX_NAME_ERR_MSG, raw),
                    ValidationIssueType.INDICES,
                    aspect
                );
            }

            if (isWildcardClusterPattern(clusterPart)) {
                if (remoteClusterNames == null || remoteClusterNames.isEmpty()) {
                    throw new ValidationException(
                        String.format(Locale.ROOT, CommonMessages.NO_REMOTE_CLUSTERS_CONFIGURED_ERR_MSG, raw),
                        ValidationIssueType.INDICES,
                        aspect
                    );
                }
                Set<String> matched = matchRemoteClusters(clusterPart, remoteClusterNames);
                if (matched.isEmpty()) {
                    throw new ValidationException(
                        String.format(Locale.ROOT, CommonMessages.NO_MATCHING_REMOTE_CLUSTER_ERR_MSG, clusterPart, raw),
                        ValidationIssueType.INDICES,
                        aspect
                    );
                }
                Map<String, List<String>> expanded = new LinkedHashMap<>();
                for (String remote : matched) {
                    expanded.computeIfAbsent(remote, k -> new ArrayList<>()).add(indexPart);
                }
                wildcardGroups.add(new WildcardPatternGroup(raw, clusterPart, indexPart, expanded));
            } else {
                String resolvedCluster = clusterPart.isEmpty() ? localClusterName + "#local" : clusterPart;
                strict.computeIfAbsent(resolvedCluster, k -> new ArrayList<>()).add(indexPart);
            }
        }

        return new ResolvedIndices(strict, wildcardGroups);
    }

    /**
     * Result of {@link #resolveIndices(List, ClusterService, Set, ValidationAspect)}: every strict
     * entry must validate; each wildcard group passes if at least one expansion validates.
     */
    public static final class ResolvedIndices {
        private final Map<String, List<String>> strict;
        private final List<WildcardPatternGroup> wildcardGroups;

        public ResolvedIndices(Map<String, List<String>> strict, List<WildcardPatternGroup> wildcardGroups) {
            this.strict = strict;
            this.wildcardGroups = wildcardGroups;
        }

        public Map<String, List<String>> getStrict() {
            return strict;
        }

        public List<WildcardPatternGroup> getWildcardGroups() {
            return wildcardGroups;
        }
    }

    /**
     * One wildcard cluster entry (e.g. {@code *:foo}) expanded against the matching remote
     * clusters.
     */
    public static final class WildcardPatternGroup {
        private final String originalEntry;
        private final String clusterPattern;
        private final String indexPart;
        private final Map<String, List<String>> expanded;

        public WildcardPatternGroup(String originalEntry, String clusterPattern, String indexPart, Map<String, List<String>> expanded) {
            this.originalEntry = originalEntry;
            this.clusterPattern = clusterPattern;
            this.indexPart = indexPart;
            this.expanded = expanded;
        }

        public String getOriginalEntry() {
            return originalEntry;
        }

        public String getClusterPattern() {
            return clusterPattern;
        }

        public String getIndexPart() {
            return indexPart;
        }

        public Map<String, List<String>> getExpanded() {
            return expanded;
        }
    }
}
