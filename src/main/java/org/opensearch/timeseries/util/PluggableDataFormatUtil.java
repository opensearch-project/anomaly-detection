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

package org.opensearch.timeseries.util;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesArray;

/**
 * Helpers for clusters that store indices in a pluggable (non-Lucene) data format.
 *
 * Core stamps the cluster default data format onto every new index unless the index name starts with a prefix in
 * {@code cluster.pluggable.dataformat.restrict.allowlist} (see MetadataCreateIndexService#updatePluggableDataFormatSettings).
 * Such indices reject {@code text} fields and multi-fields under {@code nested} objects, so mappings we create there are
 * adapted: inside nested objects, {@code text} becomes {@code keyword} and {@code fields} sub-fields are dropped.
 * Clusters without pluggable data formats are unaffected.
 */
public final class PluggableDataFormatUtil {
    public static final String ENABLED_SETTING = "cluster.pluggable.dataformat.enabled";
    public static final String ALLOWLIST_SETTING = "cluster.pluggable.dataformat.restrict.allowlist";
    public static final String INDEX_ENABLED_SETTING = "index.pluggable.dataformat.enabled";
    // mirrors IndicesService#CLUSTER_PLUGGABLE_DATAFORMAT_ENABLED_SETTING in core
    static final Setting<Boolean> ENABLED = Setting
        .boolSetting(ENABLED_SETTING, false, Setting.Property.NodeScope, Setting.Property.Dynamic);

    private static final String PROPERTIES = "properties";
    private static final String TYPE = "type";
    private static final String FIELDS = "fields";
    private static final String NESTED = "nested";
    private static final String TEXT = "text";
    private static final String KEYWORD = "keyword";

    private PluggableDataFormatUtil() {}

    /**
     * @return true if new indices on this cluster default to a pluggable data format.
     */
    public static boolean isEnabled(ClusterService clusterService) {
        if (clusterService == null) {
            return false;
        }
        // the latest applied cluster-level (persistent/transient) value, when core registers the setting
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        if (clusterSettings != null && clusterSettings.get(ENABLED_SETTING) != null) {
            return clusterSettings.get(ENABLED);
        }
        // setting not registered on this node: fall back to node settings
        Settings nodeSettings = clusterService.getSettings();
        return nodeSettings != null && ENABLED.get(nodeSettings);
    }

    /**
     * @return index name prefixes that keep the default (Lucene) format on a pluggable-data-format cluster.
     *     Core declares {@code cluster.pluggable.dataformat.restrict.allowlist} with {@code NodeScope} only (not
     *     {@code Dynamic}), so the node settings are the authoritative source; it cannot change at the cluster level.
     */
    public static List<String> getAllowlist(ClusterService clusterService) {
        Settings nodeSettings = clusterService == null ? null : clusterService.getSettings();
        return nodeSettings == null ? Collections.emptyList() : nodeSettings.getAsList(ALLOWLIST_SETTING, Collections.emptyList());
    }

    /**
     * @param clusterService cluster service
     * @param indexName index name, alias, or date-math rollover pattern such as {@code <alias-history-{now/d}-1>}
     * @return true if the cluster has pluggable data formats enabled and the index uses one. Existing indices are checked
     *     via their {@code index.pluggable.dataformat.enabled} setting; otherwise the allowlist that core applies at index
     *     creation is used.
     */
    public static boolean usesPluggableDataFormat(ClusterService clusterService, String indexName) {
        if (Strings.isNullOrEmpty(indexName) || !isEnabled(clusterService)) {
            return false;
        }
        ClusterState state = clusterService.state();
        IndexMetadata indexMetadata = state == null || state.metadata() == null ? null : state.metadata().index(indexName);
        if (indexMetadata != null) {
            return indexMetadata.getSettings().getAsBoolean(INDEX_ENABLED_SETTING, false);
        }
        String name = indexName.startsWith("<") ? indexName.substring(1) : indexName;
        return getAllowlist(clusterService).stream().noneMatch(name::startsWith);
    }

    /**
     * @return {@code mapping} adapted for pluggable data formats if {@code indexName} uses one, otherwise {@code mapping}.
     */
    public static String mappingFor(ClusterService clusterService, String indexName, String mapping) throws IOException {
        return usesPluggableDataFormat(clusterService, indexName) ? adaptMapping(mapping) : mapping;
    }

    /**
     * Inside nested objects, converts {@code text} fields to {@code keyword} and removes {@code fields} sub-fields.
     *
     * @param mapping index mapping JSON with a top-level {@code properties} object
     * @return adapted mapping JSON
     */
    public static String adaptMapping(String mapping) throws IOException {
        Map<String, Object> root = XContentHelper.convertToMap(new BytesArray(mapping), true, XContentType.JSON).v2();
        adaptProperties(root.get(PROPERTIES), false);
        return XContentFactory.jsonBuilder().map(root).toString();
    }

    /**
     * Same as {@link #adaptMapping(String)} for an already parsed {@code properties} map (adapted in place).
     */
    public static Map<String, Object> adaptPropertiesMap(Map<String, Object> properties) {
        adaptProperties(properties, false);
        return properties;
    }

    @SuppressWarnings("unchecked")
    private static void adaptProperties(Object properties, boolean inNested) {
        if (!(properties instanceof Map)) {
            return;
        }
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) properties).entrySet()) {
            if (!(entry.getValue() instanceof Map)) {
                continue;
            }
            Map<String, Object> field = (Map<String, Object>) entry.getValue();
            Object type = field.get(TYPE);
            if (inNested && TEXT.equals(type)) {
                field.put(TYPE, KEYWORD);
            }
            if (inNested) {
                field.remove(FIELDS);
            }
            adaptProperties(field.get(PROPERTIES), inNested || NESTED.equals(type));
        }
    }
}
