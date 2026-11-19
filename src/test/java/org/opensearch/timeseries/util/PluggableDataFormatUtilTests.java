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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.opensearch.Version;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.test.OpenSearchTestCase;

public class PluggableDataFormatUtilTests extends OpenSearchTestCase {

    private static final Settings PLUGGABLE_NODE_SETTINGS = Settings
        .builder()
        .put(PluggableDataFormatUtil.ENABLED_SETTING, true)
        .putList(PluggableDataFormatUtil.ALLOWLIST_SETTING, ".opendistro-anomaly-", ".opensearch-forecast")
        .build();

    private ClusterService clusterService(Settings nodeSettings, Settings persistentSettings) {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(nodeSettings);
        ClusterState state = ClusterState
            .builder(new ClusterName("test"))
            .metadata(Metadata.builder().persistentSettings(persistentSettings).build())
            .build();
        when(clusterService.state()).thenReturn(state);
        return clusterService;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> field(Map<String, Object> mapping, String... path) {
        Map<String, Object> current = mapping;
        for (String name : path) {
            current = (Map<String, Object>) ((Map<String, Object>) current.get("properties")).get(name);
        }
        return current;
    }

    private static Map<String, Object> parse(String mapping) {
        return XContentHelper.convertToMap(new BytesArray(mapping), true, XContentType.JSON).v2();
    }

    public void testAdaptAnomalyResultMapping() throws IOException {
        String original = ADIndexManagement.getResultMappings();
        Map<String, Object> adapted = parse(PluggableDataFormatUtil.adaptMapping(original));

        for (String userField : new String[] { "name", "backend_roles", "roles", "custom_attribute_names" }) {
            Map<String, Object> f = field(adapted, "user", userField);
            assertEquals("keyword", f.get("type"));
            assertFalse("sub-fields must be removed under nested", f.containsKey("fields"));
        }
        // nested keyword/double fields and top-level text are unchanged
        assertEquals("keyword", field(adapted, "entity", "value").get("type"));
        assertEquals("double", field(adapted, "feature_data", "data").get("type"));
        assertEquals("text", field(adapted, "error").get("type"));
        // schema version is preserved so existing mapping-version checks keep working
        assertEquals(parse(original).get("_meta"), adapted.get("_meta"));
    }

    public void testAdaptForecastResultMapping() throws IOException {
        Map<String, Object> adapted = parse(PluggableDataFormatUtil.adaptMapping(ForecastIndexManagement.getResultMappings()));
        assertEquals("keyword", field(adapted, "user", "name").get("type"));
        assertFalse(field(adapted, "user", "name").containsKey("fields"));
    }

    public void testAdaptInsightsResultMapping() throws IOException {
        Map<String, Object> adapted = parse(PluggableDataFormatUtil.adaptMapping(ADIndexManagement.getInsightsResultMappings()));
        assertEquals("keyword", field(adapted, "clusters", "cluster_text").get("type"));
        assertEquals("keyword", field(adapted, "clusters", "anomalies", "model_id").get("type"));
    }

    public void testAdaptKeepsNonNestedMultiFields() throws IOException {
        String mapping = "{\"properties\":{\"name\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\"}}}}}";
        assertEquals(parse(mapping), parse(PluggableDataFormatUtil.adaptMapping(mapping)));
    }

    public void testDisabledByDefault() throws IOException {
        ClusterService clusterService = clusterService(Settings.EMPTY, Settings.EMPTY);
        assertFalse(PluggableDataFormatUtil.isEnabled(clusterService));
        assertFalse(PluggableDataFormatUtil.usesPluggableDataFormat(clusterService, "opensearch-ad-plugin-result-test"));
        String mapping = ADIndexManagement.getResultMappings();
        assertSame(mapping, PluggableDataFormatUtil.mappingFor(clusterService, "opensearch-ad-plugin-result-test", mapping));
        assertFalse(PluggableDataFormatUtil.isEnabled(null));
    }

    public void testUsesPluggableDataFormatRespectsAllowlist() {
        ClusterService clusterService = clusterService(PLUGGABLE_NODE_SETTINGS, Settings.EMPTY);
        assertTrue(PluggableDataFormatUtil.usesPluggableDataFormat(clusterService, "opensearch-ad-plugin-result-test"));
        assertTrue(PluggableDataFormatUtil.usesPluggableDataFormat(clusterService, "<opensearch-forecast-results-history-{now/d}-1>"));
        assertTrue(PluggableDataFormatUtil.usesPluggableDataFormat(clusterService, "<opensearch-ad-plugin-insights-history-{now/d}-1>"));
        assertFalse(PluggableDataFormatUtil.usesPluggableDataFormat(clusterService, "<.opendistro-anomaly-results-history-{now/d}-1>"));
        assertFalse(PluggableDataFormatUtil.usesPluggableDataFormat(clusterService, ".opensearch-forecasters"));
        assertFalse(PluggableDataFormatUtil.usesPluggableDataFormat(clusterService, null));
    }

    public void testExistingIndexUsesItsOwnSetting() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getSettings()).thenReturn(PLUGGABLE_NODE_SETTINGS);
        Metadata metadata = Metadata
            .builder()
            .put(indexMetadata("opensearch-ad-plugin-result-old-history-1", false), false)
            .put(indexMetadata(".opendistro-anomaly-results-history-1", true), false)
            .build();
        when(clusterService.state()).thenReturn(ClusterState.builder(new ClusterName("test")).metadata(metadata).build());
        // a Lucene index created before the cluster default changed keeps its Lucene mapping
        assertFalse(PluggableDataFormatUtil.usesPluggableDataFormat(clusterService, "opensearch-ad-plugin-result-old-history-1"));
        assertTrue(PluggableDataFormatUtil.usesPluggableDataFormat(clusterService, ".opendistro-anomaly-results-history-1"));
    }

    private static IndexMetadata indexMetadata(String name, boolean pluggable) {
        return IndexMetadata
            .builder(name)
            .settings(
                Settings
                    .builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(PluggableDataFormatUtil.INDEX_ENABLED_SETTING, pluggable)
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    public void testClusterLevelSettingOverridesNodeSetting() {
        ClusterService clusterService = clusterService(PLUGGABLE_NODE_SETTINGS, Settings.EMPTY);
        ClusterSettings clusterSettings = new ClusterSettings(PLUGGABLE_NODE_SETTINGS, Set.of(PluggableDataFormatUtil.ENABLED));
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        assertTrue(PluggableDataFormatUtil.isEnabled(clusterService));
        clusterSettings.applySettings(Settings.builder().put(PluggableDataFormatUtil.ENABLED_SETTING, false).build());
        assertFalse(PluggableDataFormatUtil.isEnabled(clusterService));
    }

    public void testUnregisteredClusterSettingFallsBackToNodeSettings() {
        ClusterService clusterService = clusterService(PLUGGABLE_NODE_SETTINGS, Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(new ClusterSettings(PLUGGABLE_NODE_SETTINGS, Set.of()));
        assertTrue(PluggableDataFormatUtil.isEnabled(clusterService));
    }

    public void testMappingForAdaptsOnlyPluggableIndices() throws IOException {
        ClusterService clusterService = clusterService(PLUGGABLE_NODE_SETTINGS, Settings.EMPTY);
        String mapping = ADIndexManagement.getResultMappings();
        assertSame(mapping, PluggableDataFormatUtil.mappingFor(clusterService, ".opendistro-anomaly-results", mapping));
        Map<String, Object> adapted = parse(PluggableDataFormatUtil.mappingFor(clusterService, "opensearch-ad-plugin-result-a", mapping));
        assertEquals("keyword", field(adapted, "user", "name").get("type"));
    }
}
