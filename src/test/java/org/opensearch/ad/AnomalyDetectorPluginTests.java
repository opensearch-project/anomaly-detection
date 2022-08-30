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

package org.opensearch.ad;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Ignore;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;

import io.protostuff.LinkedBuffer;

public class AnomalyDetectorPluginTests extends ADUnitTestCase {
    AnomalyDetectorPlugin plugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        plugin = new AnomalyDetectorPlugin();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        plugin.close();
    }

    /**
     * We have legacy setting. AnomalyDetectorPlugin's createComponents can trigger
     * warning when using these legacy settings.
     */
    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    @Ignore
    public void testDeserializeRCFBufferPool() throws Exception {
        Settings.Builder settingsBuilder = Settings.builder();
        List<Setting<?>> allSettings = plugin.getSettings();
        for (Setting<?> setting : allSettings) {
            Object defaultVal = setting.getDefault(Settings.EMPTY);
            if (defaultVal instanceof Boolean) {
                settingsBuilder.put(setting.getKey(), (Boolean) defaultVal);
            } else {
                settingsBuilder.put(setting.getKey(), defaultVal.toString());
            }
        }
        Settings settings = settingsBuilder.build();

        Setting<?>[] settingArray = new Setting<?>[allSettings.size()];
        settingArray = allSettings.toArray(settingArray);

        ClusterSettings clusterSettings = clusterSetting(settings, settingArray);
        ClusterService clusterService = new ClusterService(settings, clusterSettings, null);

        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(settings);
        plugin.createComponents(mock(Client.class), clusterService, null, null, null, null, environment, null, null, null, null);
        GenericObjectPool<LinkedBuffer> deserializeRCFBufferPool = plugin.serializeRCFBufferPool;
        deserializeRCFBufferPool.addObject();
        LinkedBuffer buffer = deserializeRCFBufferPool.borrowObject();
        assertTrue(null != buffer);
    }

    // public void testOverriddenJobTypeAndIndex() {
    // assertEquals("opendistro_anomaly_detector", plugin.getJobType());
    // assertEquals(".opendistro-anomaly-detector-jobs", plugin.getJobIndex());
    // }

}
