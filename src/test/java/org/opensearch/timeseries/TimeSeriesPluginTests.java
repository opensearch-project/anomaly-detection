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

package org.opensearch.timeseries;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.Principal;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedRunnable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.identity.PluginSubject;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.util.PluginClient;
import org.opensearch.transport.client.Client;

import io.protostuff.LinkedBuffer;

public class TimeSeriesPluginTests extends ADUnitTestCase {
    TimeSeriesAnalyticsPlugin plugin;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        plugin = new TimeSeriesAnalyticsPlugin();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        plugin.close();
    }

    /**
     * We have legacy setting. TimeSeriesAnalyticsPlugin's createComponents can trigger
     * warning when using these legacy settings.
     */
    @Override
    protected boolean enableWarningsCheck() {
        return false;
    }

    public void testDeserializeRCFBufferPool() throws Exception {
        plugin.createComponents(mock(Client.class), clusterService(), null, null, null, null, environment(), null, null, null, null);
        GenericObjectPool<LinkedBuffer> deserializeRCFBufferPool = plugin.serializeRCFBufferPool;
        deserializeRCFBufferPool.addObject();
        LinkedBuffer buffer = deserializeRCFBufferPool.borrowObject();
        assertTrue(null != buffer);
    }

    public void testAssignSubjectSetsTheSubjectOfThePluginsClient() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        when(client.threadPool()).thenReturn(threadPool);
        AtomicReference<String> subjectSeenByAction = new AtomicReference<>();
        doAnswer(invocation -> {
            subjectSeenByAction.set(threadContext.getTransient("subject"));
            return null;
        }).when(client).execute(any(), any(), any());

        PluginClient pluginClient = plugin
            .createComponents(client, clusterService(), null, null, null, null, environment(), null, null, null, null)
            .stream()
            .filter(PluginClient.class::isInstance)
            .map(PluginClient.class::cast)
            .findFirst()
            .orElseThrow();
        plugin.assignSubject(new PluginSubject() {
            @Override
            public Principal getPrincipal() {
                return () -> "time-series-plugin";
            }

            @Override
            public <T extends Exception> void runAs(CheckedRunnable<T> r) throws T {
                try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                    threadContext.putTransient("subject", "time-series-plugin");
                    r.run();
                }
            }
        });

        pluginClient.execute(SearchAction.INSTANCE, new SearchRequest(), ActionListener.wrap(r -> {}, e -> {}));
        assertEquals("time-series-plugin", subjectSeenByAction.get());
    }

    private Environment environment() {
        Environment environment = mock(Environment.class);
        when(environment.settings()).thenReturn(defaultSettings());
        return environment;
    }

    private ClusterService clusterService() {
        List<Setting<?>> allSettings = plugin.getSettings();
        return new ClusterService(
            defaultSettings(),
            clusterSetting(defaultSettings(), allSettings.toArray(new Setting<?>[0])),
            mock(ThreadPool.class),
            null
        );
    }

    private Settings defaultSettings() {
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
        return settingsBuilder.build();
    }

    public void testOverriddenJobTypeAndIndex() {
        assertEquals("opensearch_time_series_analytics", plugin.getJobType());
        assertEquals(".opendistro-anomaly-detector-jobs", plugin.getJobIndex());
    }

}
