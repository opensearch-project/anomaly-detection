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

package org.opensearch.ad.cluster;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import org.junit.Before;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.cluster.ClusterManagerEventListener;
import org.opensearch.timeseries.cluster.HourlyCron;
import org.opensearch.timeseries.cluster.diskcleanup.BaseModelCheckpointIndexRetention;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;

public class ClusterManagerEventListenerTests extends AbstractTimeSeriesTest {
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;
    private Clock clock;
    private Cancellable hourlyCancellable;
    private Cancellable checkpointIndexRetentionCancellable;
    private ClusterManagerEventListener clusterManagerService;
    private ClientUtil clientUtil;
    private DiscoveryNodeFilterer nodeFilter;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = mock(ClusterService.class);
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_CHECKPOINT_TTL)))
        );
        when(clusterService.getClusterSettings()).thenReturn(settings);

        threadPool = mock(ThreadPool.class);
        hourlyCancellable = mock(Cancellable.class);
        checkpointIndexRetentionCancellable = mock(Cancellable.class);
        when(threadPool.scheduleWithFixedDelay(any(HourlyCron.class), any(TimeValue.class), any(String.class)))
            .thenReturn(hourlyCancellable);
        when(threadPool.scheduleWithFixedDelay(any(BaseModelCheckpointIndexRetention.class), any(TimeValue.class), any(String.class)))
            .thenReturn(checkpointIndexRetentionCancellable);
        client = mock(Client.class);
        clock = mock(Clock.class);
        clientUtil = mock(ClientUtil.class);
        HashMap<String, String> ignoredAttributes = new HashMap<String, String>();
        ignoredAttributes.put(CommonName.BOX_TYPE_KEY, CommonName.WARM_BOX_TYPE);
        nodeFilter = new DiscoveryNodeFilterer(clusterService);

        clusterManagerService = new ClusterManagerEventListener(
            clusterService,
            threadPool,
            client,
            clock,
            clientUtil,
            nodeFilter,
            AnomalyDetectorSettings.AD_CHECKPOINT_TTL,
            ForecastSettings.FORECAST_CHECKPOINT_TTL,
            Settings.EMPTY
        );
    }

    public void testOnOffClusterManager() {
        clusterManagerService.onClusterManager();
        assertThat(hourlyCancellable, is(notNullValue()));
        assertThat(checkpointIndexRetentionCancellable, is(notNullValue()));
        assertTrue(!clusterManagerService.getHourlyCron().isCancelled());
        List<Cancellable> checkpointIndexRetention = clusterManagerService.getCheckpointIndexRetentionCron();
        for (Cancellable cancellable : checkpointIndexRetention) {
            assertTrue(!cancellable.isCancelled());
        }
        clusterManagerService.offClusterManager();
        for (Cancellable cancellable : clusterManagerService.getCheckpointIndexRetentionCron()) {
            assertThat(cancellable, is(nullValue()));
        }
        assertThat(clusterManagerService.getHourlyCron(), is(nullValue()));
    }

    public void testBeforeStop() {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 1
            );

            LifecycleListener listener = null;
            if (args[0] instanceof LifecycleListener) {
                listener = (LifecycleListener) args[0];
            }

            assertTrue(listener != null);
            listener.beforeStop();

            return null;
        }).when(clusterService).addLifecycleListener(any());

        clusterManagerService.onClusterManager();
        for (Cancellable cancellable : clusterManagerService.getCheckpointIndexRetentionCron()) {
            assertThat(cancellable, is(nullValue()));
        }
        assertThat(clusterManagerService.getHourlyCron(), is(nullValue()));
        clusterManagerService.offClusterManager();
        for (Cancellable cancellable : clusterManagerService.getCheckpointIndexRetentionCron()) {
            assertThat(cancellable, is(nullValue()));
        }
        assertThat(clusterManagerService.getHourlyCron(), is(nullValue()));
    }
}
