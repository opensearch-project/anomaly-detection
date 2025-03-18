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

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import org.opensearch.ad.cluster.diskcleanup.ADCheckpointIndexRetention;
import org.opensearch.cluster.LocalNodeClusterManagerListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.forecast.cluster.diskcleanup.ForecastCheckpointIndexRetention;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.cluster.diskcleanup.IndexCleanup;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DateUtils;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.transport.client.Client;

import com.google.common.annotations.VisibleForTesting;

public class ClusterManagerEventListener implements LocalNodeClusterManagerListener {

    private Cancellable adCheckpointIndexRetentionCron;
    private Cancellable forecastCheckpointIndexRetentionCron;
    private Cancellable hourlyCron;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;
    private Clock clock;
    private ClientUtil clientUtil;
    private DiscoveryNodeFilterer nodeFilter;
    private Duration adCheckpointTtlDuration;
    private Duration forecastCheckpointTtlDuration;

    public ClusterManagerEventListener(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        Clock clock,
        ClientUtil clientUtil,
        DiscoveryNodeFilterer nodeFilter,
        Setting<TimeValue> adCheckpointTtl,
        Setting<TimeValue> forecastCheckpointTtl,
        Settings settings
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService.addLocalNodeClusterManagerListener(this);
        this.clock = clock;
        this.clientUtil = clientUtil;
        this.nodeFilter = nodeFilter;

        this.adCheckpointTtlDuration = DateUtils.toDuration(adCheckpointTtl.get(settings));
        this.forecastCheckpointTtlDuration = DateUtils.toDuration(forecastCheckpointTtl.get(settings));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(adCheckpointTtl, it -> {
            this.adCheckpointTtlDuration = DateUtils.toDuration(it);
            cancel(adCheckpointIndexRetentionCron);
            IndexCleanup indexCleanup = new IndexCleanup(client, clientUtil, clusterService);
            adCheckpointIndexRetentionCron = threadPool
                .scheduleWithFixedDelay(
                    new ADCheckpointIndexRetention(adCheckpointTtlDuration, clock, indexCleanup),
                    TimeValue.timeValueHours(24),
                    executorName()
                );
            forecastCheckpointIndexRetentionCron = threadPool
                .scheduleWithFixedDelay(
                    new ForecastCheckpointIndexRetention(forecastCheckpointTtlDuration, clock, indexCleanup),
                    TimeValue.timeValueHours(24),
                    executorName()
                );
        });
    }

    @Override
    public void onClusterManager() {
        if (hourlyCron == null) {
            hourlyCron = threadPool.scheduleWithFixedDelay(new HourlyCron(client, nodeFilter), TimeValue.timeValueHours(1), executorName());
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    cancel(hourlyCron);
                    hourlyCron = null;
                }
            });
        }

        if (adCheckpointIndexRetentionCron == null) {
            IndexCleanup indexCleanup = new IndexCleanup(client, clientUtil, clusterService);
            adCheckpointIndexRetentionCron = threadPool
                .scheduleWithFixedDelay(
                    new ADCheckpointIndexRetention(adCheckpointTtlDuration, clock, indexCleanup),
                    TimeValue.timeValueHours(24),
                    executorName()
                );
            forecastCheckpointIndexRetentionCron = threadPool
                .scheduleWithFixedDelay(
                    new ForecastCheckpointIndexRetention(forecastCheckpointTtlDuration, clock, indexCleanup),
                    TimeValue.timeValueHours(24),
                    executorName()
                );
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    cancel(adCheckpointIndexRetentionCron);
                    adCheckpointIndexRetentionCron = null;
                    cancel(forecastCheckpointIndexRetentionCron);
                    forecastCheckpointIndexRetentionCron = null;
                }
            });
        }
    }

    @Override
    public void offClusterManager() {
        cancel(hourlyCron);
        hourlyCron = null;
        cancel(adCheckpointIndexRetentionCron);
        adCheckpointIndexRetentionCron = null;
        cancel(forecastCheckpointIndexRetentionCron);
        forecastCheckpointIndexRetentionCron = null;
    }

    private void cancel(Cancellable cron) {
        if (cron != null) {
            cron.cancel();
        }
    }

    @VisibleForTesting
    public List<Cancellable> getCheckpointIndexRetentionCron() {
        return Arrays.asList(adCheckpointIndexRetentionCron, forecastCheckpointIndexRetentionCron);
    }

    public Cancellable getHourlyCron() {
        return hourlyCron;
    }

    private String executorName() {
        return ThreadPool.Names.GENERIC;
    }
}
