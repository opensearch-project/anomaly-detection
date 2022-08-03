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

import java.time.Clock;
import java.time.Duration;

import org.opensearch.ad.cluster.diskcleanup.IndexCleanup;
import org.opensearch.ad.cluster.diskcleanup.ModelCheckpointIndexRetention;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.DateUtils;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.Client;
import org.opensearch.cluster.LocalNodeMasterListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.component.LifecycleListener;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

import com.google.common.annotations.VisibleForTesting;

public class ClusterManagerEventListener implements LocalNodeMasterListener {

    private Cancellable checkpointIndexRetentionCron;
    private Cancellable hourlyCron;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Client client;
    private Clock clock;
    private ClientUtil clientUtil;
    private DiscoveryNodeFilterer nodeFilter;
    private Duration checkpointTtlDuration;

    public ClusterManagerEventListener(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        Clock clock,
        ClientUtil clientUtil,
        DiscoveryNodeFilterer nodeFilter,
        Setting<TimeValue> checkpointTtl,
        Settings settings
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService.addLocalNodeMasterListener(this);
        this.clock = clock;
        this.clientUtil = clientUtil;
        this.nodeFilter = nodeFilter;

        this.checkpointTtlDuration = DateUtils.toDuration(checkpointTtl.get(settings));

        clusterService.getClusterSettings().addSettingsUpdateConsumer(checkpointTtl, it -> {
            this.checkpointTtlDuration = DateUtils.toDuration(it);
            cancel(checkpointIndexRetentionCron);
            IndexCleanup indexCleanup = new IndexCleanup(client, clientUtil, clusterService);
            checkpointIndexRetentionCron = threadPool
                .scheduleWithFixedDelay(
                    new ModelCheckpointIndexRetention(checkpointTtlDuration, clock, indexCleanup),
                    TimeValue.timeValueHours(24),
                    executorName()
                );
        });
    }

    @Override
    public void onMaster() {
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

        if (checkpointIndexRetentionCron == null) {
            IndexCleanup indexCleanup = new IndexCleanup(client, clientUtil, clusterService);
            checkpointIndexRetentionCron = threadPool
                .scheduleWithFixedDelay(
                    new ModelCheckpointIndexRetention(checkpointTtlDuration, clock, indexCleanup),
                    TimeValue.timeValueHours(24),
                    executorName()
                );
            clusterService.addLifecycleListener(new LifecycleListener() {
                @Override
                public void beforeStop() {
                    cancel(checkpointIndexRetentionCron);
                    checkpointIndexRetentionCron = null;
                }
            });
        }
    }

    @Override
    public void offMaster() {
        cancel(hourlyCron);
        cancel(checkpointIndexRetentionCron);
        hourlyCron = null;
        checkpointIndexRetentionCron = null;
    }

    private void cancel(Cancellable cron) {
        if (cron != null) {
            cron.cancel();
        }
    }

    @VisibleForTesting
    protected Cancellable getCheckpointIndexRetentionCron() {
        return checkpointIndexRetentionCron;
    }

    protected Cancellable getHourlyCron() {
        return hourlyCron;
    }

    private String executorName() {
        return ThreadPool.Names.GENERIC;
    }
}
