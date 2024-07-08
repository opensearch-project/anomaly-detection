/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MAX_ENTITIES_PER_QUERY;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_PAGE_SIZE;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.model.FeatureData;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class ADResultProcessor extends
    ResultProcessor<AnomalyResultRequest, AnomalyResult, AnomalyResultResponse, ADTaskCacheManager, ADTaskType, ADTask, ADIndex, ADIndexManagement, ADTaskManager> {
    private static final Logger LOG = LogManager.getLogger(ADResultProcessor.class);

    public ADResultProcessor(
        Setting<TimeValue> requestTimeoutSetting,
        String entityResultAction,
        StatNames hcRequestCountStat,
        Settings settings,
        ClusterService clusterService,
        ThreadPool threadPool,
        HashRing hashRing,
        NodeStateManager nodeStateManager,
        TransportService transportService,
        ADStats timeSeriesStats,
        ADTaskManager realTimeTaskManager,
        NamedXContentRegistry xContentRegistry,
        Client client,
        SecurityClientUtil clientUtil,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Class<AnomalyResultResponse> transportResultResponseClazz,
        FeatureManager featureManager
    ) {
        super(
            requestTimeoutSetting,
            entityResultAction,
            hcRequestCountStat,
            settings,
            clusterService,
            threadPool,
            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
            hashRing,
            nodeStateManager,
            transportService,
            timeSeriesStats,
            realTimeTaskManager,
            xContentRegistry,
            client,
            clientUtil,
            indexNameExpressionResolver,
            transportResultResponseClazz,
            featureManager,
            AD_MAX_ENTITIES_PER_QUERY,
            AD_PAGE_SIZE,
            AnalysisType.AD,
            false,
            ADSingleStreamResultAction.NAME
        );
    }

    @Override
    protected AnomalyResultResponse createResultResponse(
        List<FeatureData> features,
        String error,
        Long rcfTotalUpdates,
        Long configInterval,
        Boolean isHC,
        String taskId
    ) {
        return new AnomalyResultResponse(features, error, rcfTotalUpdates, configInterval, isHC, taskId);
    }

    @Override
    protected void imputeHC(long dataStartTime, long dataEndTime, String configID, String taskId) {
        LOG
            .info(
                "Sending an HC impute request to process data from timestamp {} to {} for config {}",
                dataStartTime,
                dataEndTime,
                configID
            );

        DiscoveryNode[] dataNodes = hashRing.getNodesWithSameLocalVersion();

        client
            .execute(
                ADHCImputeAction.INSTANCE,
                new ADHCImputeRequest(configID, taskId, dataStartTime, dataEndTime, dataNodes),
                ActionListener.wrap(hcImputeResponse -> {
                    for (final ADHCImputeNodeResponse nodeResponse : hcImputeResponse.getNodes()) {
                        if (nodeResponse.getPreviousException() != null) {
                            nodeStateManager.setException(configID, nodeResponse.getPreviousException());
                        }
                    }
                }, e -> {
                    LOG.warn("fail to HC impute", e);
                    nodeStateManager.setException(configID, e);
                })
            );
    }
}
