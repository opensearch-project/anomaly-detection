/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.transport;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.forecast.constant.ForecastCommonMessages;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.ForecastTask;
import org.opensearch.forecast.model.ForecastTaskType;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class ForecastResultTransportAction extends HandledTransportAction<ForecastResultRequest, ForecastResultResponse> {

    private static final Logger LOG = LogManager.getLogger(ForecastResultTransportAction.class);
    private ResultProcessor<ForecastResultRequest, ForecastResult, ForecastResultResponse, TaskCacheManager, ForecastTaskType, ForecastTask, ForecastIndex, ForecastIndexManagement, ForecastTaskManager> resultProcessor;
    private final Client client;
    private CircuitBreakerService circuitBreakerService;
    // Cache HC forecaster id. This is used to count HC failure stats. We can tell a forecaster
    // is HC or not by checking if forecaster id exists in this field or not. Will add
    // forecaster id to this field when start to run realtime detection and remove forecaster
    // id once realtime detection done.
    private final Set<String> hcForecasters;
    private final ForecastStats forecastStats;
    private final NodeStateManager nodeStateManager;
    private final Settings settings;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final HashRing hashRing;
    private final TransportService transportService;
    private final ForecastTaskManager realTimeTaskManager;
    private final NamedXContentRegistry xContentRegistry;
    private final SecurityClientUtil clientUtil;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final FeatureManager featureManager;

    @Inject
    public ForecastResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        Client client,
        SecurityClientUtil clientUtil,
        NodeStateManager nodeStateManager,
        FeatureManager featureManager,
        ForecastModelManager modelManager,
        HashRing hashRing,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        CircuitBreakerService circuitBreakerService,
        ForecastStats forecastStats,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        ForecastTaskManager realTimeTaskManager
    ) {
        super(ForecastResultAction.NAME, transportService, actionFilters, ForecastResultRequest::new);

        this.settings = settings;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.hashRing = hashRing;
        this.transportService = transportService;
        this.realTimeTaskManager = realTimeTaskManager;
        this.xContentRegistry = xContentRegistry;
        this.clientUtil = clientUtil;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.featureManager = featureManager;

        this.client = client;
        this.circuitBreakerService = circuitBreakerService;
        this.hcForecasters = new HashSet<>();
        this.forecastStats = forecastStats;
        this.nodeStateManager = nodeStateManager;

        this.resultProcessor = null;
    }

    @Override
    protected void doExecute(Task task, ForecastResultRequest request, ActionListener<ForecastResultResponse> listener) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            String forecastID = request.getConfigId();
            ActionListener<ForecastResultResponse> original = listener;
            listener = ActionListener.wrap(r -> {
                hcForecasters.remove(forecastID);
                original.onResponse(r);
            }, e -> {
                // If exception is TimeSeriesException and it should not be counted in stats,
                // we will not count it in failure stats.
                if (!(e instanceof TimeSeriesException) || ((TimeSeriesException) e).isCountedInStats()) {
                    forecastStats.getStat(StatNames.FORECAST_EXECUTE_FAIL_COUNT.getName()).increment();
                    if (hcForecasters.contains(forecastID)) {
                        forecastStats.getStat(StatNames.FORECAST_HC_EXECUTE_FAIL_COUNT.getName()).increment();
                    }
                }
                hcForecasters.remove(forecastID);
                original.onFailure(e);
            });

            if (!ForecastEnabledSetting.isForecastEnabled()) {
                throw new EndRunException(forecastID, ForecastCommonMessages.DISABLED_ERR_MSG, true).countedInStats(false);
            }

            forecastStats.getStat(StatNames.FORECAST_EXECUTE_REQUEST_COUNT.getName()).increment();

            if (circuitBreakerService.isOpen()) {
                listener.onFailure(new LimitExceededException(forecastID, CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
                return;
            }

            this.resultProcessor = new ForecastResultProcessor(
                ForecastSettings.FORECAST_REQUEST_TIMEOUT,
                TimeSeriesSettings.INTERVAL_RATIO_FOR_REQUESTS,
                EntityForecastResultAction.NAME,
                StatNames.FORECAST_HC_EXECUTE_REQUEST_COUNT,
                settings,
                clusterService,
                threadPool,
                hashRing,
                nodeStateManager,
                transportService,
                forecastStats,
                realTimeTaskManager,
                xContentRegistry,
                client,
                clientUtil,
                indexNameExpressionResolver,
                ForecastResultResponse.class,
                featureManager,
                AnalysisType.FORECAST,
                false
            );

            try {
                nodeStateManager
                    .getConfig(
                        forecastID,
                        AnalysisType.FORECAST,
                        resultProcessor.onGetConfig(listener, forecastID, request, Optional.of(hcForecasters))
                    );
            } catch (Exception ex) {
                ResultProcessor.handleExecuteException(ex, listener, forecastID);
            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }
}
