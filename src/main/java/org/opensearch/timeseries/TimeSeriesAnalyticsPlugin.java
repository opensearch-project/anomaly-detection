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

import static java.util.Collections.unmodifiableList;
import static org.opensearch.ad.constant.ADCommonName.ANOMALY_RESULT_INDEX_ALIAS;
import static org.opensearch.ad.constant.ADCommonName.CHECKPOINT_INDEX_NAME;
import static org.opensearch.ad.constant.ADCommonName.DETECTION_STATE_INDEX;
import static org.opensearch.ad.indices.ADIndexManagement.ALL_AD_RESULTS_INDEX_PATTERN;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_COOLDOWN_MINUTES;
import static org.opensearch.forecast.constant.ForecastCommonName.FORECAST_CHECKPOINT_INDEX_NAME;
import static org.opensearch.forecast.constant.ForecastCommonName.FORECAST_STATE_INDEX;
import static org.opensearch.timeseries.constant.CommonName.JOB_INDEX;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.SpecialPermission;
import org.opensearch.action.ActionRequest;
import org.opensearch.ad.ADJobProcessor;
import org.opensearch.ad.ADTaskProfileRunner;
import org.opensearch.ad.AnomalyDetectorRunner;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.InsightsJobProcessor;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.ml.ADModelManager;
import org.opensearch.ad.ml.ADRealTimeInferencer;
import org.opensearch.ad.ml.HybridThresholdingModel;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.ratelimit.ADCheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.ADCheckpointReadWorker;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.ratelimit.ADColdEntityWorker;
import org.opensearch.ad.ratelimit.ADColdStartWorker;
import org.opensearch.ad.ratelimit.ADResultWriteWorker;
import org.opensearch.ad.ratelimit.ADSaveResultStrategy;
import org.opensearch.ad.rest.RestAnomalyDetectorJobAction;
import org.opensearch.ad.rest.RestAnomalyDetectorSuggestAction;
import org.opensearch.ad.rest.RestDeleteAnomalyDetectorAction;
import org.opensearch.ad.rest.RestDeleteAnomalyResultsAction;
import org.opensearch.ad.rest.RestExecuteAnomalyDetectorAction;
import org.opensearch.ad.rest.RestGetAnomalyDetectorAction;
import org.opensearch.ad.rest.RestIndexAnomalyDetectorAction;
import org.opensearch.ad.rest.RestInsightsJobAction;
import org.opensearch.ad.rest.RestPreviewAnomalyDetectorAction;
import org.opensearch.ad.rest.RestSearchADTasksAction;
import org.opensearch.ad.rest.RestSearchAnomalyDetectorAction;
import org.opensearch.ad.rest.RestSearchAnomalyDetectorInfoAction;
import org.opensearch.ad.rest.RestSearchAnomalyResultAction;
import org.opensearch.ad.rest.RestSearchTopAnomalyResultAction;
import org.opensearch.ad.rest.RestStatsAnomalyDetectorAction;
import org.opensearch.ad.rest.RestValidateAnomalyDetectorAction;
import org.opensearch.ad.rest.handler.ADIndexJobActionHandler;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.ADNumericSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.LegacyOpenDistroAnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.suppliers.ADModelsOnNodeCountSupplier;
import org.opensearch.ad.stats.suppliers.ADModelsOnNodeSupplier;
import org.opensearch.ad.task.ADBatchTaskRunner;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADBatchAnomalyResultAction;
import org.opensearch.ad.transport.ADBatchAnomalyResultTransportAction;
import org.opensearch.ad.transport.ADBatchTaskRemoteExecutionAction;
import org.opensearch.ad.transport.ADBatchTaskRemoteExecutionTransportAction;
import org.opensearch.ad.transport.ADCancelTaskAction;
import org.opensearch.ad.transport.ADCancelTaskTransportAction;
import org.opensearch.ad.transport.ADEntityProfileAction;
import org.opensearch.ad.transport.ADEntityProfileTransportAction;
import org.opensearch.ad.transport.ADHCImputeAction;
import org.opensearch.ad.transport.ADHCImputeTransportAction;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.ADProfileTransportAction;
import org.opensearch.ad.transport.ADResultBulkAction;
import org.opensearch.ad.transport.ADResultBulkTransportAction;
import org.opensearch.ad.transport.ADSingleStreamResultAction;
import org.opensearch.ad.transport.ADSingleStreamResultTransportAction;
import org.opensearch.ad.transport.ADStatsNodesAction;
import org.opensearch.ad.transport.ADStatsNodesTransportAction;
import org.opensearch.ad.transport.ADTaskProfileAction;
import org.opensearch.ad.transport.ADTaskProfileTransportAction;
import org.opensearch.ad.transport.AnomalyDetectorJobAction;
import org.opensearch.ad.transport.AnomalyDetectorJobTransportAction;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultTransportAction;
import org.opensearch.ad.transport.DeleteADModelAction;
import org.opensearch.ad.transport.DeleteADModelTransportAction;
import org.opensearch.ad.transport.DeleteAnomalyDetectorAction;
import org.opensearch.ad.transport.DeleteAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.DeleteAnomalyResultsAction;
import org.opensearch.ad.transport.DeleteAnomalyResultsTransportAction;
import org.opensearch.ad.transport.EntityADResultAction;
import org.opensearch.ad.transport.EntityADResultTransportAction;
import org.opensearch.ad.transport.ForwardADTaskAction;
import org.opensearch.ad.transport.ForwardADTaskTransportAction;
import org.opensearch.ad.transport.GetAnomalyDetectorAction;
import org.opensearch.ad.transport.GetAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.IndexAnomalyDetectorAction;
import org.opensearch.ad.transport.IndexAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.InsightsJobAction;
import org.opensearch.ad.transport.InsightsJobTransportAction;
import org.opensearch.ad.transport.PreviewAnomalyDetectorAction;
import org.opensearch.ad.transport.PreviewAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.RCFPollingAction;
import org.opensearch.ad.transport.RCFPollingTransportAction;
import org.opensearch.ad.transport.RCFResultAction;
import org.opensearch.ad.transport.RCFResultTransportAction;
import org.opensearch.ad.transport.SearchADTasksAction;
import org.opensearch.ad.transport.SearchADTasksTransportAction;
import org.opensearch.ad.transport.SearchAnomalyDetectorAction;
import org.opensearch.ad.transport.SearchAnomalyDetectorInfoAction;
import org.opensearch.ad.transport.SearchAnomalyDetectorInfoTransportAction;
import org.opensearch.ad.transport.SearchAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.SearchAnomalyResultAction;
import org.opensearch.ad.transport.SearchAnomalyResultTransportAction;
import org.opensearch.ad.transport.SearchTopAnomalyResultAction;
import org.opensearch.ad.transport.SearchTopAnomalyResultTransportAction;
import org.opensearch.ad.transport.StatsAnomalyDetectorAction;
import org.opensearch.ad.transport.StatsAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.StopDetectorAction;
import org.opensearch.ad.transport.StopDetectorTransportAction;
import org.opensearch.ad.transport.SuggestAnomalyDetectorParamAction;
import org.opensearch.ad.transport.SuggestAnomalyDetectorParamTransportAction;
import org.opensearch.ad.transport.ThresholdResultAction;
import org.opensearch.ad.transport.ThresholdResultTransportAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.handler.ADIndexMemoryPressureAwareResultHandler;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.forecast.ExecuteForecastResultResponseRecorder;
import org.opensearch.forecast.ForecastJobProcessor;
import org.opensearch.forecast.ForecastTaskProfileRunner;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.ml.ForecastCheckpointDao;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.forecast.ml.ForecastModelManager;
import org.opensearch.forecast.ml.ForecastRealTimeInferencer;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.ratelimit.ForecastCheckpointMaintainWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointReadWorker;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.forecast.ratelimit.ForecastColdEntityWorker;
import org.opensearch.forecast.ratelimit.ForecastColdStartWorker;
import org.opensearch.forecast.ratelimit.ForecastResultWriteWorker;
import org.opensearch.forecast.ratelimit.ForecastSaveResultStrategy;
import org.opensearch.forecast.rest.RestDeleteForecasterAction;
import org.opensearch.forecast.rest.RestForecasterJobAction;
import org.opensearch.forecast.rest.RestForecasterSuggestAction;
import org.opensearch.forecast.rest.RestGetForecasterAction;
import org.opensearch.forecast.rest.RestIndexForecasterAction;
import org.opensearch.forecast.rest.RestRunOnceForecasterAction;
import org.opensearch.forecast.rest.RestSearchForecastTasksAction;
import org.opensearch.forecast.rest.RestSearchForecasterAction;
import org.opensearch.forecast.rest.RestSearchForecasterInfoAction;
import org.opensearch.forecast.rest.RestSearchTopForecastResultAction;
import org.opensearch.forecast.rest.RestStatsForecasterAction;
import org.opensearch.forecast.rest.RestValidateForecasterAction;
import org.opensearch.forecast.rest.handler.ForecastIndexJobActionHandler;
import org.opensearch.forecast.settings.ForecastEnabledSetting;
import org.opensearch.forecast.settings.ForecastNumericSetting;
import org.opensearch.forecast.settings.ForecastSettings;
import org.opensearch.forecast.stats.ForecastModelsOnNodeSupplier;
import org.opensearch.forecast.stats.ForecastStats;
import org.opensearch.forecast.stats.suppliers.ForecastModelsOnNodeCountSupplier;
import org.opensearch.forecast.task.ForecastTaskManager;
import org.opensearch.forecast.transport.DeleteForecastModelAction;
import org.opensearch.forecast.transport.DeleteForecastModelTransportAction;
import org.opensearch.forecast.transport.DeleteForecasterAction;
import org.opensearch.forecast.transport.DeleteForecasterTransportAction;
import org.opensearch.forecast.transport.EntityForecastResultAction;
import org.opensearch.forecast.transport.EntityForecastResultTransportAction;
import org.opensearch.forecast.transport.ForecastEntityProfileAction;
import org.opensearch.forecast.transport.ForecastEntityProfileTransportAction;
import org.opensearch.forecast.transport.ForecastProfileAction;
import org.opensearch.forecast.transport.ForecastProfileTransportAction;
import org.opensearch.forecast.transport.ForecastResultAction;
import org.opensearch.forecast.transport.ForecastResultBulkAction;
import org.opensearch.forecast.transport.ForecastResultBulkTransportAction;
import org.opensearch.forecast.transport.ForecastResultTransportAction;
import org.opensearch.forecast.transport.ForecastRunOnceAction;
import org.opensearch.forecast.transport.ForecastRunOnceProfileAction;
import org.opensearch.forecast.transport.ForecastRunOnceProfileTransportAction;
import org.opensearch.forecast.transport.ForecastRunOnceTransportAction;
import org.opensearch.forecast.transport.ForecastSingleStreamResultAction;
import org.opensearch.forecast.transport.ForecastSingleStreamResultTransportAction;
import org.opensearch.forecast.transport.ForecastStatsNodesAction;
import org.opensearch.forecast.transport.ForecastStatsNodesTransportAction;
import org.opensearch.forecast.transport.ForecasterJobAction;
import org.opensearch.forecast.transport.ForecasterJobTransportAction;
import org.opensearch.forecast.transport.GetForecasterAction;
import org.opensearch.forecast.transport.GetForecasterTransportAction;
import org.opensearch.forecast.transport.IndexForecasterAction;
import org.opensearch.forecast.transport.IndexForecasterTransportAction;
import org.opensearch.forecast.transport.SearchForecastTasksAction;
import org.opensearch.forecast.transport.SearchForecastTasksTransportAction;
import org.opensearch.forecast.transport.SearchForecasterAction;
import org.opensearch.forecast.transport.SearchForecasterInfoAction;
import org.opensearch.forecast.transport.SearchForecasterInfoTransportAction;
import org.opensearch.forecast.transport.SearchForecasterTransportAction;
import org.opensearch.forecast.transport.SearchTopForecastResultAction;
import org.opensearch.forecast.transport.SearchTopForecastResultTransportAction;
import org.opensearch.forecast.transport.StatsForecasterAction;
import org.opensearch.forecast.transport.StatsForecasterTransportAction;
import org.opensearch.forecast.transport.StopForecasterAction;
import org.opensearch.forecast.transport.StopForecasterTransportAction;
import org.opensearch.forecast.transport.SuggestForecasterParamAction;
import org.opensearch.forecast.transport.SuggestForecasterParamTransportAction;
import org.opensearch.forecast.transport.ValidateForecasterAction;
import org.opensearch.forecast.transport.ValidateForecasterTransportAction;
import org.opensearch.forecast.transport.handler.ForecastIndexMemoryPressureAwareResultHandler;
import org.opensearch.forecast.transport.handler.ForecastSearchHandler;
import org.opensearch.identity.PluginSubject;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.jobscheduler.spi.JobSchedulerExtension;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.IdentityAwarePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.secure_sm.AccessController;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.cluster.ADDataMigrator;
import org.opensearch.timeseries.cluster.ClusterEventListener;
import org.opensearch.timeseries.cluster.ClusterManagerEventListener;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.dataprocessor.Imputer;
import org.opensearch.timeseries.dataprocessor.LinearUniformImputer;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.function.ThrowingSupplierWrapper;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.ratelimit.CheckPointMaintainRequestAdapter;
import org.opensearch.timeseries.settings.TimeSeriesEnabledSetting;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.timeseries.stats.suppliers.IndexStatusSupplier;
import org.opensearch.timeseries.stats.suppliers.SettableSupplier;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.transport.CronAction;
import org.opensearch.timeseries.transport.CronTransportAction;
import org.opensearch.timeseries.transport.handler.ResultBulkIndexingHandler;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.IndexUtils;
import org.opensearch.timeseries.util.PluginClient;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import com.amazon.randomcutforest.parkservices.RCFCaster;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.amazon.randomcutforest.parkservices.state.RCFCasterMapper;
import com.amazon.randomcutforest.parkservices.state.RCFCasterState;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV3StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.protostuff.LinkedBuffer;
import io.protostuff.runtime.RuntimeSchema;

/**
 * Entry point of time series analytics plugin.
 */
public class TimeSeriesAnalyticsPlugin extends Plugin
    implements
        ActionPlugin,
        ScriptPlugin,
        SystemIndexPlugin,
        JobSchedulerExtension,
        IdentityAwarePlugin {

    private static final Logger LOG = LogManager.getLogger(TimeSeriesAnalyticsPlugin.class);

    // AD constants
    public static final String LEGACY_AD_BASE = "/_opendistro/_anomaly_detection";
    public static final String LEGACY_OPENDISTRO_AD_BASE_URI = LEGACY_AD_BASE + "/detectors";
    public static final String AD_BASE_URI = "/_plugins/_anomaly_detection";
    public static final String AD_BASE_DETECTORS_URI = AD_BASE_URI + "/detectors";
    public static final String AD_THREAD_POOL_PREFIX = "opensearch.ad.";
    public static final String AD_THREAD_POOL_NAME = "ad-threadpool";
    public static final String AD_BATCH_TASK_THREAD_POOL_NAME = "ad-batch-task-threadpool";

    // forecasting constants
    public static final String FORECAST_BASE_URI = "/_plugins/_forecast";
    public static final String FORECAST_FORECASTERS_URI = FORECAST_BASE_URI + "/forecasters";
    public static final String FORECAST_THREAD_POOL_PREFIX = "opensearch.forecast.";
    public static final String FORECAST_THREAD_POOL_NAME = "forecast-threadpool";

    public static final String TIME_SERIES_JOB_TYPE = "opensearch_time_series_analytics";

    private static Gson gson;
    private ADIndexManagement anomalyDetectionIndices;
    private ForecastIndexManagement forecastIndices;
    private AnomalyDetectorRunner anomalyDetectorRunner;
    private Client client;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private NamedXContentRegistry xContentRegistry;
    private ADStats adStats;
    private ForecastStats forecastStats;
    private ClientUtil clientUtil;
    private SecurityClientUtil securityClientUtil;
    private DiscoveryNodeFilterer nodeFilter;
    private IndexUtils indexUtils;
    private ADTaskManager adTaskManager;
    private ForecastTaskManager forecastTaskManager;
    private ADBatchTaskRunner adBatchTaskRunner;
    // package private for testing
    GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;
    private NodeStateManager stateManager;
    private ExecuteADResultResponseRecorder adResultResponseRecorder;
    private ExecuteForecastResultResponseRecorder forecastResultResponseRecorder;
    private ADIndexJobActionHandler adIndexJobActionHandler;
    private ForecastIndexJobActionHandler forecastIndexJobActionHandler;

    private PluginClient pluginClient;

    static {
        SpecialPermission.check();
        // gson intialization requires "java.lang.RuntimePermission" "accessDeclaredMembers" to
        // initialize ConstructorConstructor
        AccessController.doPrivileged(TimeSeriesAnalyticsPlugin::initGson);
    }

    public TimeSeriesAnalyticsPlugin() {}

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        // AD
        ADJobProcessor adJobRunner = ADJobProcessor.getInstance();
        adJobRunner.setClient(client);
        adJobRunner.setThreadPool(threadPool);
        adJobRunner.registerSettings(settings);
        adJobRunner.setIndexManagement(anomalyDetectionIndices);
        adJobRunner.setTaskManager(adTaskManager);
        adJobRunner.setNodeStateManager(stateManager);
        adJobRunner.setExecuteResultResponseRecorder(adResultResponseRecorder);
        adJobRunner.setIndexJobActionHandler(adIndexJobActionHandler);
        adJobRunner.setClock(getClock());

        // Insights
        InsightsJobProcessor insightsJobRunner = InsightsJobProcessor.getInstance();
        insightsJobRunner.setClient(client);
        insightsJobRunner.setThreadPool(threadPool);
        insightsJobRunner.registerSettings(settings);
        insightsJobRunner.setIndexManagement(anomalyDetectionIndices);
        insightsJobRunner.setTaskManager(adTaskManager);
        insightsJobRunner.setNodeStateManager(stateManager);
        insightsJobRunner.setExecuteResultResponseRecorder(adResultResponseRecorder);
        insightsJobRunner.setIndexJobActionHandler(adIndexJobActionHandler);
        insightsJobRunner.setClock(getClock());
        insightsJobRunner.setXContentRegistry(xContentRegistry);
        insightsJobRunner.setPluginClient(pluginClient);

        RestGetAnomalyDetectorAction restGetAnomalyDetectorAction = new RestGetAnomalyDetectorAction();
        RestIndexAnomalyDetectorAction restIndexAnomalyDetectorAction = new RestIndexAnomalyDetectorAction(settings, clusterService);
        RestSearchAnomalyDetectorAction searchAnomalyDetectorAction = new RestSearchAnomalyDetectorAction();
        RestSearchAnomalyResultAction searchAnomalyResultAction = new RestSearchAnomalyResultAction();
        RestSearchADTasksAction searchADTasksAction = new RestSearchADTasksAction();
        RestDeleteAnomalyDetectorAction deleteAnomalyDetectorAction = new RestDeleteAnomalyDetectorAction();
        RestExecuteAnomalyDetectorAction executeAnomalyDetectorAction = new RestExecuteAnomalyDetectorAction(settings, clusterService);
        RestStatsAnomalyDetectorAction statsAnomalyDetectorAction = new RestStatsAnomalyDetectorAction(adStats, this.nodeFilter);
        RestAnomalyDetectorJobAction anomalyDetectorJobAction = new RestAnomalyDetectorJobAction(settings, clusterService);
        RestSearchAnomalyDetectorInfoAction searchAnomalyDetectorInfoAction = new RestSearchAnomalyDetectorInfoAction();
        RestPreviewAnomalyDetectorAction previewAnomalyDetectorAction = new RestPreviewAnomalyDetectorAction();
        RestDeleteAnomalyResultsAction deleteAnomalyResultsAction = new RestDeleteAnomalyResultsAction();
        RestSearchTopAnomalyResultAction searchTopAnomalyResultAction = new RestSearchTopAnomalyResultAction();
        RestValidateAnomalyDetectorAction validateAnomalyDetectorAction = new RestValidateAnomalyDetectorAction(settings, clusterService);
        RestAnomalyDetectorSuggestAction suggestAnomalyDetectorAction = new RestAnomalyDetectorSuggestAction(settings, clusterService);
        RestInsightsJobAction insightsJobAction = new RestInsightsJobAction(settings, clusterService);

        // Forecast
        RestIndexForecasterAction restIndexForecasterAction = new RestIndexForecasterAction(settings, clusterService);
        RestForecasterJobAction restForecasterJobAction = new RestForecasterJobAction();
        RestGetForecasterAction restGetForecasterAction = new RestGetForecasterAction();
        RestDeleteForecasterAction deleteForecasterAction = new RestDeleteForecasterAction();
        RestSearchForecasterAction searchForecasterAction = new RestSearchForecasterAction();
        RestSearchForecasterInfoAction searchForecasterInfoAction = new RestSearchForecasterInfoAction();
        RestSearchTopForecastResultAction searchTopForecastResultAction = new RestSearchTopForecastResultAction();
        RestSearchForecastTasksAction searchForecastTasksAction = new RestSearchForecastTasksAction();
        RestStatsForecasterAction statsForecasterAction = new RestStatsForecasterAction(forecastStats, this.nodeFilter);
        RestRunOnceForecasterAction runOnceForecasterAction = new RestRunOnceForecasterAction();
        RestValidateForecasterAction validateForecasterAction = new RestValidateForecasterAction(settings, clusterService);
        RestForecasterSuggestAction suggestForecasterParamAction = new RestForecasterSuggestAction(settings, clusterService);

        ForecastJobProcessor forecastJobRunner = ForecastJobProcessor.getInstance();
        forecastJobRunner.setClient(client);
        forecastJobRunner.setThreadPool(threadPool);
        forecastJobRunner.registerSettings(settings);
        forecastJobRunner.setIndexManagement(forecastIndices);
        forecastJobRunner.setTaskManager(forecastTaskManager);
        forecastJobRunner.setNodeStateManager(stateManager);
        forecastJobRunner.setExecuteResultResponseRecorder(forecastResultResponseRecorder);
        forecastJobRunner.setIndexJobActionHandler(forecastIndexJobActionHandler);
        forecastJobRunner.setClock(getClock());

        return ImmutableList
            .of(
                // AD
                restGetAnomalyDetectorAction,
                restIndexAnomalyDetectorAction,
                searchAnomalyDetectorAction,
                searchAnomalyResultAction,
                searchADTasksAction,
                deleteAnomalyDetectorAction,
                executeAnomalyDetectorAction,
                anomalyDetectorJobAction,
                statsAnomalyDetectorAction,
                searchAnomalyDetectorInfoAction,
                previewAnomalyDetectorAction,
                deleteAnomalyResultsAction,
                searchTopAnomalyResultAction,
                validateAnomalyDetectorAction,
                suggestAnomalyDetectorAction,
                insightsJobAction,
                // Forecast
                restIndexForecasterAction,
                restForecasterJobAction,
                restGetForecasterAction,
                deleteForecasterAction,
                searchForecasterAction,
                searchForecasterInfoAction,
                searchTopForecastResultAction,
                searchForecastTasksAction,
                statsForecasterAction,
                runOnceForecasterAction,
                validateForecasterAction,
                suggestForecasterParamAction
            );
    }

    private static Void initGson() {
        gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
        return null;
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        // =====================
        // Common components
        // =====================
        this.client = client;
        this.pluginClient = new PluginClient(client);
        this.threadPool = threadPool;
        this.xContentRegistry = xContentRegistry;
        Settings settings = environment.settings();
        this.clientUtil = new ClientUtil(client);
        this.indexUtils = new IndexUtils(clusterService, indexNameExpressionResolver);
        this.nodeFilter = new DiscoveryNodeFilterer(clusterService);
        this.clusterService = clusterService;
        Imputer imputer = new LinearUniformImputer(true);

        JvmService jvmService = new JvmService(environment.settings());
        RandomCutForestMapper rcfMapper = new RandomCutForestMapper();
        rcfMapper.setSaveExecutorContextEnabled(true);
        rcfMapper.setSaveTreeStateEnabled(true);
        rcfMapper.setPartialTreeStateEnabled(true);
        V1JsonToV3StateConverter converter = new V1JsonToV3StateConverter();

        CircuitBreakerService circuitBreakerService = new CircuitBreakerService(jvmService).init();

        long heapSizeBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();

        serializeRCFBufferPool = AccessController.doPrivileged(() -> {
            return new GenericObjectPool<>(new BasePooledObjectFactory<LinkedBuffer>() {
                @Override
                public LinkedBuffer create() throws Exception {
                    return LinkedBuffer.allocate(TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES);
                }

                @Override
                public PooledObject<LinkedBuffer> wrap(LinkedBuffer obj) {
                    return new DefaultPooledObject<>(obj);
                }
            });
        });
        serializeRCFBufferPool.setMaxTotal(TimeSeriesSettings.MAX_TOTAL_RCF_SERIALIZATION_BUFFERS);
        serializeRCFBufferPool.setMaxIdle(TimeSeriesSettings.MAX_TOTAL_RCF_SERIALIZATION_BUFFERS);
        serializeRCFBufferPool.setMinIdle(0);
        serializeRCFBufferPool.setBlockWhenExhausted(false);
        serializeRCFBufferPool.setTimeBetweenEvictionRuns(TimeSeriesSettings.HOURLY_MAINTENANCE);

        stateManager = new NodeStateManager(
            client,
            xContentRegistry,
            settings,
            clientUtil,
            getClock(),
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            clusterService,
            TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
            TimeSeriesSettings.BACKOFF_MINUTES
        );
        securityClientUtil = new SecurityClientUtil(stateManager, settings);

        SearchFeatureDao searchFeatureDao = new SearchFeatureDao(
            client,
            xContentRegistry,
            securityClientUtil,
            settings,
            clusterService,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE
        );

        FeatureManager featureManager = new FeatureManager(
            searchFeatureDao,
            imputer,
            TimeSeriesSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS,
            TimeSeriesSettings.MIN_TRAIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SHINGLE_PROPORTION_MISSING,
            AnomalyDetectorSettings.MAX_IMPUTATION_NEIGHBOR_DISTANCE,
            AnomalyDetectorSettings.PREVIEW_SAMPLE_RATE,
            AnomalyDetectorSettings.MAX_PREVIEW_SAMPLES,
            threadPool
        );

        Random random = new Random(42);

        // =====================
        // AD components
        // =====================
        ADEnabledSetting.getInstance().init(clusterService);
        ADNumericSetting.getInstance().init(clusterService);
        // convert from checked IOException to unchecked RuntimeException
        this.anomalyDetectionIndices = ThrowingSupplierWrapper
            .throwingSupplierWrapper(
                () -> new ADIndexManagement(
                    client,
                    clusterService,
                    threadPool,
                    settings,
                    nodeFilter,
                    TimeSeriesSettings.MAX_UPDATE_RETRY_TIMES,
                    xContentRegistry
                )
            )
            .get();

        double adModelMaxSizePercent = AnomalyDetectorSettings.AD_MODEL_MAX_SIZE_PERCENTAGE.get(settings);

        MemoryTracker adMemoryTracker = new MemoryTracker(jvmService, adModelMaxSizePercent, clusterService, circuitBreakerService);

        ADCheckpointDao adCheckpoint = new ADCheckpointDao(
            client,
            clientUtil,
            gson,
            rcfMapper,
            converter,
            new ThresholdedRandomCutForestMapper(),
            AccessController.doPrivileged(() -> RuntimeSchema.getSchema(ThresholdedRandomCutForestState.class)),
            HybridThresholdingModel.class,
            anomalyDetectionIndices,
            TimeSeriesSettings.MAX_CHECKPOINT_BYTES,
            serializeRCFBufferPool,
            TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES,
            1 - TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            getClock()
        );

        ADCacheProvider adCacheProvider = new ADCacheProvider();

        CheckPointMaintainRequestAdapter<ThresholdedRandomCutForest, ADIndex, ADIndexManagement, ADCheckpointDao, ADPriorityCache> adAdapter =
            new CheckPointMaintainRequestAdapter<>(
                adCheckpoint,
                ADCommonName.CHECKPOINT_INDEX_NAME,
                AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
                getClock(),
                clusterService,
                settings,
                adCacheProvider
            );

        ADCheckpointWriteWorker adCheckpointWriteQueue = new ADCheckpointWriteWorker(
            heapSizeBytes,
            TimeSeriesSettings.CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            adCheckpoint,
            ADCommonName.CHECKPOINT_INDEX_NAME,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE
        );

        ADCheckpointMaintainWorker adCheckpointMaintainQueue = new ADCheckpointMaintainWorker(
            heapSizeBytes,
            TimeSeriesSettings.CHECKPOINT_MAINTAIN_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            adCheckpointWriteQueue,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            adAdapter::convert
        );

        ADPriorityCache adPriorityCache = new ADPriorityCache(
            adCheckpoint,
            AnomalyDetectorSettings.AD_DEDICATED_CACHE_SIZE.get(settings),
            AnomalyDetectorSettings.AD_CHECKPOINT_TTL,
            AnomalyDetectorSettings.MAX_INACTIVE_ENTITIES,
            adMemoryTracker,
            TimeSeriesSettings.NUM_TREES,
            getClock(),
            clusterService,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            threadPool,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            settings,
            AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
            adCheckpointWriteQueue,
            adCheckpointMaintainQueue
        );

        // cache provider allows us to break circular dependency among PriorityCache, CacheBuffer,
        // CheckPointMaintainRequestAdapter, and CheckpointMaintainWorker
        adCacheProvider.set(adPriorityCache);

        ADColdStart adEntityColdStarter = new ADColdStart(
            getClock(),
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            (int) (AD_COOLDOWN_MINUTES.get(settings).getMinutes()),
            anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT)
        );

        ADModelManager adModelManager = new ADModelManager(
            adCheckpoint,
            getClock(),
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.MIN_PREVIEW_SIZE,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
            adEntityColdStarter,
            featureManager,
            adMemoryTracker,
            settings,
            clusterService
        );

        ADIndexMemoryPressureAwareResultHandler adIndexMemoryPressureAwareResultHandler = new ADIndexMemoryPressureAwareResultHandler(
            client,
            anomalyDetectionIndices,
            clusterService
        );

        ADResultWriteWorker adResultWriteQueue = new ADResultWriteWorker(
            heapSizeBytes,
            TimeSeriesSettings.RESULT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            adIndexMemoryPressureAwareResultHandler,
            xContentRegistry,
            stateManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE
        );

        ADSaveResultStrategy adSaveResultStrategy = new ADSaveResultStrategy(
            anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT),
            adResultWriteQueue
        );

        ADDataMigrator adDataMigrator = new ADDataMigrator(client, clusterService, xContentRegistry, anomalyDetectionIndices);
        HashRing hashRing = new HashRing(nodeFilter, getClock(), settings, client, clusterService, adDataMigrator, adModelManager);
        ADTaskProfileRunner adTaskProfileRunner = new ADTaskProfileRunner(hashRing, client);
        ADTaskCacheManager adTaskCacheManager = new ADTaskCacheManager(settings, clusterService, adMemoryTracker);

        adTaskManager = new ADTaskManager(
            settings,
            clusterService,
            client,
            xContentRegistry,
            anomalyDetectionIndices,
            nodeFilter,
            hashRing,
            adTaskCacheManager,
            threadPool,
            stateManager,
            adTaskProfileRunner
        );

        ADColdStartWorker adColdstartQueue = new ADColdStartWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            adEntityColdStarter,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            adPriorityCache,
            adModelManager,
            adSaveResultStrategy,
            adTaskManager,
            adCheckpointWriteQueue
        );

        Map<String, TimeSeriesStat<?>> adStatsMap = ImmutableMap
            .<String, TimeSeriesStat<?>>builder()
            // ad stats
            .put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_HC_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(
                StatNames.ANOMALY_RESULTS_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, ADCommonName.ANOMALY_RESULT_INDEX_ALIAS))
            )
            .put(
                StatNames.AD_MODELS_CHECKPOINT_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, ADCommonName.CHECKPOINT_INDEX_NAME))
            )
            .put(
                StatNames.ANOMALY_DETECTION_STATE_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, ADCommonName.DETECTION_STATE_INDEX))
            )
            .put(StatNames.DETECTOR_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.SINGLE_STREAM_DETECTOR_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.HC_DETECTOR_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_CANCELED_BATCH_TASK_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_TOTAL_BATCH_TASK_EXECUTION_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_BATCH_TASK_FAILURE_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_MODEL_CORRUTPION_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(
                StatNames.MODEL_INFORMATION.getName(),
                new TimeSeriesStat<>(false, new ADModelsOnNodeSupplier(adModelManager, adCacheProvider, settings, clusterService))
            )
            .put(
                StatNames.AD_CONFIG_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, ADCommonName.CONFIG_INDEX))
            )
            .put(
                StatNames.FORECAST_CONFIG_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, ForecastCommonName.CONFIG_INDEX))
            )
            .put(
                StatNames.JOB_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, CommonName.JOB_INDEX))
            )
            .put(
                StatNames.MODEL_COUNT.getName(),
                new TimeSeriesStat<>(false, new ADModelsOnNodeCountSupplier(adModelManager, adCacheProvider))
            )
            .build();

        adStats = new ADStats(adStatsMap);

        ADRealTimeInferencer adInferencer = new ADRealTimeInferencer(
            adModelManager,
            adStats,
            adCheckpoint,
            adColdstartQueue,
            adSaveResultStrategy,
            adCacheProvider,
            threadPool,
            getClock(),
            searchFeatureDao
        );

        ADCheckpointReadWorker adCheckpointReadQueue = new ADCheckpointReadWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            adModelManager,
            adCheckpoint,
            adColdstartQueue,
            stateManager,
            adCacheProvider,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            adCheckpointWriteQueue,
            adInferencer
        );

        ADColdEntityWorker adColdEntityQueue = new ADColdEntityWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.AD_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            adCheckpointReadQueue,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager
        );

        anomalyDetectorRunner = new AnomalyDetectorRunner(adModelManager, featureManager, AnomalyDetectorSettings.MAX_PREVIEW_RESULTS);

        ResultBulkIndexingHandler<AnomalyResult, ADIndex, ADIndexManagement> anomalyResultBulkIndexHandler =
            new ResultBulkIndexingHandler<>(
                client,
                settings,
                threadPool,
                ANOMALY_RESULT_INDEX_ALIAS,
                anomalyDetectionIndices,
                this.clientUtil,
                this.indexUtils,
                clusterService,
                AnomalyDetectorSettings.AD_BACKOFF_INITIAL_DELAY,
                AnomalyDetectorSettings.AD_MAX_RETRY_FOR_BACKOFF
            );

        ADSearchHandler adSearchHandler = new ADSearchHandler(settings, clusterService, client, pluginClient);

        ResultBulkIndexingHandler<AnomalyResult, ADIndex, ADIndexManagement> anomalyResultHandler = new ResultBulkIndexingHandler<>(
            client,
            settings,
            threadPool,
            ANOMALY_RESULT_INDEX_ALIAS,
            anomalyDetectionIndices,
            this.clientUtil,
            this.indexUtils,
            clusterService,
            AnomalyDetectorSettings.AD_BACKOFF_INITIAL_DELAY,
            AnomalyDetectorSettings.AD_MAX_RETRY_FOR_BACKOFF
        );

        // =====================
        // common components, need AD/forecasting components to initialize
        // =====================

        adBatchTaskRunner = new ADBatchTaskRunner(
            settings,
            threadPool,
            clusterService,
            client,
            securityClientUtil,
            circuitBreakerService,
            featureManager,
            adTaskManager,
            anomalyDetectionIndices,
            adStats,
            anomalyResultBulkIndexHandler,
            adTaskCacheManager,
            searchFeatureDao,
            hashRing,
            adModelManager
        );

        adResultResponseRecorder = new ExecuteADResultResponseRecorder(
            anomalyDetectionIndices,
            anomalyResultHandler,
            adTaskManager,
            nodeFilter,
            threadPool,
            client,
            stateManager,
            getClock(),
            TimeSeriesSettings.NUM_MIN_SAMPLES
        );

        adIndexJobActionHandler = new ADIndexJobActionHandler(
            client,
            anomalyDetectionIndices,
            xContentRegistry,
            adTaskManager,
            adResultResponseRecorder,
            stateManager,
            settings
        );

        // =====================
        // forecast components
        // =====================
        ForecastEnabledSetting.getInstance().init(clusterService);
        ForecastNumericSetting.getInstance().init(clusterService);

        forecastIndices = ThrowingSupplierWrapper
            .throwingSupplierWrapper(
                () -> new ForecastIndexManagement(
                    client,
                    clusterService,
                    threadPool,
                    settings,
                    nodeFilter,
                    ForecastSettings.FORECAST_MAX_UPDATE_RETRY_TIMES,
                    xContentRegistry
                )
            )
            .get();

        double forecastModelMaxSizePercent = ForecastSettings.FORECAST_MODEL_MAX_SIZE_PERCENTAGE.get(settings);

        MemoryTracker forecastMemoryTracker = new MemoryTracker(
            jvmService,
            forecastModelMaxSizePercent,
            clusterService,
            circuitBreakerService
        );

        ForecastCheckpointDao forecastCheckpoint = new ForecastCheckpointDao(
            client,
            clientUtil,
            gson,
            TimeSeriesSettings.MAX_CHECKPOINT_BYTES,
            serializeRCFBufferPool,
            TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES,
            forecastIndices,
            new RCFCasterMapper(),
            AccessController.doPrivileged(() -> RuntimeSchema.getSchema(RCFCasterState.class)),
            getClock()
        );

        ForecastCacheProvider forecastCacheProvider = new ForecastCacheProvider();

        CheckPointMaintainRequestAdapter<RCFCaster, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastPriorityCache> forecastAdapter =
            new CheckPointMaintainRequestAdapter<RCFCaster, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastPriorityCache>(
                forecastCheckpoint,
                ForecastIndex.CHECKPOINT.getIndexName(),
                ForecastSettings.FORECAST_CHECKPOINT_SAVING_FREQ,
                getClock(),
                clusterService,
                settings,
                forecastCacheProvider
            );

        ForecastCheckpointWriteWorker forecastCheckpointWriteQueue = new ForecastCheckpointWriteWorker(
            heapSizeBytes,
            TimeSeriesSettings.CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            forecastCheckpoint,
            ForecastIndex.CHECKPOINT.getIndexName(),
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE
        );

        ForecastCheckpointMaintainWorker forecastCheckpointMaintainQueue = new ForecastCheckpointMaintainWorker(
            heapSizeBytes,
            TimeSeriesSettings.CHECKPOINT_MAINTAIN_REQUEST_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            forecastCheckpointWriteQueue,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            forecastAdapter::convert
        );

        ForecastPriorityCache forecastPriorityCache = new ForecastPriorityCache(
            forecastCheckpoint,
            ForecastSettings.FORECAST_DEDICATED_CACHE_SIZE.get(settings),
            AnomalyDetectorSettings.AD_CHECKPOINT_TTL,
            AnomalyDetectorSettings.MAX_INACTIVE_ENTITIES,
            adMemoryTracker,
            TimeSeriesSettings.NUM_TREES,
            getClock(),
            clusterService,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            threadPool,
            FORECAST_THREAD_POOL_NAME,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            settings,
            ForecastSettings.FORECAST_CHECKPOINT_SAVING_FREQ,
            forecastCheckpointWriteQueue,
            forecastCheckpointMaintainQueue
        );

        // cache provider allows us to break circular dependency among PriorityCache, CacheBuffer,
        // CheckPointMaintainRequestAdapter, and CheckpointMaintainWorker
        forecastCacheProvider.set(forecastPriorityCache);

        ForecastColdStart forecastColdStarter = new ForecastColdStart(
            getClock(),
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            (int) (AD_COOLDOWN_MINUTES.get(settings).getMinutes()),
            -1, // no hard coded random seed
            -1, // interpolation is disabled so we don't need to specify the number of sampled points
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            forecastIndices.getSchemaVersion(ForecastIndex.RESULT)
        );

        ForecastModelManager forecastModelManager = new ForecastModelManager(
            forecastCheckpoint,
            getClock(),
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            forecastColdStarter,
            forecastMemoryTracker,
            featureManager
        );

        ForecastIndexMemoryPressureAwareResultHandler forecastIndexMemoryPressureAwareResultHandler =
            new ForecastIndexMemoryPressureAwareResultHandler(client, forecastIndices, clusterService);

        ForecastResultWriteWorker forecastResultWriteQueue = new ForecastResultWriteWorker(
            heapSizeBytes,
            TimeSeriesSettings.RESULT_WRITE_QUEUE_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            forecastIndexMemoryPressureAwareResultHandler,
            xContentRegistry,
            stateManager,
            TimeSeriesSettings.HOURLY_MAINTENANCE
        );

        ForecastSaveResultStrategy forecastSaveResultStrategy = new ForecastSaveResultStrategy(
            forecastIndices.getSchemaVersion(ForecastIndex.RESULT),
            forecastResultWriteQueue
        );

        TaskCacheManager forecastTaskCacheManager = new TaskCacheManager(settings, clusterService);

        forecastTaskManager = new ForecastTaskManager(
            forecastTaskCacheManager,
            client,
            xContentRegistry,
            forecastIndices,
            clusterService,
            settings,
            threadPool,
            stateManager
        );

        ForecastColdStartWorker forecastColdstartQueue = new ForecastColdStartWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_COLD_START_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            forecastColdStarter,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager,
            forecastPriorityCache,
            forecastModelManager,
            forecastSaveResultStrategy,
            forecastTaskManager,
            forecastCheckpointWriteQueue
        );

        Map<String, TimeSeriesStat<?>> forecastStatsMap = ImmutableMap
            .<String, TimeSeriesStat<?>>builder()
            // forecast stats
            .put(StatNames.FORECAST_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.FORECAST_EXECUTE_FAIL_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.FORECAST_HC_EXECUTE_REQUEST_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(StatNames.FORECAST_HC_EXECUTE_FAIL_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(
                StatNames.FORECAST_RESULTS_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, ForecastIndex.RESULT.getIndexName()))
            )
            .put(
                StatNames.FORECAST_MODELS_CHECKPOINT_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, ForecastIndex.CHECKPOINT.getIndexName()))
            )
            .put(
                StatNames.FORECAST_STATE_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, ForecastIndex.STATE.getIndexName()))
            )
            .put(StatNames.FORECASTER_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.SINGLE_STREAM_FORECASTER_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.HC_FORECASTER_COUNT.getName(), new TimeSeriesStat<>(true, new SettableSupplier()))
            .put(StatNames.FORECAST_MODEL_CORRUPTION_COUNT.getName(), new TimeSeriesStat<>(false, new CounterSupplier()))
            .put(
                StatNames.MODEL_INFORMATION.getName(),
                new TimeSeriesStat<>(false, new ForecastModelsOnNodeSupplier(forecastCacheProvider, settings, clusterService))
            )
            .put(
                StatNames.FORECAST_CONFIG_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, ForecastCommonName.CONFIG_INDEX))
            )
            .put(
                StatNames.JOB_INDEX_STATUS.getName(),
                new TimeSeriesStat<>(true, new IndexStatusSupplier(indexUtils, CommonName.JOB_INDEX))
            )
            .put(StatNames.MODEL_COUNT.getName(), new TimeSeriesStat<>(false, new ForecastModelsOnNodeCountSupplier(forecastCacheProvider)))
            .build();

        forecastStats = new ForecastStats(forecastStatsMap);

        ForecastRealTimeInferencer forecastInferencer = new ForecastRealTimeInferencer(
            forecastModelManager,
            forecastStats,
            forecastCheckpoint,
            forecastColdstartQueue,
            forecastSaveResultStrategy,
            forecastCacheProvider,
            threadPool,
            getClock(),
            searchFeatureDao
        );

        ForecastCheckpointReadWorker forecastCheckpointReadQueue = new ForecastCheckpointReadWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            TimeSeriesSettings.QUEUE_MAINTENANCE,
            forecastModelManager,
            forecastCheckpoint,
            forecastColdstartQueue,
            stateManager,
            forecastCacheProvider,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            forecastCheckpointWriteQueue,
            forecastInferencer
        );

        ForecastColdEntityWorker forecastColdEntityQueue = new ForecastColdEntityWorker(
            heapSizeBytes,
            TimeSeriesSettings.FEATURE_REQUEST_SIZE_IN_BYTES,
            ForecastSettings.FORECAST_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            circuitBreakerService,
            threadPool,
            settings,
            TimeSeriesSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            TimeSeriesSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.LOW_SEGMENT_PRUNE_RATIO,
            TimeSeriesSettings.MAINTENANCE_FREQ_CONSTANT,
            forecastCheckpointReadQueue,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            stateManager
        );

        ResultBulkIndexingHandler<ForecastResult, ForecastIndex, ForecastIndexManagement> forecastResultHandler =
            new ResultBulkIndexingHandler<>(
                client,
                settings,
                threadPool,
                ForecastIndex.RESULT.getIndexName(),
                forecastIndices,
                this.clientUtil,
                this.indexUtils,
                clusterService,
                ForecastSettings.FORECAST_BACKOFF_INITIAL_DELAY,
                ForecastSettings.FORECAST_MAX_RETRY_FOR_BACKOFF
            );

        forecastResultResponseRecorder = new ExecuteForecastResultResponseRecorder(
            forecastIndices,
            forecastResultHandler,
            forecastTaskManager,
            nodeFilter,
            threadPool,
            client,
            stateManager,
            getClock(),
            TimeSeriesSettings.NUM_MIN_SAMPLES
        );

        ForecastSearchHandler forecastSearchHandler = new ForecastSearchHandler(settings, clusterService, client, pluginClient);

        forecastIndexJobActionHandler = new ForecastIndexJobActionHandler(
            client,
            forecastIndices,
            xContentRegistry,
            forecastTaskManager,
            forecastResultResponseRecorder,
            stateManager,
            settings
        );

        // return objects used by Guice to inject dependencies for e.g.,
        // transport action handler constructors
        return ImmutableList
            .of(
                // common components
                searchFeatureDao,
                imputer,
                gson,
                jvmService,
                hashRing,
                featureManager,
                stateManager,
                new ClusterEventListener(clusterService, hashRing),
                circuitBreakerService,
                new ClusterManagerEventListener(
                    clusterService,
                    threadPool,
                    client,
                    getClock(),
                    clientUtil,
                    nodeFilter,
                    AnomalyDetectorSettings.AD_CHECKPOINT_TTL,
                    ForecastSettings.FORECAST_CHECKPOINT_TTL,
                    settings
                ),
                nodeFilter,
                // AD components
                anomalyDetectionIndices,
                anomalyDetectorRunner,
                adModelManager,
                adStats,
                adIndexMemoryPressureAwareResultHandler,
                adCheckpoint,
                adCacheProvider,
                adTaskManager,
                adBatchTaskRunner,
                adSearchHandler,
                adColdstartQueue,
                adResultWriteQueue,
                adCheckpointReadQueue,
                adCheckpointWriteQueue,
                adColdEntityQueue,
                adEntityColdStarter,
                adTaskCacheManager,
                adResultResponseRecorder,
                adIndexJobActionHandler,
                adSaveResultStrategy,
                new ADTaskProfileRunner(hashRing, client),
                adInferencer,
                // forecast components
                forecastIndices,
                forecastStats,
                forecastModelManager,
                forecastIndexMemoryPressureAwareResultHandler,
                forecastCheckpoint,
                forecastCacheProvider,
                forecastColdstartQueue,
                forecastResultWriteQueue,
                forecastCheckpointReadQueue,
                forecastCheckpointWriteQueue,
                forecastColdEntityQueue,
                forecastColdStarter,
                forecastTaskManager,
                forecastSearchHandler,
                forecastIndexJobActionHandler,
                forecastTaskCacheManager,
                forecastSaveResultStrategy,
                new ForecastTaskProfileRunner(),
                forecastInferencer,
                pluginClient
            );
    }

    /**
     * createComponents doesn't work for Clock as OS process cannot start
     * complaining it cannot find Clock instances for transport actions constructors.
     * @return a UTC clock
     */
    protected Clock getClock() {
        return Clock.systemUTC();
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return ImmutableList
            .of(
                new ScalingExecutorBuilder(
                    AD_THREAD_POOL_NAME,
                    1,
                    // HCAD can be heavy after supporting 1 million entities.
                    // Limit to use at most half of the processors.
                    Math.max(1, OpenSearchExecutors.allocatedProcessors(settings) / 2),
                    TimeValue.timeValueMinutes(10),
                    AD_THREAD_POOL_PREFIX + AD_THREAD_POOL_NAME
                ),
                new ScalingExecutorBuilder(
                    AD_BATCH_TASK_THREAD_POOL_NAME,
                    1,
                    Math.max(1, OpenSearchExecutors.allocatedProcessors(settings) / 8),
                    TimeValue.timeValueMinutes(10),
                    AD_THREAD_POOL_PREFIX + AD_BATCH_TASK_THREAD_POOL_NAME
                ),
                new ScalingExecutorBuilder(
                    FORECAST_THREAD_POOL_NAME,
                    1,
                    // this pool is used by both real time and run once.
                    // HCAD can be heavy after supporting 1 million entities.
                    // Limit to use at most 3/4 of the processors.
                    Math.max(1, OpenSearchExecutors.allocatedProcessors(settings) * 3 / 4),
                    TimeValue.timeValueMinutes(10),
                    FORECAST_THREAD_POOL_PREFIX + FORECAST_THREAD_POOL_NAME
                )
            );
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> adEnabledSetting = ADEnabledSetting.getInstance().getSettings();
        List<Setting<?>> adNumericSetting = ADNumericSetting.getInstance().getSettings();

        List<Setting<?>> forecastEnabledSetting = ForecastEnabledSetting.getInstance().getSettings();
        List<Setting<?>> forecastNumericSetting = ForecastNumericSetting.getInstance().getSettings();

        List<Setting<?>> timeSeriesEnabledSetting = TimeSeriesEnabledSetting.getInstance().getSettings();

        List<Setting<?>> systemSetting = ImmutableList
            .of(
                // ======================================
                // AD settings
                // ======================================
                // HCAD cache
                LegacyOpenDistroAnomalyDetectorSettings.MAX_CACHE_MISS_HANDLING_PER_SECOND,
                AnomalyDetectorSettings.AD_DEDICATED_CACHE_SIZE,
                // Detector config
                LegacyOpenDistroAnomalyDetectorSettings.DETECTION_INTERVAL,
                LegacyOpenDistroAnomalyDetectorSettings.DETECTION_WINDOW_DELAY,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
                AnomalyDetectorSettings.DETECTION_INTERVAL,
                AnomalyDetectorSettings.DETECTION_WINDOW_DELAY,
                AnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
                // Fault tolerance
                LegacyOpenDistroAnomalyDetectorSettings.REQUEST_TIMEOUT,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                LegacyOpenDistroAnomalyDetectorSettings.COOLDOWN_MINUTES,
                LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_MINUTES,
                LegacyOpenDistroAnomalyDetectorSettings.BACKOFF_INITIAL_DELAY,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF,
                AnomalyDetectorSettings.AD_REQUEST_TIMEOUT,
                AnomalyDetectorSettings.AD_MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                AnomalyDetectorSettings.AD_COOLDOWN_MINUTES,
                AnomalyDetectorSettings.AD_BACKOFF_MINUTES,
                AnomalyDetectorSettings.AD_BACKOFF_INITIAL_DELAY,
                AnomalyDetectorSettings.AD_MAX_RETRY_FOR_BACKOFF,
                // result index rollover
                LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS,
                LegacyOpenDistroAnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                // resource usage control
                LegacyOpenDistroAnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS,
                LegacyOpenDistroAnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_PRIMARY_SHARDS,
                AnomalyDetectorSettings.AD_MODEL_MAX_SIZE_PERCENTAGE,
                AnomalyDetectorSettings.AD_MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.AD_MAX_HC_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.AD_INDEX_PRESSURE_SOFT_LIMIT,
                AnomalyDetectorSettings.AD_INDEX_PRESSURE_HARD_LIMIT,
                AnomalyDetectorSettings.AD_MAX_PRIMARY_SHARDS,
                // Security
                LegacyOpenDistroAnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES,
                AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES,
                // Insights
                AnomalyDetectorSettings.INSIGHTS_ENABLED,
                // Historical
                LegacyOpenDistroAnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE,
                LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
                LegacyOpenDistroAnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE,
                AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE,
                AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS,
                AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
                AnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE,
                AnomalyDetectorSettings.MAX_TOP_ENTITIES_FOR_HISTORICAL_ANALYSIS,
                AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS,
                AnomalyDetectorSettings.MAX_CACHED_DELETED_TASKS,
                // rate limiting
                AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.AD_ENTITY_COLD_START_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
                AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
                AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_BATCH_SIZE,
                AnomalyDetectorSettings.AD_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.AD_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS,
                AnomalyDetectorSettings.AD_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS,
                AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
                AnomalyDetectorSettings.AD_CHECKPOINT_TTL,
                // query limit
                LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW,
                AnomalyDetectorSettings.AD_MAX_ENTITIES_PER_QUERY,
                AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW,
                AnomalyDetectorSettings.MAX_CONCURRENT_PREVIEW,
                AnomalyDetectorSettings.AD_PAGE_SIZE,
                // clean resource
                AnomalyDetectorSettings.DELETE_AD_RESULT_WHEN_DELETE_DETECTOR,
                // stats/profile API
                AnomalyDetectorSettings.AD_MAX_MODEL_SIZE_PER_NODE,
                // ======================================
                // Forecast settings
                // ======================================
                // HC forecasting cache
                ForecastSettings.FORECAST_DEDICATED_CACHE_SIZE,
                // config parameters
                ForecastSettings.FORECAST_INTERVAL,
                ForecastSettings.FORECAST_WINDOW_DELAY,
                // Fault tolerance
                ForecastSettings.FORECAST_BACKOFF_MINUTES,
                ForecastSettings.FORECAST_BACKOFF_INITIAL_DELAY,
                ForecastSettings.FORECAST_MAX_RETRY_FOR_BACKOFF,
                // result index rollover
                ForecastSettings.FORECAST_RESULT_HISTORY_MAX_DOCS_PER_SHARD,
                ForecastSettings.FORECAST_RESULT_HISTORY_RETENTION_PERIOD,
                ForecastSettings.FORECAST_RESULT_HISTORY_ROLLOVER_PERIOD,
                // resource usage control
                ForecastSettings.FORECAST_MODEL_MAX_SIZE_PERCENTAGE,
                // TODO: add validation code
                // ForecastSettings.FORECAST_MAX_SINGLE_STREAM_FORECASTERS,
                // ForecastSettings.FORECAST_MAX_HC_FORECASTERS,
                ForecastSettings.FORECAST_INDEX_PRESSURE_SOFT_LIMIT,
                ForecastSettings.FORECAST_INDEX_PRESSURE_HARD_LIMIT,
                ForecastSettings.FORECAST_MAX_PRIMARY_SHARDS,
                // restful apis
                ForecastSettings.FORECAST_REQUEST_TIMEOUT,
                // resource constraint
                ForecastSettings.MAX_SINGLE_STREAM_FORECASTERS,
                ForecastSettings.MAX_HC_FORECASTERS,
                // Security
                ForecastSettings.FORECAST_FILTER_BY_BACKEND_ROLES,
                // Historical
                ForecastSettings.MAX_OLD_TASK_DOCS_PER_FORECASTER,
                // rate limiting
                ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_CONCURRENCY,
                ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
                ForecastSettings.FORECAST_COLD_START_QUEUE_CONCURRENCY,
                ForecastSettings.FORECAST_RESULT_WRITE_QUEUE_CONCURRENCY,
                ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_BATCH_SIZE,
                ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
                ForecastSettings.FORECAST_RESULT_WRITE_QUEUE_BATCH_SIZE,
                ForecastSettings.FORECAST_COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_COLD_START_QUEUE_MAX_HEAP_PERCENT,
                ForecastSettings.FORECAST_EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS,
                ForecastSettings.FORECAST_EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS,
                ForecastSettings.FORECAST_CHECKPOINT_SAVING_FREQ,
                ForecastSettings.FORECAST_CHECKPOINT_TTL,
                // query limit
                ForecastSettings.FORECAST_MAX_ENTITIES_PER_INTERVAL,
                ForecastSettings.FORECAST_PAGE_SIZE,
                // stats/profile API
                ForecastSettings.FORECAST_MAX_MODEL_SIZE_PER_NODE,
                // clean resource
                ForecastSettings.DELETE_FORECAST_RESULT_WHEN_DELETE_FORECASTER,
                // ======================================
                // Common settings
                // ======================================
                // Fault tolerance
                TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                TimeSeriesSettings.BACKOFF_MINUTES,
                TimeSeriesSettings.COOLDOWN_MINUTES,
                // tasks
                TimeSeriesSettings.MAX_CACHED_DELETED_TASKS
            );
        return unmodifiableList(
            Stream
                .of(
                    adEnabledSetting.stream(),
                    forecastEnabledSetting.stream(),
                    timeSeriesEnabledSetting.stream(),
                    systemSetting.stream(),
                    adNumericSetting.stream(),
                    forecastNumericSetting.stream()
                )
                .reduce(Stream::concat)
                .orElseGet(Stream::empty)
                .collect(Collectors.toList())
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return ImmutableList
            .of(
                AnomalyDetector.XCONTENT_REGISTRY,
                AnomalyResult.XCONTENT_REGISTRY,
                DetectorInternalState.XCONTENT_REGISTRY,
                Job.XCONTENT_REGISTRY,
                Forecaster.XCONTENT_REGISTRY
            );
    }

    /*
     * Register action and handler so that transportClient can find proxy for action
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays
            .asList(
                // AD
                new ActionHandler<>(DeleteADModelAction.INSTANCE, DeleteADModelTransportAction.class),
                new ActionHandler<>(StopDetectorAction.INSTANCE, StopDetectorTransportAction.class),
                new ActionHandler<>(RCFResultAction.INSTANCE, RCFResultTransportAction.class),
                new ActionHandler<>(ThresholdResultAction.INSTANCE, ThresholdResultTransportAction.class),
                new ActionHandler<>(AnomalyResultAction.INSTANCE, AnomalyResultTransportAction.class),
                new ActionHandler<>(CronAction.INSTANCE, CronTransportAction.class),
                new ActionHandler<>(ADStatsNodesAction.INSTANCE, ADStatsNodesTransportAction.class),
                new ActionHandler<>(ADProfileAction.INSTANCE, ADProfileTransportAction.class),
                new ActionHandler<>(RCFPollingAction.INSTANCE, RCFPollingTransportAction.class),
                new ActionHandler<>(SearchAnomalyDetectorAction.INSTANCE, SearchAnomalyDetectorTransportAction.class),
                new ActionHandler<>(SearchAnomalyResultAction.INSTANCE, SearchAnomalyResultTransportAction.class),
                new ActionHandler<>(SearchADTasksAction.INSTANCE, SearchADTasksTransportAction.class),
                new ActionHandler<>(StatsAnomalyDetectorAction.INSTANCE, StatsAnomalyDetectorTransportAction.class),
                new ActionHandler<>(DeleteAnomalyDetectorAction.INSTANCE, DeleteAnomalyDetectorTransportAction.class),
                new ActionHandler<>(GetAnomalyDetectorAction.INSTANCE, GetAnomalyDetectorTransportAction.class),
                new ActionHandler<>(IndexAnomalyDetectorAction.INSTANCE, IndexAnomalyDetectorTransportAction.class),
                new ActionHandler<>(AnomalyDetectorJobAction.INSTANCE, AnomalyDetectorJobTransportAction.class),
                new ActionHandler<>(ADResultBulkAction.INSTANCE, ADResultBulkTransportAction.class),
                new ActionHandler<>(EntityADResultAction.INSTANCE, EntityADResultTransportAction.class),
                new ActionHandler<>(ADEntityProfileAction.INSTANCE, ADEntityProfileTransportAction.class),
                new ActionHandler<>(SearchAnomalyDetectorInfoAction.INSTANCE, SearchAnomalyDetectorInfoTransportAction.class),
                new ActionHandler<>(PreviewAnomalyDetectorAction.INSTANCE, PreviewAnomalyDetectorTransportAction.class),
                new ActionHandler<>(ADBatchAnomalyResultAction.INSTANCE, ADBatchAnomalyResultTransportAction.class),
                new ActionHandler<>(ADBatchTaskRemoteExecutionAction.INSTANCE, ADBatchTaskRemoteExecutionTransportAction.class),
                new ActionHandler<>(ADTaskProfileAction.INSTANCE, ADTaskProfileTransportAction.class),
                new ActionHandler<>(ADCancelTaskAction.INSTANCE, ADCancelTaskTransportAction.class),
                new ActionHandler<>(ForwardADTaskAction.INSTANCE, ForwardADTaskTransportAction.class),
                new ActionHandler<>(DeleteAnomalyResultsAction.INSTANCE, DeleteAnomalyResultsTransportAction.class),
                new ActionHandler<>(SearchTopAnomalyResultAction.INSTANCE, SearchTopAnomalyResultTransportAction.class),
                new ActionHandler<>(ValidateAnomalyDetectorAction.INSTANCE, ValidateAnomalyDetectorTransportAction.class),
                new ActionHandler<>(ADSingleStreamResultAction.INSTANCE, ADSingleStreamResultTransportAction.class),
                new ActionHandler<>(ADHCImputeAction.INSTANCE, ADHCImputeTransportAction.class),
                new ActionHandler<>(SuggestAnomalyDetectorParamAction.INSTANCE, SuggestAnomalyDetectorParamTransportAction.class),
                new ActionHandler<>(InsightsJobAction.INSTANCE, InsightsJobTransportAction.class),

                // forecast
                new ActionHandler<>(IndexForecasterAction.INSTANCE, IndexForecasterTransportAction.class),
                new ActionHandler<>(ForecastResultAction.INSTANCE, ForecastResultTransportAction.class),
                new ActionHandler<>(EntityForecastResultAction.INSTANCE, EntityForecastResultTransportAction.class),
                new ActionHandler<>(ForecastResultBulkAction.INSTANCE, ForecastResultBulkTransportAction.class),
                new ActionHandler<>(ForecastSingleStreamResultAction.INSTANCE, ForecastSingleStreamResultTransportAction.class),
                new ActionHandler<>(ForecasterJobAction.INSTANCE, ForecasterJobTransportAction.class),
                new ActionHandler<>(StopForecasterAction.INSTANCE, StopForecasterTransportAction.class),
                new ActionHandler<>(DeleteForecastModelAction.INSTANCE, DeleteForecastModelTransportAction.class),
                new ActionHandler<>(GetForecasterAction.INSTANCE, GetForecasterTransportAction.class),
                new ActionHandler<>(DeleteForecasterAction.INSTANCE, DeleteForecasterTransportAction.class),
                new ActionHandler<>(SearchForecasterAction.INSTANCE, SearchForecasterTransportAction.class),
                new ActionHandler<>(SearchForecasterInfoAction.INSTANCE, SearchForecasterInfoTransportAction.class),
                new ActionHandler<>(SearchTopForecastResultAction.INSTANCE, SearchTopForecastResultTransportAction.class),
                new ActionHandler<>(ForecastEntityProfileAction.INSTANCE, ForecastEntityProfileTransportAction.class),
                new ActionHandler<>(ForecastProfileAction.INSTANCE, ForecastProfileTransportAction.class),
                new ActionHandler<>(SearchForecastTasksAction.INSTANCE, SearchForecastTasksTransportAction.class),
                new ActionHandler<>(StatsForecasterAction.INSTANCE, StatsForecasterTransportAction.class),
                new ActionHandler<>(ForecastStatsNodesAction.INSTANCE, ForecastStatsNodesTransportAction.class),
                new ActionHandler<>(ForecastRunOnceAction.INSTANCE, ForecastRunOnceTransportAction.class),
                new ActionHandler<>(ForecastRunOnceProfileAction.INSTANCE, ForecastRunOnceProfileTransportAction.class),
                new ActionHandler<>(ValidateForecasterAction.INSTANCE, ValidateForecasterTransportAction.class),
                new ActionHandler<>(SuggestForecasterParamAction.INSTANCE, SuggestForecasterParamTransportAction.class)
            );
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        List<SystemIndexDescriptor> systemIndexDescriptors = new ArrayList<>();
        systemIndexDescriptors.add(new SystemIndexDescriptor(ADCommonName.CONFIG_INDEX, "Anomaly detection config index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(ForecastCommonName.CONFIG_INDEX, "Forecasting config index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(ALL_AD_RESULTS_INDEX_PATTERN, "AD result index pattern"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(CHECKPOINT_INDEX_NAME, "AD Checkpoints index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(DETECTION_STATE_INDEX, "AD State index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(FORECAST_CHECKPOINT_INDEX_NAME, "Forecast Checkpoints index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(FORECAST_STATE_INDEX, "Forecast state index"));
        systemIndexDescriptors.add(new SystemIndexDescriptor(JOB_INDEX, "Time Series Analytics job index"));
        return systemIndexDescriptors;
    }

    @Override
    public String getJobType() {
        return TIME_SERIES_JOB_TYPE;
    }

    @Override
    public String getJobIndex() {
        return CommonName.JOB_INDEX;
    }

    @Override
    public ScheduledJobRunner getJobRunner() {
        return JobRunner.getJobRunnerInstance();
    }

    @Override
    public ScheduledJobParser getJobParser() {
        return (parser, id, jobDocVersion) -> {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            return Job.parse(parser);
        };
    }

    @Override
    public void close() {
        if (serializeRCFBufferPool != null) {
            try {
                AccessController.doPrivileged(() -> {
                    serializeRCFBufferPool.clear();
                    serializeRCFBufferPool.close();
                });
                serializeRCFBufferPool = null;
            } catch (Exception e) {
                LOG.error("Failed to shut down object Pool", e);
            }
        }
    }

    @Override
    public void assignSubject(PluginSubject pluginSubject) {
        if (this.pluginClient != null) {
            this.pluginClient.setSubject(pluginSubject);
        }
    }
}
