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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad;

import static java.util.Collections.unmodifiableList;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.SpecialPermission;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParserUtils;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.caching.CacheProvider;
import com.amazon.opendistroforelasticsearch.ad.caching.EntityCache;
import com.amazon.opendistroforelasticsearch.ad.caching.PriorityCache;
import com.amazon.opendistroforelasticsearch.ad.cluster.ADClusterEventListener;
import com.amazon.opendistroforelasticsearch.ad.cluster.HashRing;
import com.amazon.opendistroforelasticsearch.ad.cluster.MasterEventListener;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.IntegerSensitiveSingleFeatureLinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.Interpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.LinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import com.amazon.opendistroforelasticsearch.ad.feature.FeatureManager;
import com.amazon.opendistroforelasticsearch.ad.feature.SearchFeatureDao;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityColdStarter;
import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelPartitioner;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.DetectorInternalState;
import com.amazon.opendistroforelasticsearch.ad.rest.RestAnomalyDetectorJobAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestDeleteAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestExecuteAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestGetAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestIndexAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestPreviewAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestSearchADTasksAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestSearchAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestSearchAnomalyDetectorInfoAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestSearchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.rest.RestStatsAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.opendistroforelasticsearch.ad.settings.EnabledSetting;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStat;
import com.amazon.opendistroforelasticsearch.ad.stats.ADStats;
import com.amazon.opendistroforelasticsearch.ad.stats.StatNames;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.CounterSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.IndexStatusSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.ModelsOnNodeSupplier;
import com.amazon.opendistroforelasticsearch.ad.stats.suppliers.SettableSupplier;
import com.amazon.opendistroforelasticsearch.ad.task.ADBatchTaskRunner;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskCacheManager;
import com.amazon.opendistroforelasticsearch.ad.task.ADTaskManager;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchAnomalyResultTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchTaskRemoteExecutionAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADBatchTaskRemoteExecutionTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADCancelTaskAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADCancelTaskTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADResultBulkAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADResultBulkTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodesAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADStatsNodesTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ADTaskProfileTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectorJobAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyDetectorJobTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AnomalyResultTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.CronAction;
import com.amazon.opendistroforelasticsearch.ad.transport.CronTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.DeleteAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.DeleteAnomalyDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.DeleteModelAction;
import com.amazon.opendistroforelasticsearch.ad.transport.DeleteModelTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.EntityProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.EntityProfileTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.EntityResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.EntityResultTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ForwardADTaskAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ForwardADTaskTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.GetAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.GetAnomalyDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.IndexAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.IndexAnomalyDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.PreviewAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.PreviewAnomalyDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ProfileTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFPollingTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.RCFResultTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchADTasksAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchADTasksTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyDetectorInfoAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyDetectorInfoTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchAnomalyResultTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.StatsAnomalyDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.StatsAnomalyDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorAction;
import com.amazon.opendistroforelasticsearch.ad.transport.StopDetectorTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ThresholdResultAction;
import com.amazon.opendistroforelasticsearch.ad.transport.ThresholdResultTransportAction;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.ADSearchHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyIndexHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.AnomalyResultBulkIndexHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.DetectionStateHandler;
import com.amazon.opendistroforelasticsearch.ad.transport.handler.MultiEntityResultHandler;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.DiscoveryNodeFilterer;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.Throttler;
import com.amazon.opendistroforelasticsearch.ad.util.ThrowingConsumerWrapper;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobSchedulerExtension;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParser;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.randomcutforest.serialize.RandomCutForestSerDe;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * Entry point of AD plugin.
 */
public class AnomalyDetectorPlugin extends Plugin implements ActionPlugin, ScriptPlugin, JobSchedulerExtension {

    private static final Logger LOG = LogManager.getLogger(AnomalyDetectorPlugin.class);

    public static final String AD_BASE_URI = "/_opendistro/_anomaly_detection";
    public static final String AD_BASE_DETECTORS_URI = AD_BASE_URI + "/detectors";
    public static final String AD_THREAD_POOL_PREFIX = "opendistro.ad.";
    public static final String AD_THREAD_POOL_NAME = "ad-threadpool";
    public static final String AD_BATCH_TASK_THREAD_POOL_NAME = "ad-batch-task-threadpool";
    public static final String AD_JOB_TYPE = "opendistro_anomaly_detector";
    private static Gson gson;
    private AnomalyDetectionIndices anomalyDetectionIndices;
    private AnomalyDetectorRunner anomalyDetectorRunner;
    private Client client;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private ADStats adStats;
    private ClientUtil clientUtil;
    private DiscoveryNodeFilterer nodeFilter;
    private IndexUtils indexUtils;
    private DetectionStateHandler detectorStateHandler;
    private ADTaskCacheManager adTaskCacheManager;
    private ADTaskManager adTaskManager;
    private ADBatchTaskRunner adBatchTaskRunner;

    static {
        SpecialPermission.check();
        // gson intialization requires "java.lang.RuntimePermission" "accessDeclaredMembers" to
        // initialize ConstructorConstructor
        AccessController.doPrivileged((PrivilegedAction<Void>) AnomalyDetectorPlugin::initGson);
    }

    public AnomalyDetectorPlugin() {}

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
        AnomalyIndexHandler<AnomalyResult> anomalyResultHandler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            settings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            this.clientUtil,
            this.indexUtils,
            clusterService
        );

        AnomalyDetectorJobRunner jobRunner = AnomalyDetectorJobRunner.getJobRunnerInstance();
        jobRunner.setClient(client);
        jobRunner.setClientUtil(clientUtil);
        jobRunner.setThreadPool(threadPool);
        jobRunner.setAnomalyResultHandler(anomalyResultHandler);
        jobRunner.setDetectionStateHandler(detectorStateHandler);
        jobRunner.setSettings(settings);
        jobRunner.setIndexUtil(anomalyDetectionIndices);
        jobRunner.setAdTaskManager(adTaskManager);

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

        return ImmutableList
            .of(
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
                previewAnomalyDetectorAction
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
        EnabledSetting.getInstance().init(clusterService);
        this.client = client;
        this.threadPool = threadPool;
        Settings settings = environment.settings();
        Throttler throttler = new Throttler(getClock());
        this.clientUtil = new ClientUtil(settings, client, throttler, threadPool);
        this.indexUtils = new IndexUtils(client, clientUtil, clusterService, indexNameExpressionResolver);
        this.nodeFilter = new DiscoveryNodeFilterer(clusterService);
        this.anomalyDetectionIndices = new AnomalyDetectionIndices(client, clusterService, threadPool, settings, nodeFilter);
        this.clusterService = clusterService;

        SingleFeatureLinearUniformInterpolator singleFeatureLinearUniformInterpolator =
            new IntegerSensitiveSingleFeatureLinearUniformInterpolator();
        Interpolator interpolator = new LinearUniformInterpolator(singleFeatureLinearUniformInterpolator);
        SearchFeatureDao searchFeatureDao = new SearchFeatureDao(
            client,
            xContentRegistry,
            interpolator,
            clientUtil,
            threadPool,
            settings,
            clusterService
        );

        JvmService jvmService = new JvmService(environment.settings());
        RandomCutForestSerDe rcfSerde = new RandomCutForestSerDe();
        CheckpointDao checkpoint = new CheckpointDao(
            client,
            clientUtil,
            CommonName.CHECKPOINT_INDEX_NAME,
            gson,
            rcfSerde,
            HybridThresholdingModel.class,
            getClock(),
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            anomalyDetectionIndices,
            AnomalyDetectorSettings.MAX_BULK_CHECKPOINT_SIZE,
            AnomalyDetectorSettings.CHECKPOINT_BULK_PER_SECOND
        );

        double modelMaxSizePercent = AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.get(settings);

        MemoryTracker memoryTracker = new MemoryTracker(
            jvmService,
            modelMaxSizePercent,
            AnomalyDetectorSettings.DESIRED_MODEL_SIZE_PERCENTAGE,
            clusterService,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE
        );

        ModelPartitioner modelPartitioner = new ModelPartitioner(
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.NUM_TREES,
            nodeFilter,
            memoryTracker
        );

        NodeStateManager stateManager = new NodeStateManager(
            client,
            xContentRegistry,
            settings,
            clientUtil,
            getClock(),
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            modelPartitioner
        );

        FeatureManager featureManager = new FeatureManager(
            searchFeatureDao,
            interpolator,
            getClock(),
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.TRAIN_SAMPLE_TIME_RANGE_IN_HOURS,
            AnomalyDetectorSettings.MIN_TRAIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SHINGLE_PROPORTION_MISSING,
            AnomalyDetectorSettings.MAX_IMPUTATION_NEIGHBOR_DISTANCE,
            AnomalyDetectorSettings.PREVIEW_SAMPLE_RATE,
            AnomalyDetectorSettings.MAX_PREVIEW_SAMPLES,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            threadPool,
            AD_THREAD_POOL_NAME
        );

        EntityColdStarter entityColdStarter = new EntityColdStarter(
            getClock(),
            threadPool,
            stateManager,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.MULTI_ENTITY_NUM_TREES,
            AnomalyDetectorSettings.TIME_DECAY,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            interpolator,
            searchFeatureDao,
            AnomalyDetectorSettings.DEFAULT_MULTI_ENTITY_SHINGLE,
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
            AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
            AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
            AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES,
            featureManager,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.MAX_SMALL_STATES,
            checkpoint,
            settings
        );

        ModelManager modelManager = new ModelManager(
            rcfSerde,
            checkpoint,
            gson,
            getClock(),
            AnomalyDetectorSettings.NUM_TREES,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.TIME_DECAY,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
            AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
            AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
            AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES,
            HybridThresholdingModel.class,
            AnomalyDetectorSettings.MIN_PREVIEW_SIZE,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            entityColdStarter,
            modelPartitioner,
            featureManager,
            memoryTracker
        );

        EntityCache cache = new PriorityCache(
            checkpoint,
            AnomalyDetectorSettings.DEDICATED_CACHE_SIZE,
            AnomalyDetectorSettings.CHECKPOINT_TTL,
            AnomalyDetectorSettings.MAX_INACTIVE_ENTITIES,
            memoryTracker,
            modelManager,
            AnomalyDetectorSettings.MULTI_ENTITY_NUM_TREES,
            getClock(),
            clusterService,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES,
            settings,
            threadPool,
            AnomalyDetectorSettings.MAX_CACHE_MISS_HANDLING_PER_SECOND.get(settings)
        );

        CacheProvider cacheProvider = new CacheProvider(cache);

        HashRing hashRing = new HashRing(nodeFilter, getClock(), settings);

        anomalyDetectorRunner = new AnomalyDetectorRunner(modelManager, featureManager, AnomalyDetectorSettings.MAX_PREVIEW_RESULTS);

        Map<String, ADStat<?>> stats = ImmutableMap
            .<String, ADStat<?>>builder()
            .put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_HC_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.MODEL_INFORMATION.getName(), new ADStat<>(false, new ModelsOnNodeSupplier(modelManager, cacheProvider)))
            .put(
                StatNames.ANOMALY_DETECTORS_INDEX_STATUS.getName(),
                new ADStat<>(true, new IndexStatusSupplier(indexUtils, AnomalyDetector.ANOMALY_DETECTORS_INDEX))
            )
            .put(
                StatNames.ANOMALY_RESULTS_INDEX_STATUS.getName(),
                new ADStat<>(true, new IndexStatusSupplier(indexUtils, CommonName.ANOMALY_RESULT_INDEX_ALIAS))
            )
            .put(
                StatNames.MODELS_CHECKPOINT_INDEX_STATUS.getName(),
                new ADStat<>(true, new IndexStatusSupplier(indexUtils, CommonName.CHECKPOINT_INDEX_NAME))
            )
            .put(
                StatNames.ANOMALY_DETECTION_JOB_INDEX_STATUS.getName(),
                new ADStat<>(true, new IndexStatusSupplier(indexUtils, AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX))
            )
            .put(
                StatNames.ANOMALY_DETECTION_STATE_STATUS.getName(),
                new ADStat<>(true, new IndexStatusSupplier(indexUtils, CommonName.DETECTION_STATE_INDEX))
            )
            .put(StatNames.DETECTOR_COUNT.getName(), new ADStat<>(true, new SettableSupplier()))
            .put(StatNames.SINGLE_ENTITY_DETECTOR_COUNT.getName(), new ADStat<>(true, new SettableSupplier()))
            .put(StatNames.MULTI_ENTITY_DETECTOR_COUNT.getName(), new ADStat<>(true, new SettableSupplier()))
            .put(StatNames.AD_EXECUTING_BATCH_TASK_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_CANCELED_BATCH_TASK_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_TOTAL_BATCH_TASK_EXECUTION_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_BATCH_TASK_FAILURE_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .build();

        adStats = new ADStats(indexUtils, modelManager, stats);
        ADCircuitBreakerService adCircuitBreakerService = new ADCircuitBreakerService(jvmService).init();
        this.detectorStateHandler = new DetectionStateHandler(
            client,
            settings,
            threadPool,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initDetectionStateIndex),
            anomalyDetectionIndices::doesDetectorStateIndexExist,
            this.clientUtil,
            this.indexUtils,
            clusterService,
            xContentRegistry,
            stateManager
        );

        MultiEntityResultHandler multiEntityResultHandler = new MultiEntityResultHandler(
            client,
            settings,
            threadPool,
            anomalyDetectionIndices,
            this.clientUtil,
            this.indexUtils,
            clusterService,
            stateManager
        );

        adTaskCacheManager = new ADTaskCacheManager(settings, clusterService, memoryTracker);
        adTaskManager = new ADTaskManager(
            settings,
            clusterService,
            client,
            xContentRegistry,
            anomalyDetectionIndices,
            nodeFilter,
            hashRing,
            adTaskCacheManager
        );
        AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler = new AnomalyResultBulkIndexHandler(
            client,
            settings,
            threadPool,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            this.clientUtil,
            this.indexUtils,
            clusterService,
            anomalyDetectionIndices
        );
        adBatchTaskRunner = new ADBatchTaskRunner(
            settings,
            threadPool,
            clusterService,
            client,
            nodeFilter,
            indexNameExpressionResolver,
            adCircuitBreakerService,
            featureManager,
            adTaskManager,
            anomalyDetectionIndices,
            adStats,
            anomalyResultBulkIndexHandler,
            adTaskCacheManager
        );

        ADSearchHandler adSearchHandler = new ADSearchHandler(settings, clusterService, client);

        // return objects used by Guice to inject dependencies for e.g.,
        // transport action handler constructors
        return ImmutableList
            .of(
                anomalyDetectionIndices,
                anomalyDetectorRunner,
                searchFeatureDao,
                singleFeatureLinearUniformInterpolator,
                interpolator,
                gson,
                jvmService,
                hashRing,
                featureManager,
                modelManager,
                stateManager,
                new ADClusterEventListener(clusterService, hashRing, modelManager, nodeFilter),
                adCircuitBreakerService,
                adStats,
                new MasterEventListener(clusterService, threadPool, client, getClock(), clientUtil, nodeFilter),
                nodeFilter,
                detectorStateHandler,
                multiEntityResultHandler,
                checkpoint,
                modelPartitioner,
                cacheProvider,
                adTaskManager,
                adBatchTaskRunner,
                adSearchHandler
            );
    }

    /**
     * createComponents doesn't work for Clock as ES process cannot start
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
                    Math.max(1, OpenSearchExecutors.allocatedProcessors(settings) / 4),
                    TimeValue.timeValueMinutes(10),
                    AD_THREAD_POOL_PREFIX + AD_THREAD_POOL_NAME
                ),
                new ScalingExecutorBuilder(
                    AD_BATCH_TASK_THREAD_POOL_NAME,
                    1,
                    Math.max(1, OpenSearchExecutors.allocatedProcessors(settings) / 8),
                    TimeValue.timeValueMinutes(10),
                    AD_THREAD_POOL_PREFIX + AD_BATCH_TASK_THREAD_POOL_NAME
                )
            );
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> enabledSetting = EnabledSetting.getInstance().getSettings();

        List<Setting<?>> systemSetting = ImmutableList
            .of(
                AnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.MAX_ANOMALY_FEATURES,
                AnomalyDetectorSettings.REQUEST_TIMEOUT,
                AnomalyDetectorSettings.DETECTION_INTERVAL,
                AnomalyDetectorSettings.DETECTION_WINDOW_DELAY,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS,
                AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                AnomalyDetectorSettings.COOLDOWN_MINUTES,
                AnomalyDetectorSettings.BACKOFF_MINUTES,
                AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY,
                AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
                AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY,
                AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW,
                AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT,
                AnomalyDetectorSettings.MAX_PRIMARY_SHARDS,
                AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES,
                AnomalyDetectorSettings.MAX_CACHE_MISS_HANDLING_PER_SECOND,
                AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE,
                AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS,
                AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
                AnomalyDetectorSettings.BATCH_TASK_PIECE_SIZE,
                AnomalyDetectorSettings.MAX_TOP_ENTITIES_FOR_HISTORICAL_ANALYSIS,
                AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS
            );
        return unmodifiableList(Stream.concat(enabledSetting.stream(), systemSetting.stream()).collect(Collectors.toList()));
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return ImmutableList
            .of(
                AnomalyDetector.XCONTENT_REGISTRY,
                AnomalyResult.XCONTENT_REGISTRY,
                DetectorInternalState.XCONTENT_REGISTRY,
                AnomalyDetectorJob.XCONTENT_REGISTRY
            );
    }

    /*
     * Register action and handler so that transportClient can find proxy for action
     */
    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays
            .asList(
                new ActionHandler<>(DeleteModelAction.INSTANCE, DeleteModelTransportAction.class),
                new ActionHandler<>(StopDetectorAction.INSTANCE, StopDetectorTransportAction.class),
                new ActionHandler<>(RCFResultAction.INSTANCE, RCFResultTransportAction.class),
                new ActionHandler<>(ThresholdResultAction.INSTANCE, ThresholdResultTransportAction.class),
                new ActionHandler<>(AnomalyResultAction.INSTANCE, AnomalyResultTransportAction.class),
                new ActionHandler<>(CronAction.INSTANCE, CronTransportAction.class),
                new ActionHandler<>(ADStatsNodesAction.INSTANCE, ADStatsNodesTransportAction.class),
                new ActionHandler<>(ProfileAction.INSTANCE, ProfileTransportAction.class),
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
                new ActionHandler<>(EntityResultAction.INSTANCE, EntityResultTransportAction.class),
                new ActionHandler<>(EntityProfileAction.INSTANCE, EntityProfileTransportAction.class),
                new ActionHandler<>(SearchAnomalyDetectorInfoAction.INSTANCE, SearchAnomalyDetectorInfoTransportAction.class),
                new ActionHandler<>(PreviewAnomalyDetectorAction.INSTANCE, PreviewAnomalyDetectorTransportAction.class),
                new ActionHandler<>(ADBatchAnomalyResultAction.INSTANCE, ADBatchAnomalyResultTransportAction.class),
                new ActionHandler<>(ADBatchTaskRemoteExecutionAction.INSTANCE, ADBatchTaskRemoteExecutionTransportAction.class),
                new ActionHandler<>(ADTaskProfileAction.INSTANCE, ADTaskProfileTransportAction.class),
                new ActionHandler<>(ADCancelTaskAction.INSTANCE, ADCancelTaskTransportAction.class),
                new ActionHandler<>(ForwardADTaskAction.INSTANCE, ForwardADTaskTransportAction.class)
            );
    }

    @Override
    public String getJobType() {
        return AD_JOB_TYPE;
    }

    @Override
    public String getJobIndex() {
        return AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
    }

    @Override
    public ScheduledJobRunner getJobRunner() {
        return AnomalyDetectorJobRunner.getJobRunnerInstance();
    }

    @Override
    public ScheduledJobParser getJobParser() {
        return (parser, id, jobDocVersion) -> {
            XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            return AnomalyDetectorJob.parse(parser);
        };
    }
}
