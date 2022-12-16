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

import static java.util.Collections.unmodifiableList;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
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
import org.opensearch.action.ActionResponse;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.caching.PriorityCache;
import org.opensearch.ad.cluster.ADClusterEventListener;
import org.opensearch.ad.cluster.ADDataMigrator;
import org.opensearch.ad.cluster.ClusterManagerEventListener;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.dataprocessor.IntegerSensitiveSingleFeatureLinearUniformInterpolator;
import org.opensearch.ad.dataprocessor.Interpolator;
import org.opensearch.ad.dataprocessor.LinearUniformInterpolator;
import org.opensearch.ad.dataprocessor.SingleFeatureLinearUniformInterpolator;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityColdStarter;
import org.opensearch.ad.ml.HybridThresholdingModel;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorInternalState;
import org.opensearch.ad.ratelimit.CheckPointMaintainRequestAdapter;
import org.opensearch.ad.ratelimit.CheckpointMaintainWorker;
import org.opensearch.ad.ratelimit.CheckpointReadWorker;
import org.opensearch.ad.ratelimit.CheckpointWriteWorker;
import org.opensearch.ad.ratelimit.ColdEntityWorker;
import org.opensearch.ad.ratelimit.EntityColdStartWorker;
import org.opensearch.ad.ratelimit.ResultWriteWorker;
import org.opensearch.ad.rest.RestAnomalyDetectorJobAction;
import org.opensearch.ad.rest.RestDeleteAnomalyDetectorAction;
import org.opensearch.ad.rest.RestDeleteAnomalyResultsAction;
import org.opensearch.ad.rest.RestExecuteAnomalyDetectorAction;
import org.opensearch.ad.rest.RestGetAnomalyDetectorAction;
import org.opensearch.ad.rest.RestIndexAnomalyDetectorAction;
import org.opensearch.ad.rest.RestPreviewAnomalyDetectorAction;
import org.opensearch.ad.rest.RestSearchADTasksAction;
import org.opensearch.ad.rest.RestSearchAnomalyDetectorAction;
import org.opensearch.ad.rest.RestSearchAnomalyDetectorInfoAction;
import org.opensearch.ad.rest.RestSearchAnomalyResultAction;
import org.opensearch.ad.rest.RestSearchTopAnomalyResultAction;
import org.opensearch.ad.rest.RestStatsAnomalyDetectorAction;
import org.opensearch.ad.rest.RestValidateAnomalyDetectorAction;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.settings.LegacyOpenDistroAnomalyDetectorSettings;
import org.opensearch.ad.settings.NumericSetting;
import org.opensearch.ad.stats.ADStat;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.StatNames;
import org.opensearch.ad.stats.suppliers.CounterSupplier;
import org.opensearch.ad.stats.suppliers.IndexStatusSupplier;
import org.opensearch.ad.stats.suppliers.ModelsOnNodeCountSupplier;
import org.opensearch.ad.stats.suppliers.ModelsOnNodeSupplier;
import org.opensearch.ad.stats.suppliers.SettableSupplier;
import org.opensearch.ad.task.ADBatchTaskRunner;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADBatchAnomalyResultAction;
import org.opensearch.ad.transport.ADBatchAnomalyResultTransportAction;
import org.opensearch.ad.transport.ADBatchTaskRemoteExecutionAction;
import org.opensearch.ad.transport.ADBatchTaskRemoteExecutionTransportAction;
import org.opensearch.ad.transport.ADCancelTaskAction;
import org.opensearch.ad.transport.ADCancelTaskTransportAction;
import org.opensearch.ad.transport.ADResultBulkAction;
import org.opensearch.ad.transport.ADResultBulkTransportAction;
import org.opensearch.ad.transport.ADStatsNodesAction;
import org.opensearch.ad.transport.ADStatsNodesTransportAction;
import org.opensearch.ad.transport.ADTaskProfileAction;
import org.opensearch.ad.transport.ADTaskProfileTransportAction;
import org.opensearch.ad.transport.AnomalyDetectorJobAction;
import org.opensearch.ad.transport.AnomalyDetectorJobTransportAction;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultTransportAction;
import org.opensearch.ad.transport.CronAction;
import org.opensearch.ad.transport.CronTransportAction;
import org.opensearch.ad.transport.DeleteAnomalyDetectorAction;
import org.opensearch.ad.transport.DeleteAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.DeleteAnomalyResultsAction;
import org.opensearch.ad.transport.DeleteAnomalyResultsTransportAction;
import org.opensearch.ad.transport.DeleteModelAction;
import org.opensearch.ad.transport.DeleteModelTransportAction;
import org.opensearch.ad.transport.EntityProfileAction;
import org.opensearch.ad.transport.EntityProfileTransportAction;
import org.opensearch.ad.transport.EntityResultAction;
import org.opensearch.ad.transport.EntityResultTransportAction;
import org.opensearch.ad.transport.ForwardADTaskAction;
import org.opensearch.ad.transport.ForwardADTaskTransportAction;
import org.opensearch.ad.transport.GetAnomalyDetectorAction;
import org.opensearch.ad.transport.GetAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.IndexAnomalyDetectorAction;
import org.opensearch.ad.transport.IndexAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.PreviewAnomalyDetectorAction;
import org.opensearch.ad.transport.PreviewAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.ProfileAction;
import org.opensearch.ad.transport.ProfileTransportAction;
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
import org.opensearch.ad.transport.ThresholdResultAction;
import org.opensearch.ad.transport.ThresholdResultTransportAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorAction;
import org.opensearch.ad.transport.ValidateAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.handler.ADSearchHandler;
import org.opensearch.ad.transport.handler.AnomalyIndexHandler;
import org.opensearch.ad.transport.handler.AnomalyResultBulkIndexHandler;
import org.opensearch.ad.transport.handler.MultiEntityResultHandler;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.ad.util.IndexUtils;
import org.opensearch.ad.util.Throttler;
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
import org.opensearch.jobscheduler.spi.JobSchedulerExtension;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.monitor.jvm.JvmInfo;
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

import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV3StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.protostuff.LinkedBuffer;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

/**
 * Entry point of AD plugin.
 */
public class AnomalyDetectorPlugin extends Plugin implements ActionPlugin, ScriptPlugin, JobSchedulerExtension {

    private static final Logger LOG = LogManager.getLogger(AnomalyDetectorPlugin.class);

    public static final String LEGACY_AD_BASE = "/_opendistro/_anomaly_detection";
    public static final String LEGACY_OPENDISTRO_AD_BASE_URI = LEGACY_AD_BASE + "/detectors";
    public static final String AD_BASE_URI = "/_plugins/_anomaly_detection";
    public static final String AD_BASE_DETECTORS_URI = AD_BASE_URI + "/detectors";
    public static final String AD_THREAD_POOL_PREFIX = "opensearch.ad.";
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
    private ADTaskManager adTaskManager;
    private ADBatchTaskRunner adBatchTaskRunner;
    // package private for testing
    GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;
    private NodeStateManager stateManager;
    private ExecuteADResultResponseRecorder adResultResponseRecorder;

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
        AnomalyDetectorJobRunner jobRunner = AnomalyDetectorJobRunner.getJobRunnerInstance();
        jobRunner.setClient(client);
        jobRunner.setThreadPool(threadPool);
        jobRunner.setSettings(settings);
        jobRunner.setAnomalyDetectionIndices(anomalyDetectionIndices);
        jobRunner.setAdTaskManager(adTaskManager);
        jobRunner.setNodeStateManager(stateManager);
        jobRunner.setExecuteADResultResponseRecorder(adResultResponseRecorder);

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
                previewAnomalyDetectorAction,
                deleteAnomalyResultsAction,
                searchTopAnomalyResultAction,
                validateAnomalyDetectorAction
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
        NumericSetting.getInstance().init(clusterService);
        this.client = client;
        this.threadPool = threadPool;
        Settings settings = environment.settings();
        Throttler throttler = new Throttler(getClock());
        this.clientUtil = new ClientUtil(settings, client, throttler, threadPool);
        this.indexUtils = new IndexUtils(client, clientUtil, clusterService, indexNameExpressionResolver);
        this.nodeFilter = new DiscoveryNodeFilterer(clusterService);
        this.anomalyDetectionIndices = new AnomalyDetectionIndices(
            client,
            clusterService,
            threadPool,
            settings,
            nodeFilter,
            AnomalyDetectorSettings.MAX_UPDATE_RETRY_TIMES
        );
        this.clusterService = clusterService;

        SingleFeatureLinearUniformInterpolator singleFeatureLinearUniformInterpolator =
            new IntegerSensitiveSingleFeatureLinearUniformInterpolator();
        Interpolator interpolator = new LinearUniformInterpolator(singleFeatureLinearUniformInterpolator);
        SearchFeatureDao searchFeatureDao = new SearchFeatureDao(
            client,
            xContentRegistry,
            interpolator,
            clientUtil,
            settings,
            clusterService,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE
        );

        JvmService jvmService = new JvmService(environment.settings());
        RandomCutForestMapper mapper = new RandomCutForestMapper();
        mapper.setSaveExecutorContextEnabled(true);
        mapper.setSaveTreeStateEnabled(true);
        mapper.setPartialTreeStateEnabled(true);
        V1JsonToV3StateConverter converter = new V1JsonToV3StateConverter();

        double modelMaxSizePercent = AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.get(settings);

        ADCircuitBreakerService adCircuitBreakerService = new ADCircuitBreakerService(jvmService).init();

        MemoryTracker memoryTracker = new MemoryTracker(
            jvmService,
            modelMaxSizePercent,
            AnomalyDetectorSettings.DESIRED_MODEL_SIZE_PERCENTAGE,
            clusterService,
            adCircuitBreakerService
        );

        stateManager = new NodeStateManager(
            client,
            xContentRegistry,
            settings,
            clientUtil,
            getClock(),
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            clusterService
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

        long heapSizeBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();

        serializeRCFBufferPool = AccessController.doPrivileged(new PrivilegedAction<GenericObjectPool<LinkedBuffer>>() {
            @Override
            public GenericObjectPool<LinkedBuffer> run() {
                return new GenericObjectPool<>(new BasePooledObjectFactory<LinkedBuffer>() {
                    @Override
                    public LinkedBuffer create() throws Exception {
                        return LinkedBuffer.allocate(AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES);
                    }

                    @Override
                    public PooledObject<LinkedBuffer> wrap(LinkedBuffer obj) {
                        return new DefaultPooledObject<>(obj);
                    }
                });
            }
        });
        serializeRCFBufferPool.setMaxTotal(AnomalyDetectorSettings.MAX_TOTAL_RCF_SERIALIZATION_BUFFERS);
        serializeRCFBufferPool.setMaxIdle(AnomalyDetectorSettings.MAX_TOTAL_RCF_SERIALIZATION_BUFFERS);
        serializeRCFBufferPool.setMinIdle(0);
        serializeRCFBufferPool.setBlockWhenExhausted(false);
        serializeRCFBufferPool.setTimeBetweenEvictionRuns(AnomalyDetectorSettings.HOURLY_MAINTENANCE);

        CheckpointDao checkpoint = new CheckpointDao(
            client,
            clientUtil,
            CommonName.CHECKPOINT_INDEX_NAME,
            gson,
            mapper,
            converter,
            new ThresholdedRandomCutForestMapper(),
            AccessController
                .doPrivileged(
                    (PrivilegedAction<Schema<ThresholdedRandomCutForestState>>) () -> RuntimeSchema
                        .getSchema(ThresholdedRandomCutForestState.class)
                ),
            HybridThresholdingModel.class,
            anomalyDetectionIndices,
            AnomalyDetectorSettings.MAX_CHECKPOINT_BYTES,
            serializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES,
            1 - AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE
        );

        Random random = new Random(42);

        CacheProvider cacheProvider = new CacheProvider();

        CheckPointMaintainRequestAdapter adapter = new CheckPointMaintainRequestAdapter(
            cacheProvider,
            checkpoint,
            CommonName.CHECKPOINT_INDEX_NAME,
            AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ,
            getClock(),
            clusterService,
            settings
        );

        CheckpointWriteWorker checkpointWriteQueue = new CheckpointWriteWorker(
            heapSizeBytes,
            AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            AnomalyDetectorSettings.QUEUE_MAINTENANCE,
            checkpoint,
            CommonName.CHECKPOINT_INDEX_NAME,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            stateManager,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE
        );

        CheckpointMaintainWorker checkpointMaintainQueue = new CheckpointMaintainWorker(
            heapSizeBytes,
            AnomalyDetectorSettings.CHECKPOINT_MAINTAIN_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            checkpointWriteQueue,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            stateManager,
            adapter
        );

        EntityCache cache = new PriorityCache(
            checkpoint,
            AnomalyDetectorSettings.DEDICATED_CACHE_SIZE.get(settings),
            AnomalyDetectorSettings.CHECKPOINT_TTL,
            AnomalyDetectorSettings.MAX_INACTIVE_ENTITIES,
            memoryTracker,
            AnomalyDetectorSettings.NUM_TREES,
            getClock(),
            clusterService,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            threadPool,
            checkpointWriteQueue,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            checkpointMaintainQueue,
            settings,
            AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ
        );

        cacheProvider.set(cache);

        EntityColdStarter entityColdStarter = new EntityColdStarter(
            getClock(),
            threadPool,
            stateManager,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.NUM_TREES,
            AnomalyDetectorSettings.TIME_DECAY,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            interpolator,
            searchFeatureDao,
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            settings,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            AnomalyDetectorSettings.MAX_COLD_START_ROUNDS
        );

        EntityColdStartWorker coldstartQueue = new EntityColdStartWorker(
            heapSizeBytes,
            AnomalyDetectorSettings.ENTITY_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            AnomalyDetectorSettings.QUEUE_MAINTENANCE,
            entityColdStarter,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            stateManager,
            cacheProvider
        );

        ModelManager modelManager = new ModelManager(
            checkpoint,
            getClock(),
            AnomalyDetectorSettings.NUM_TREES,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.TIME_DECAY,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.MIN_PREVIEW_SIZE,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ,
            entityColdStarter,
            featureManager,
            memoryTracker,
            settings,
            clusterService
        );

        MultiEntityResultHandler multiEntityResultHandler = new MultiEntityResultHandler(
            client,
            settings,
            threadPool,
            anomalyDetectionIndices,
            this.clientUtil,
            this.indexUtils,
            clusterService
        );

        ResultWriteWorker resultWriteQueue = new ResultWriteWorker(
            heapSizeBytes,
            AnomalyDetectorSettings.RESULT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            AnomalyDetectorSettings.QUEUE_MAINTENANCE,
            multiEntityResultHandler,
            xContentRegistry,
            stateManager,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE
        );

        Map<String, ADStat<?>> stats = ImmutableMap
            .<String, ADStat<?>>builder()
            .put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_HC_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(
                StatNames.MODEL_INFORMATION.getName(),
                new ADStat<>(false, new ModelsOnNodeSupplier(modelManager, cacheProvider, settings, clusterService))
            )
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
            .put(StatNames.MODEL_COUNT.getName(), new ADStat<>(false, new ModelsOnNodeCountSupplier(modelManager, cacheProvider)))
            .put(StatNames.MODEL_CORRUTPION_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .build();

        adStats = new ADStats(stats);

        CheckpointReadWorker checkpointReadQueue = new CheckpointReadWorker(
            heapSizeBytes,
            AnomalyDetectorSettings.ENTITY_FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            AnomalyDetectorSettings.QUEUE_MAINTENANCE,
            modelManager,
            checkpoint,
            coldstartQueue,
            resultWriteQueue,
            stateManager,
            anomalyDetectionIndices,
            cacheProvider,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            adStats
        );

        ColdEntityWorker coldEntityQueue = new ColdEntityWorker(
            heapSizeBytes,
            AnomalyDetectorSettings.ENTITY_FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            checkpointReadQueue,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            stateManager
        );

        ADDataMigrator dataMigrator = new ADDataMigrator(client, clusterService, xContentRegistry, anomalyDetectionIndices);
        HashRing hashRing = new HashRing(nodeFilter, getClock(), settings, client, clusterService, dataMigrator, modelManager);

        anomalyDetectorRunner = new AnomalyDetectorRunner(modelManager, featureManager, AnomalyDetectorSettings.MAX_PREVIEW_RESULTS);

        ADTaskCacheManager adTaskCacheManager = new ADTaskCacheManager(settings, clusterService, memoryTracker);
        adTaskManager = new ADTaskManager(
            settings,
            clusterService,
            client,
            xContentRegistry,
            anomalyDetectionIndices,
            nodeFilter,
            hashRing,
            adTaskCacheManager,
            threadPool
        );
        AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler = new AnomalyResultBulkIndexHandler(
            client,
            settings,
            threadPool,
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
            adCircuitBreakerService,
            featureManager,
            adTaskManager,
            anomalyDetectionIndices,
            adStats,
            anomalyResultBulkIndexHandler,
            adTaskCacheManager,
            searchFeatureDao,
            hashRing,
            modelManager
        );

        ADSearchHandler adSearchHandler = new ADSearchHandler(settings, clusterService, client);

        AnomalyIndexHandler<AnomalyResult> anomalyResultHandler = new AnomalyIndexHandler<AnomalyResult>(
            client,
            settings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            anomalyDetectionIndices,
            this.clientUtil,
            this.indexUtils,
            clusterService
        );

        adResultResponseRecorder = new ExecuteADResultResponseRecorder(
            anomalyDetectionIndices,
            anomalyResultHandler,
            adTaskManager,
            nodeFilter,
            threadPool,
            client
        );

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
                new ADClusterEventListener(clusterService, hashRing),
                adCircuitBreakerService,
                adStats,
                new ClusterManagerEventListener(
                    clusterService,
                    threadPool,
                    client,
                    getClock(),
                    clientUtil,
                    nodeFilter,
                    AnomalyDetectorSettings.CHECKPOINT_TTL,
                    settings
                ),
                nodeFilter,
                multiEntityResultHandler,
                checkpoint,
                cacheProvider,
                adTaskManager,
                adBatchTaskRunner,
                adSearchHandler,
                coldstartQueue,
                resultWriteQueue,
                checkpointReadQueue,
                checkpointWriteQueue,
                coldEntityQueue,
                entityColdStarter,
                adTaskCacheManager,
                adResultResponseRecorder
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
                )
            );
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> enabledSetting = EnabledSetting.getInstance().getSettings();
        List<Setting<?>> numericSetting = NumericSetting.getInstance().getSettings();

        List<Setting<?>> systemSetting = ImmutableList
            .of(
                // HCAD cache
                LegacyOpenDistroAnomalyDetectorSettings.MAX_CACHE_MISS_HANDLING_PER_SECOND,
                AnomalyDetectorSettings.DEDICATED_CACHE_SIZE,
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
                AnomalyDetectorSettings.REQUEST_TIMEOUT,
                AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                AnomalyDetectorSettings.COOLDOWN_MINUTES,
                AnomalyDetectorSettings.BACKOFF_MINUTES,
                AnomalyDetectorSettings.BACKOFF_INITIAL_DELAY,
                AnomalyDetectorSettings.MAX_RETRY_FOR_BACKOFF,
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
                AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
                AnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.INDEX_PRESSURE_SOFT_LIMIT,
                AnomalyDetectorSettings.INDEX_PRESSURE_HARD_LIMIT,
                AnomalyDetectorSettings.MAX_PRIMARY_SHARDS,
                // Security
                LegacyOpenDistroAnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES,
                AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES,
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
                AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.ENTITY_COLD_START_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.RESULT_WRITE_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_BATCH_SIZE,
                AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
                AnomalyDetectorSettings.RESULT_WRITE_QUEUE_BATCH_SIZE,
                AnomalyDetectorSettings.COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.CHECKPOINT_MAINTAIN_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.EXPECTED_COLD_ENTITY_EXECUTION_TIME_IN_MILLISECS,
                AnomalyDetectorSettings.EXPECTED_CHECKPOINT_MAINTAIN_TIME_IN_MILLISECS,
                AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ,
                AnomalyDetectorSettings.CHECKPOINT_TTL,
                // query limit
                LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY,
                LegacyOpenDistroAnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW,
                AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY,
                AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW,
                AnomalyDetectorSettings.MAX_CONCURRENT_PREVIEW,
                AnomalyDetectorSettings.PAGE_SIZE,
                // clean resource
                AnomalyDetectorSettings.DELETE_AD_RESULT_WHEN_DELETE_DETECTOR,
                // stats/profile API
                AnomalyDetectorSettings.MAX_MODEL_SIZE_PER_NODE
            );
        return unmodifiableList(
            Stream
                .of(enabledSetting.stream(), systemSetting.stream(), numericSetting.stream())
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
                new ActionHandler<>(ForwardADTaskAction.INSTANCE, ForwardADTaskTransportAction.class),
                new ActionHandler<>(DeleteAnomalyResultsAction.INSTANCE, DeleteAnomalyResultsTransportAction.class),
                new ActionHandler<>(SearchTopAnomalyResultAction.INSTANCE, SearchTopAnomalyResultTransportAction.class),
                new ActionHandler<>(ValidateAnomalyDetectorAction.INSTANCE, ValidateAnomalyDetectorTransportAction.class)
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

    @Override
    public void close() {
        if (serializeRCFBufferPool != null) {
            try {
                AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                    serializeRCFBufferPool.clear();
                    serializeRCFBufferPool.close();
                    return null;
                });
                serializeRCFBufferPool = null;
            } catch (Exception e) {
                LOG.error("Failed to shut down object Pool", e);
            }
        }
    }
}
