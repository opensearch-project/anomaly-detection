/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ad;

import static java.util.Collections.unmodifiableList;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.opensearch.SpecialPermission;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.caching.PriorityCache;
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
import org.opensearch.ad.ratelimit.CheckpointReadWorker;
import org.opensearch.ad.ratelimit.CheckpointWriteWorker;
import org.opensearch.ad.ratelimit.ColdEntityWorker;
import org.opensearch.ad.ratelimit.EntityColdStartWorker;
import org.opensearch.ad.ratelimit.ResultWriteWorker;
import org.opensearch.ad.rest.RestAnomalyDetectorJobAction;
import org.opensearch.ad.rest.RestDeleteAnomalyDetectorAction;
import org.opensearch.ad.rest.RestDeleteAnomalyResultsAction;
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
import org.opensearch.ad.transport.ADCancelTaskAction;
import org.opensearch.ad.transport.ADCancelTaskTransportAction;
import org.opensearch.ad.transport.ADJobParameterAction;
import org.opensearch.ad.transport.ADJobParameterTransportAction;
import org.opensearch.ad.transport.ADJobRunnerAction;
import org.opensearch.ad.transport.ADJobRunnerTransportAction;
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
import org.opensearch.ad.transport.DeleteAnomalyDetectorAction;
import org.opensearch.ad.transport.DeleteAnomalyDetectorTransportAction;
import org.opensearch.ad.transport.DeleteAnomalyResultsAction;
import org.opensearch.ad.transport.DeleteAnomalyResultsTransportAction;
import org.opensearch.ad.transport.DeleteModelAction;
import org.opensearch.ad.transport.DeleteModelTransportAction;
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
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.sdk.BaseExtension;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.sdk.api.ActionExtension;
import org.opensearch.sdk.rest.ExtensionRestHandler;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ScalingExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

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

public class AnomalyDetectorExtension extends BaseExtension implements ActionExtension {

    private static final String EXTENSION_SETTINGS_PATH = "/ad-extension.yml";

    public static final String LEGACY_AD_BASE = "/_opendistro/_anomaly_detection";
    public static final String LEGACY_OPENDISTRO_AD_BASE_URI = LEGACY_AD_BASE + "/detectors";
    public static final String AD_BASE_DETECTORS_URI = "/detectors";
    public static final String AD_THREAD_POOL_PREFIX = "opensearch.ad.";
    public static final String AD_THREAD_POOL_NAME = "ad-threadpool";
    public static final String AD_BATCH_TASK_THREAD_POOL_NAME = "ad-batch-task-threadpool";
    public static final String AD_JOB_TYPE = "opendistro_anomaly_detector";

    @Deprecated
    private SDKRestClient sdkRestClient;
    private OpenSearchAsyncClient sdkJavaAsyncClient;
    private static Gson gson;
    // package private for testing
    GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;
    private ADStats adStats;
    private DiscoveryNodeFilterer nodeFilter;

    static {
        SpecialPermission.check();
        // gson intialization requires "java.lang.RuntimePermission" "accessDeclaredMembers" to
        // initialize ConstructorConstructor
        AccessController.doPrivileged((PrivilegedAction<Void>) AnomalyDetectorExtension::initGson);
    }

    public AnomalyDetectorExtension() {
        super(EXTENSION_SETTINGS_PATH);
    }

    @Override
    public List<ExtensionRestHandler> getExtensionRestHandlers() {
        return List
            .of(
                new RestIndexAnomalyDetectorAction(extensionsRunner(), restClient()),
                new RestValidateAnomalyDetectorAction(extensionsRunner(), restClient()),
                new RestPreviewAnomalyDetectorAction(extensionsRunner(), restClient()),
                new RestGetAnomalyDetectorAction(extensionsRunner(), restClient()),
                new RestAnomalyDetectorJobAction(extensionsRunner(), restClient()),
                new RestDeleteAnomalyDetectorAction(extensionsRunner(), restClient()),
                new RestDeleteAnomalyResultsAction(extensionsRunner(), restClient()),
                new RestStatsAnomalyDetectorAction(extensionsRunner(), restClient(), adStats, nodeFilter),
                new RestSearchAnomalyDetectorAction(extensionsRunner(), restClient()),
                new RestSearchAnomalyResultAction(extensionsRunner(), restClient()),
                new RestSearchADTasksAction(extensionsRunner(), restClient()),
                new RestSearchAnomalyDetectorInfoAction(extensionsRunner(), restClient()),
                new RestSearchTopAnomalyResultAction(extensionsRunner(), restClient())
            );
    }

    private static Void initGson() {
        gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();
        return null;
    }

    @Override
    public Collection<Object> createComponents(ExtensionsRunner runner) {
        this.sdkRestClient = createRestClient(runner);
        this.sdkJavaAsyncClient = createJavaAsyncClient(runner);
        SDKClusterService sdkClusterService = runner.getSdkClusterService();
        Settings environmentSettings = runner.getEnvironmentSettings();
        SDKNamedXContentRegistry xContentRegistry = runner.getNamedXContentRegistry();
        ThreadPool threadPool = runner.getThreadPool();
        IndexNameExpressionResolver indexNameExpressionResolver = runner.getIndexNameExpressionResolver();

        Throttler throttler = new Throttler(getClock());
        ClientUtil clientUtil = new ClientUtil(environmentSettings, restClient(), throttler);
        IndexUtils indexUtils = new IndexUtils(restClient(), clientUtil, sdkClusterService, indexNameExpressionResolver, javaAsyncClient());
        nodeFilter = new DiscoveryNodeFilterer(sdkClusterService);
        AnomalyDetectionIndices anomalyDetectionIndices = new AnomalyDetectionIndices(
            sdkRestClient,
            sdkJavaAsyncClient,
            sdkClusterService,
            threadPool,
            environmentSettings,
            nodeFilter,
            AnomalyDetectorSettings.MAX_UPDATE_RETRY_TIMES
        );

        SingleFeatureLinearUniformInterpolator singleFeatureLinearUniformInterpolator =
            new IntegerSensitiveSingleFeatureLinearUniformInterpolator();
        Interpolator interpolator = new LinearUniformInterpolator(singleFeatureLinearUniformInterpolator);
        SearchFeatureDao searchFeatureDao = new SearchFeatureDao(
            sdkRestClient,
            xContentRegistry,
            interpolator,
            clientUtil,
            environmentSettings,
            sdkClusterService,
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE
        );

        JvmService jvmService = new JvmService(environmentSettings);
        RandomCutForestMapper mapper = new RandomCutForestMapper();
        mapper.setSaveExecutorContextEnabled(true);
        mapper.setSaveTreeStateEnabled(true);
        mapper.setPartialTreeStateEnabled(true);
        V1JsonToV3StateConverter converter = new V1JsonToV3StateConverter();

        ADCircuitBreakerService adCircuitBreakerService = new ADCircuitBreakerService(jvmService).init();

        MemoryTracker memoryTracker = new MemoryTracker(
            jvmService,
            AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.get(environmentSettings),
            AnomalyDetectorSettings.DESIRED_MODEL_SIZE_PERCENTAGE,
            sdkClusterService,
            adCircuitBreakerService
        );

        NodeStateManager stateManager = new NodeStateManager(
            sdkRestClient,
            xContentRegistry,
            environmentSettings,
            clientUtil,
            getClock(),
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            sdkClusterService
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
            sdkRestClient,
            sdkJavaAsyncClient,
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

        CheckpointWriteWorker checkpointWriteQueue = new CheckpointWriteWorker(
            heapSizeBytes,
            AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            sdkClusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            environmentSettings,
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

        EntityCache cache = new PriorityCache(
            checkpoint,
            AnomalyDetectorSettings.DEDICATED_CACHE_SIZE.get(environmentSettings),
            AnomalyDetectorSettings.CHECKPOINT_TTL,
            AnomalyDetectorSettings.MAX_INACTIVE_ENTITIES,
            memoryTracker,
            AnomalyDetectorSettings.NUM_TREES,
            getClock(),
            sdkClusterService,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            threadPool,
            checkpointWriteQueue,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT
        );

        CacheProvider cacheProvider = new CacheProvider(cache);
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
            environmentSettings,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            AnomalyDetectorSettings.MAX_COLD_START_ROUNDS
        );
        EntityColdStartWorker coldstartQueue = new EntityColdStartWorker(
            heapSizeBytes,
            AnomalyDetectorSettings.ENTITY_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.ENTITY_COLD_START_QUEUE_MAX_HEAP_PERCENT,
            sdkClusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            environmentSettings,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            AnomalyDetectorSettings.QUEUE_MAINTENANCE,
            entityColdStarter,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            stateManager
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
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            entityColdStarter,
            featureManager,
            memoryTracker
        );
        MultiEntityResultHandler multiEntityResultHandler = new MultiEntityResultHandler(
            sdkRestClient,
            environmentSettings,
            threadPool,
            anomalyDetectionIndices,
            clientUtil,
            indexUtils,
            sdkClusterService
        );

        ResultWriteWorker resultWriteQueue = new ResultWriteWorker(
            heapSizeBytes,
            AnomalyDetectorSettings.RESULT_WRITE_QUEUE_SIZE_IN_BYTES,
            AnomalyDetectorSettings.RESULT_WRITE_QUEUE_MAX_HEAP_PERCENT,
            sdkClusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            environmentSettings,
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

        CheckpointReadWorker checkpointReadQueue = new CheckpointReadWorker(
            heapSizeBytes,
            AnomalyDetectorSettings.ENTITY_FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.CHECKPOINT_READ_QUEUE_MAX_HEAP_PERCENT,
            sdkClusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            environmentSettings,
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
            checkpointWriteQueue
        );

        ColdEntityWorker coldEntityQueue = new ColdEntityWorker(
            heapSizeBytes,
            AnomalyDetectorSettings.ENTITY_FEATURE_REQUEST_SIZE_IN_BYTES,
            AnomalyDetectorSettings.COLD_ENTITY_QUEUE_MAX_HEAP_PERCENT,
            sdkClusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            environmentSettings,
            AnomalyDetectorSettings.MAX_QUEUED_TASKS_RATIO,
            getClock(),
            AnomalyDetectorSettings.MEDIUM_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.LOW_SEGMENT_PRUNE_RATIO,
            AnomalyDetectorSettings.MAINTENANCE_FREQ_CONSTANT,
            checkpointReadQueue,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            stateManager
        );

        Map<String, ADStat<?>> stats = ImmutableMap
            .<String, ADStat<?>>builder()
            .put(StatNames.AD_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_HC_EXECUTE_REQUEST_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName(), new ADStat<>(false, new CounterSupplier()))
            .put(
                StatNames.MODEL_INFORMATION.getName(),
                new ADStat<>(false, new ModelsOnNodeSupplier(modelManager, cacheProvider, environmentSettings, sdkClusterService))
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
            .build();

        adStats = new ADStats(stats);

        AnomalyDetectorRunner anomalyDetectorRunner = new AnomalyDetectorRunner(
            modelManager,
            featureManager,
            AnomalyDetectorSettings.MAX_PREVIEW_RESULTS
        );

        ADTaskCacheManager adTaskCacheManager = new ADTaskCacheManager(environmentSettings, sdkClusterService, memoryTracker);
        ADTaskManager adTaskManager = new ADTaskManager(
            environmentSettings,
            sdkClusterService,
            sdkRestClient,
            sdkJavaAsyncClient,
            xContentRegistry,
            anomalyDetectionIndices,
            nodeFilter,
            // null, //hashring MultiNode support https://github.com/opensearch-project/opensearch-sdk-java/issues/200
            adTaskCacheManager,
            threadPool
        );
        AnomalyResultBulkIndexHandler anomalyResultBulkIndexHandler = new AnomalyResultBulkIndexHandler(
            sdkRestClient,
            environmentSettings,
            threadPool,
            clientUtil,
            indexUtils,
            sdkClusterService,
            anomalyDetectionIndices
        );
        ADBatchTaskRunner adBatchTaskRunner = new ADBatchTaskRunner(
            environmentSettings,
            threadPool,
            sdkClusterService,
            sdkRestClient,
            adCircuitBreakerService,
            featureManager,
            adTaskManager,
            anomalyDetectionIndices,
            adStats,
            anomalyResultBulkIndexHandler,
            adTaskCacheManager,
            searchFeatureDao,
            null,
            modelManager
        );

        AnomalyIndexHandler<AnomalyResult> anomalyResultHandler = new AnomalyIndexHandler<AnomalyResult>(
            restClient(),
            environmentSettings,
            threadPool,
            CommonName.ANOMALY_RESULT_INDEX_ALIAS,
            anomalyDetectionIndices,
            clientUtil,
            indexUtils,
            sdkClusterService
        );

        AnomalyDetectorJobRunner jobRunner = AnomalyDetectorJobRunner.getJobRunnerInstance();
        jobRunner.setClient(restClient());
        jobRunner.setThreadPool(threadPool);
        jobRunner.setAnomalyResultHandler(anomalyResultHandler);
        jobRunner.setSettings(environmentSettings);
        jobRunner.setAnomalyDetectionIndices(anomalyDetectionIndices);
        jobRunner.setNodeFilter(nodeFilter);
        jobRunner.setAdTaskManager(adTaskManager);

        ADSearchHandler adSearchHandler = new ADSearchHandler(environmentSettings, sdkClusterService, sdkRestClient);

        return ImmutableList
            .of(
                sdkRestClient,
                sdkJavaAsyncClient,
                anomalyDetectionIndices,
                anomalyDetectorRunner,
                searchFeatureDao,
                singleFeatureLinearUniformInterpolator,
                interpolator,
                gson,
                jvmService,
                featureManager,
                modelManager,
                stateManager,
                adCircuitBreakerService,
                adStats,
                nodeFilter,
                multiEntityResultHandler,
                checkpoint,
                cacheProvider,
                adTaskManager,
                adBatchTaskRunner,
                coldstartQueue,
                resultWriteQueue,
                checkpointReadQueue,
                adSearchHandler,
                checkpointWriteQueue,
                coldEntityQueue,
                entityColdStarter,
                adTaskCacheManager
            );
    }

    @Override
    public List<Setting<?>> getSettings() {
        // Copied from AnomalyDetectorPlugin getSettings
        List<Setting<?>> enabledSetting = EnabledSetting.getInstance().getSettings();
        List<Setting<?>> systemSetting = ImmutableList
            .of(
                AnomalyDetectorSettings.DEDICATED_CACHE_SIZE,
                AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW,
                AnomalyDetectorSettings.PAGE_SIZE,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_MAX_DOCS_PER_SHARD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_ROLLOVER_PERIOD,
                AnomalyDetectorSettings.AD_RESULT_HISTORY_RETENTION_PERIOD,
                AnomalyDetectorSettings.MAX_PRIMARY_SHARDS,
                AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE,
                AnomalyDetectorSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE,
                AnomalyDetectorSettings.BACKOFF_MINUTES,
                AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_MAX_HEAP_PERCENT,
                AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_CONCURRENCY,
                AnomalyDetectorSettings.CHECKPOINT_WRITE_QUEUE_BATCH_SIZE,
                AnomalyDetectorSettings.COOLDOWN_MINUTES,
                AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
                AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS,
                AnomalyDetectorSettings.DELETE_AD_RESULT_WHEN_DELETE_DETECTOR,
                AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE,
                AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS,
                AnomalyDetectorSettings.REQUEST_TIMEOUT,
                AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES,
                AnomalyDetectorSettings.DETECTION_INTERVAL,
                AnomalyDetectorSettings.DETECTION_WINDOW_DELAY,
                AnomalyDetectorSettings.MAX_SINGLE_ENTITY_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.MAX_MULTI_ENTITY_ANOMALY_DETECTORS,
                AnomalyDetectorSettings.MAX_ANOMALY_FEATURES
            );
        return unmodifiableList(
            Stream
                .of(enabledSetting.stream(), systemSetting.stream())
                .reduce(Stream::concat)
                .orElseGet(Stream::empty)
                .collect(Collectors.toList())
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        // Copied from AnomalyDetectorPlugin getNamedXContent
        return ImmutableList
            .of(
                AnomalyDetector.XCONTENT_REGISTRY,
                AnomalyResult.XCONTENT_REGISTRY,
                DetectorInternalState.XCONTENT_REGISTRY,
                AnomalyDetectorJob.XCONTENT_REGISTRY
            );
    }

    private OpenSearchAsyncClient createJavaAsyncClient(ExtensionsRunner runner) {
        @SuppressWarnings("resource")
        OpenSearchAsyncClient client = runner.getSdkClient().initializeJavaAsyncClient();
        return client;
    }

    // TODO: replace or override client object on BaseExtension
    // https://github.com/opensearch-project/opensearch-sdk-java/issues/160
    public OpenSearchAsyncClient javaAsyncClient() {
        return this.sdkJavaAsyncClient;
    }

    @Deprecated
    private SDKRestClient createRestClient(ExtensionsRunner runner) {
        @SuppressWarnings("resource")
        SDKRestClient client = runner.getSdkClient().initializeRestClient();
        return client;
    }

    @Deprecated
    public SDKRestClient restClient() {
        return this.sdkRestClient;
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
                )
            );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays
            .asList(
                new ActionHandler<>(IndexAnomalyDetectorAction.INSTANCE, IndexAnomalyDetectorTransportAction.class),
                new ActionHandler<>(ValidateAnomalyDetectorAction.INSTANCE, ValidateAnomalyDetectorTransportAction.class),
                new ActionHandler<>(PreviewAnomalyDetectorAction.INSTANCE, PreviewAnomalyDetectorTransportAction.class),
                new ActionHandler<>(GetAnomalyDetectorAction.INSTANCE, GetAnomalyDetectorTransportAction.class),
                new ActionHandler<>(ADJobRunnerAction.INSTANCE, ADJobRunnerTransportAction.class),
                new ActionHandler<>(ADJobParameterAction.INSTANCE, ADJobParameterTransportAction.class),
                new ActionHandler<>(AnomalyDetectorJobAction.INSTANCE, AnomalyDetectorJobTransportAction.class),
                new ActionHandler<>(AnomalyResultAction.INSTANCE, AnomalyResultTransportAction.class),
                new ActionHandler<>(RCFResultAction.INSTANCE, RCFResultTransportAction.class),
                new ActionHandler<>(ADResultBulkAction.INSTANCE, ADResultBulkTransportAction.class),
                new ActionHandler<>(EntityResultAction.INSTANCE, EntityResultTransportAction.class),
                new ActionHandler<>(ProfileAction.INSTANCE, ProfileTransportAction.class),
                new ActionHandler<>(DeleteAnomalyDetectorAction.INSTANCE, DeleteAnomalyDetectorTransportAction.class),
                new ActionHandler<>(DeleteAnomalyResultsAction.INSTANCE, DeleteAnomalyResultsTransportAction.class),
                new ActionHandler<>(StatsAnomalyDetectorAction.INSTANCE, StatsAnomalyDetectorTransportAction.class),
                new ActionHandler<>(ADStatsNodesAction.INSTANCE, ADStatsNodesTransportAction.class),
                new ActionHandler<>(SearchAnomalyDetectorAction.INSTANCE, SearchAnomalyDetectorTransportAction.class),
                new ActionHandler<>(SearchAnomalyResultAction.INSTANCE, SearchAnomalyResultTransportAction.class),
                new ActionHandler<>(SearchADTasksAction.INSTANCE, SearchADTasksTransportAction.class),
                new ActionHandler<>(SearchAnomalyDetectorInfoAction.INSTANCE, SearchAnomalyDetectorInfoTransportAction.class),
                new ActionHandler<>(SearchTopAnomalyResultAction.INSTANCE, SearchTopAnomalyResultTransportAction.class),
                new ActionHandler<>(StopDetectorAction.INSTANCE, StopDetectorTransportAction.class),
                new ActionHandler<>(DeleteModelAction.INSTANCE, DeleteModelTransportAction.class),
                new ActionHandler<>(ForwardADTaskAction.INSTANCE, ForwardADTaskTransportAction.class),
                new ActionHandler<>(ADBatchAnomalyResultAction.INSTANCE, ADBatchAnomalyResultTransportAction.class),
                new ActionHandler<>(ADCancelTaskAction.INSTANCE, ADCancelTaskTransportAction.class),
                new ActionHandler<>(RCFPollingAction.INSTANCE, RCFPollingTransportAction.class),
                new ActionHandler<>(ADTaskProfileAction.INSTANCE, ADTaskProfileTransportAction.class)
            );
    }

    public static void main(String[] args) throws IOException {
        // Execute this extension by instantiating it and passing to ExtensionsRunner
        ExtensionsRunner.run(new AnomalyDetectorExtension());
    }
}
