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
import org.opensearch.ad.ratelimit.CheckpointWriteWorker;
import org.opensearch.ad.rest.RestAnomalyDetectorJobAction;
import org.opensearch.ad.rest.RestGetAnomalyDetectorAction;
import org.opensearch.ad.rest.RestIndexAnomalyDetectorAction;
import org.opensearch.ad.rest.RestValidateAnomalyDetectorAction;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADJobParameterAction;
import org.opensearch.ad.transport.ADJobParameterTransportAction;
import org.opensearch.ad.transport.ADJobRunnerAction;
import org.opensearch.ad.transport.ADJobRunnerTransportAction;
import org.opensearch.ad.transport.AnomalyDetectorJobAction;
import org.opensearch.ad.transport.AnomalyDetectorJobTransportAction;
import org.opensearch.ad.transport.StopDetectorAction;
import org.opensearch.ad.transport.StopDetectorTransportAction;
import org.opensearch.ad.transport.handler.AnomalyIndexHandler;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.ad.util.IndexUtils;
import org.opensearch.ad.util.Throttler;
import org.opensearch.client.opensearch.OpenSearchAsyncClient;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.sdk.ActionExtension;
import org.opensearch.sdk.BaseExtension;
import org.opensearch.sdk.ExtensionRestHandler;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.threadpool.ThreadPool;

import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV3StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.protostuff.LinkedBuffer;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

public class AnomalyDetectorExtension extends BaseExtension implements ActionExtension {

    private static final String EXTENSION_SETTINGS_PATH = "/ad-extension.yml";

    public static final String AD_BASE_DETECTORS_URI = "/detectors";
    public static final String AD_JOB_TYPE = "opendistro_anomaly_detector";
    public static final String AD_THREAD_POOL_NAME = "ad-threadpool";

    @Deprecated
    private SDKRestClient sdkRestClient;
    private OpenSearchAsyncClient sdkJavaAsyncClient;
    private static Gson gson;
    // package private for testing
    GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;

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
                new RestIndexAnomalyDetectorAction(extensionsRunner(), restClient(), javaAsyncClient()),
                new RestValidateAnomalyDetectorAction(extensionsRunner(), restClient(), javaAsyncClient()),
                new RestGetAnomalyDetectorAction(extensionsRunner(), restClient()),
                new RestAnomalyDetectorJobAction(extensionsRunner(), restClient())
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

        Throttler throttler = new Throttler(getClock());
        ClientUtil clientUtil = new ClientUtil(environmentSettings, restClient(), throttler);
        IndexUtils indexUtils = new IndexUtils(
            restClient(),
            clientUtil,
            sdkClusterService,
            null // indexNameExpressionResolver
        );
        DiscoveryNodeFilterer nodeFilter = new DiscoveryNodeFilterer(sdkClusterService);
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

        ADTaskCacheManager adTaskCacheManager = new ADTaskCacheManager(environmentSettings, sdkClusterService, memoryTracker);

        ADTaskManager adTaskManager = new ADTaskManager(
            environmentSettings,
            sdkClusterService,
            sdkRestClient,
            sdkJavaAsyncClient,
            xContentRegistry,
            anomalyDetectionIndices,
            nodeFilter,
            null, // hashRing
            adTaskCacheManager,
            threadPool
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

        return ImmutableList
            .of(
                sdkRestClient,
                sdkJavaAsyncClient,
                anomalyDetectionIndices,
                searchFeatureDao,
                singleFeatureLinearUniformInterpolator,
                interpolator,
                gson,
                jvmService,
                featureManager,
                modelManager,
                stateManager,
                adCircuitBreakerService,
                nodeFilter,
                checkpoint,
                adTaskManager,
                checkpointWriteQueue,
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
     * createComponents doesn't work for Clock as ES process cannot start
     * complaining it cannot find Clock instances for transport actions constructors.
     * @return a UTC clock
     */
    protected Clock getClock() {
        return Clock.systemUTC();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays
            .asList(
                new ActionHandler<>(ADJobRunnerAction.INSTANCE, ADJobRunnerTransportAction.class),
                new ActionHandler<>(ADJobParameterAction.INSTANCE, ADJobParameterTransportAction.class),
                new ActionHandler<>(AnomalyDetectorJobAction.INSTANCE, AnomalyDetectorJobTransportAction.class),
                new ActionHandler<>(StopDetectorAction.INSTANCE, StopDetectorTransportAction.class)
                // TODO : Register DeleteModelTransportAction here for stop detector
            );
    }

    public static void main(String[] args) throws IOException {
        // Execute this extension by instantiating it and passing to ExtensionsRunner
        ExtensionsRunner.run(new AnomalyDetectorExtension());
    }
}
