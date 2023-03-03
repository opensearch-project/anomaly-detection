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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.DetectorInternalState;
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
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.sdk.BaseExtension;
import org.opensearch.sdk.ExtensionRestHandler;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.threadpool.ThreadPool;

import com.google.common.collect.ImmutableList;

public class AnomalyDetectorExtension extends BaseExtension {

    private static final String EXTENSION_SETTINGS_PATH = "/ad-extension.yml";

    public static final String AD_BASE_DETECTORS_URI = "/detectors";

    public AnomalyDetectorExtension() {
        super(EXTENSION_SETTINGS_PATH);
    }

    @Override
    public List<ExtensionRestHandler> getExtensionRestHandlers() {
        return List
            .of(
                new RestIndexAnomalyDetectorAction(extensionsRunner(), this),
                new RestValidateAnomalyDetectorAction(extensionsRunner(), this),
                new RestGetAnomalyDetectorAction(extensionsRunner(), this),
                new RestAnomalyDetectorJobAction(extensionsRunner(), this)
            );
    }

    @Override
    public Collection<Object> createComponents(ExtensionsRunner runner) {

        SDKRestClient sdkRestClient = getRestClient();
        SDKClusterService sdkClusterService = runner.getSdkClusterService();
        Settings environmentSettings = runner.getEnvironmentSettings();
        SDKNamedXContentRegistry xContentRegistry = runner.getNamedXContentRegistry();
        ThreadPool threadPool = runner.getThreadPool();

        JvmService jvmService = new JvmService(environmentSettings);

        ADCircuitBreakerService adCircuitBreakerService = new ADCircuitBreakerService(jvmService).init();

        MemoryTracker memoryTracker = new MemoryTracker(
            jvmService,
            AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE.get(environmentSettings),
            AnomalyDetectorSettings.DESIRED_MODEL_SIZE_PERCENTAGE,
            sdkClusterService,
            adCircuitBreakerService
        );

        ADTaskCacheManager adTaskCacheManager = new ADTaskCacheManager(environmentSettings, sdkClusterService, memoryTracker);

        AnomalyDetectionIndices anomalyDetectionIndices = new AnomalyDetectionIndices(
            sdkRestClient,
            sdkClusterService,
            threadPool,
            environmentSettings,
            null, // nodeFilter
            AnomalyDetectorSettings.MAX_UPDATE_RETRY_TIMES
        );

        ADTaskManager adTaskManager = new ADTaskManager(
            environmentSettings,
            sdkClusterService,
            sdkRestClient,
            xContentRegistry,
            anomalyDetectionIndices,
            null, // nodeFilter
            null, // hashRing
            adTaskCacheManager,
            threadPool
        );

        return ImmutableList
            .of(sdkRestClient, anomalyDetectionIndices, jvmService, adCircuitBreakerService, adTaskManager, adTaskCacheManager);
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

    // TODO: replace or override client object on BaseExtension
    // https://github.com/opensearch-project/opensearch-sdk-java/issues/160
    public OpenSearchClient getClient() {
        @SuppressWarnings("resource")
        OpenSearchClient client = new SDKClient()
            .initializeJavaClient(
                getExtensionSettings().getOpensearchAddress(),
                Integer.parseInt(getExtensionSettings().getOpensearchPort())
            );
        return client;
    }

    @Deprecated
    public SDKRestClient getRestClient() {
        @SuppressWarnings("resource")
        SDKRestClient client = new SDKClient().initializeRestClient(getExtensionSettings());
        return client;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays
            .asList(
                new ActionHandler<>(ADJobRunnerAction.INSTANCE, ADJobRunnerTransportAction.class),
                new ActionHandler<>(ADJobParameterAction.INSTANCE, ADJobParameterTransportAction.class),
                new ActionHandler<>(AnomalyDetectorJobAction.INSTANCE, AnomalyDetectorJobTransportAction.class)
            );
    }

    public static void main(String[] args) throws IOException {
        // Execute this extension by instantiating it and passing to ExtensionsRunner
        ExtensionsRunner.run(new AnomalyDetectorExtension());
    }
}
