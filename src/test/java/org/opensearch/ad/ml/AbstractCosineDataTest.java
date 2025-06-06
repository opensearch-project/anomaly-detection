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

package org.opensearch.ad.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.dataprocessor.Imputer;
import org.opensearch.timeseries.dataprocessor.LinearUniformImputer;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.SecurityClientUtil;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.google.common.collect.ImmutableList;

public class AbstractCosineDataTest extends AbstractTimeSeriesTest {
    int numMinSamples;
    String modelId;
    String entityName;
    String detectorId;
    ModelState<ThresholdedRandomCutForest> modelState;
    Clock clock;
    float priority;
    ADColdStart entityColdStarter;
    NodeStateManager stateManager;
    SearchFeatureDao searchFeatureDao;
    Imputer imputer;
    FeatureManager featureManager;
    Settings settings;
    ThreadPool threadPool;
    AtomicBoolean released;
    Runnable releaseSemaphore;
    ActionListener<List<AnomalyResult>> listener;
    CountDownLatch inProgressLatch;
    ADCheckpointWriteWorker checkpointWriteQueue;
    Entity entity;
    AnomalyDetector detector;
    long rcfSeed;
    ADModelManager modelManager;
    ClientUtil clientUtil;
    ClusterService clusterService;
    ClusterSettings clusterSettings;
    DiscoveryNode discoveryNode;
    Set<Setting<?>> nodestateSetting;
    int detectorInterval = 1;
    int shingleSize;
    Client client;
    SecurityClientUtil securityCientUtil;

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        // numMinSamples should be larger than shingleSize; otherwise, we will get rcf exception
        numMinSamples = 3;
        shingleSize = 2;

        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());

        threadPool = mock(ThreadPool.class);
        setUpADThreadPool(threadPool);

        settings = Settings.builder().put(AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ.getKey(), TimeValue.timeValueHours(12)).build();

        client = mock(Client.class);
        clientUtil = mock(ClientUtil.class);

        detector = TestHelpers.AnomalyDetectorBuilder
            .newInstance(1)
            .setDetectionInterval(new IntervalTimeConfiguration(detectorInterval, ChronoUnit.MINUTES))
            .setCategoryFields(ImmutableList.of(randomAlphaOfLength(5)))
            .setShingleSize(shingleSize)
            .build();
        when(clock.millis()).thenReturn(1602401500000L);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);

            listener.onResponse(TestHelpers.createGetResponse(detector, detectorId, ADCommonName.CONFIG_INDEX));

            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(), any(ActionListener.class));

        Set<Setting<?>> nodestateSetting = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        nodestateSetting.add(TimeSeriesSettings.MAX_RETRY_FOR_UNRESPONSIVE_NODE);
        nodestateSetting.add(TimeSeriesSettings.BACKOFF_MINUTES);
        nodestateSetting.add(AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ);
        stateManager = createNodeStateManager(
            client,
            clientUtil,
            threadPool,
            createClusterServiceForNode(threadPool, createDiscoverynode("node1"), nodestateSetting)
        );

        imputer = new LinearUniformImputer(true);

        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections
                .unmodifiableSet(
                    new HashSet<>(
                        Arrays
                            .asList(
                                AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW,
                                AnomalyDetectorSettings.AD_PAGE_SIZE,
                                AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ
                            )
                    )
                )
        );
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        securityCientUtil = new SecurityClientUtil(stateManager, settings);
        searchFeatureDao = spy(
            new SearchFeatureDao(
                client,
                xContentRegistry(), // Important. Without this, ParseUtils cannot parse anything
                securityCientUtil,
                clusterService,
                TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
                clock,
                1,
                1,
                60_000L
            )
        );

        featureManager = new FeatureManager(
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

        checkpointWriteQueue = mock(ADCheckpointWriteWorker.class);

        rcfSeed = 2051L;
        entityColdStarter = new ADColdStart(
            clock,
            threadPool,
            stateManager,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_TREES,
            numMinSamples,
            AnomalyDetectorSettings.MAX_SAMPLE_STRIDE,
            AnomalyDetectorSettings.MAX_TRAIN_SAMPLE,
            searchFeatureDao,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            featureManager,
            // settings,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            checkpointWriteQueue,
            rcfSeed,
            TimeSeriesSettings.MAX_COLD_START_ROUNDS,
            1,
            0
        );

        detectorId = "123";
        modelId = "123_entity_abc";
        entityName = "abc";
        priority = 0.3f;
        entity = Entity.createSingleAttributeEntity("field", entityName);

        released = new AtomicBoolean();

        resetListener();

        modelManager = new ADModelManager(
            mock(ADCheckpointDao.class),
            mock(Clock.class),
            TimeSeriesSettings.NUM_TREES,
            TimeSeriesSettings.NUM_SAMPLES_PER_TREE,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            TimeSeriesSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.MIN_PREVIEW_SIZE,
            TimeSeriesSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ,
            entityColdStarter,
            mock(FeatureManager.class),
            mock(MemoryTracker.class),
            settings,
            clusterService
        );
    }

    protected void resetListener() {
        inProgressLatch = new CountDownLatch(1);
        releaseSemaphore = () -> {
            released.set(true);
            inProgressLatch.countDown();
        };
        listener = ActionListener.wrap(releaseSemaphore);
    }

    protected void checkSemaphoreRelease() throws InterruptedException {
        assertTrue(inProgressLatch.await(30, TimeUnit.SECONDS));
        assertTrue(released.get());
    }

    public int searchInsert(long[] timestamps, long target) {
        int pivot, left = 0, right = timestamps.length - 1;
        while (left <= right) {
            pivot = left + (right - left) / 2;
            if (timestamps[pivot] == target) {
                return pivot;
            }
            if (target < timestamps[pivot]) {
                right = pivot - 1;
            } else {
                left = pivot + 1;
            }
        }
        return left;
    }
}
