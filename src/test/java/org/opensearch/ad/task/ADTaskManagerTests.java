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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.ad.task;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.opensearch.ad.TestHelpers.randomDetector;
import static org.opensearch.ad.TestHelpers.randomFeature;
import static org.opensearch.ad.TestHelpers.randomUser;
import static org.opensearch.ad.constant.CommonName.ANOMALY_RESULT_INDEX_ALIAS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.DELETE_AD_RESULT_WHEN_DELETE_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_BATCH_TASK_PER_NODE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.cluster.ADDataMigrator;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.DuplicateTaskException;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.transport.AnomalyDetectorJobResponse;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;

public class ADTaskManagerTests extends ADUnitTestCase {

    private Settings settings;
    private Client client;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;
    private DiscoveryNodeFilterer nodeFilter;
    private AnomalyDetectionIndices anomalyDetectionIndices;
    private ADTaskCacheManager adTaskCacheManager;
    private HashRing hashRing;
    private TransportService transportService;
    private ADTaskManager adTaskManager;
    private ThreadPool threadPool;
    private ADDataMigrator dataMigrator;
    private SearchFeatureDao searchFeatureDao;

    private Instant startTime;
    private Instant endTime;
    private ActionListener<AnomalyDetectorJobResponse> listener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        Instant now = Instant.now();
        startTime = now.minus(10, ChronoUnit.DAYS);
        endTime = now.minus(1, ChronoUnit.DAYS);

        settings = Settings
            .builder()
            .put(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.getKey(), 2)
            .put(BATCH_TASK_PIECE_INTERVAL_SECONDS.getKey(), 1)
            .put(REQUEST_TIMEOUT.getKey(), TimeValue.timeValueSeconds(10))
            .build();

        clusterSettings = clusterSetting(
            settings,
            MAX_OLD_AD_TASK_DOCS_PER_DETECTOR,
            BATCH_TASK_PIECE_INTERVAL_SECONDS,
            REQUEST_TIMEOUT,
            DELETE_AD_RESULT_WHEN_DELETE_DETECTOR,
            MAX_BATCH_TASK_PER_NODE,
            MAX_RUNNING_ENTITIES_PER_DETECTOR_FOR_HISTORICAL_ANALYSIS
        );

        clusterService = new ClusterService(settings, clusterSettings, null);

        client = mock(Client.class);
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        anomalyDetectionIndices = mock(AnomalyDetectionIndices.class);
        adTaskCacheManager = mock(ADTaskCacheManager.class);
        hashRing = mock(HashRing.class);
        transportService = mock(TransportService.class);
        threadPool = mock(ThreadPool.class);
        dataMigrator = mock(ADDataMigrator.class);
        searchFeatureDao = mock(SearchFeatureDao.class);
        adTaskManager = new ADTaskManager(
            settings,
            clusterService,
            client,
            NamedXContentRegistry.EMPTY,
            anomalyDetectionIndices,
            nodeFilter,
            hashRing,
            adTaskCacheManager,
            threadPool,
            dataMigrator,
            searchFeatureDao
        );

        listener = spy(new ActionListener<AnomalyDetectorJobResponse>() {
            @Override
            public void onResponse(AnomalyDetectorJobResponse bulkItemResponses) {}

            @Override
            public void onFailure(Exception e) {}
        });
    }

    public void testCreateTaskIndexNotAcknowledged() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(false, false, ANOMALY_RESULT_INDEX_ALIAS));
            return null;
        }).when(anomalyDetectionIndices).initDetectionStateIndex(any());
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));

        adTaskManager
            .startHistoricalAnalysisTask(detector, new DetectionDateRange(startTime, endTime), randomUser(), 1, transportService, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(
            "Create index .opendistro-anomaly-detection-state with mappings not acknowledged",
            exceptionCaptor.getValue().getMessage()
        );
    }

    public void testCreateTaskIndexWithResourceAlreadyExistsException() throws IOException {
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException("index created"));
            return null;
        }).when(anomalyDetectionIndices).initDetectionStateIndex(any());
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));

        adTaskManager
            .startHistoricalAnalysisTask(detector, new DetectionDateRange(startTime, endTime), randomUser(), 1, transportService, listener);
        verify(listener, never()).onFailure(any());
    }

    public void testCreateTaskIndexWithException() throws IOException {
        String error = randomAlphaOfLength(5);
        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException(error));
            return null;
        }).when(anomalyDetectionIndices).initDetectionStateIndex(any());
        AnomalyDetector detector = randomDetector(ImmutableList.of(randomFeature(true)), randomAlphaOfLength(5), 1, randomAlphaOfLength(5));

        adTaskManager
            .startHistoricalAnalysisTask(detector, new DetectionDateRange(startTime, endTime), randomUser(), 1, transportService, listener);
        verify(listener, times(1)).onFailure(exceptionCaptor.capture());
        assertEquals(error, exceptionCaptor.getValue().getMessage());
    }

    public void testDeleteDuplicateTasks() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask();
        adTaskManager.handleADTaskException(adTask, new DuplicateTaskException("test"));
        verify(client, times(1)).delete(any(), any());
    }

    public void testParseEntityForSingleCategoryHC() throws IOException {
        ADTask adTask = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.INIT,
                Instant.now(),
                randomAlphaOfLength(5),
                TestHelpers.randomAnomalyDetectorUsingCategoryFields(randomAlphaOfLength(5), ImmutableList.of(randomAlphaOfLength(5)))
            );
        String entityValue = adTaskManager.convertEntityToString(adTask);
        Entity entity = adTaskManager.parseEntityFromString(entityValue, adTask);
        assertEquals(entity, adTask.getEntity());
    }

    public void testParseEntityForMultiCategoryHC() throws IOException {
        ADTask adTask = TestHelpers
            .randomAdTask(
                randomAlphaOfLength(5),
                ADTaskState.INIT,
                Instant.now(),
                randomAlphaOfLength(5),
                TestHelpers
                    .randomAnomalyDetectorUsingCategoryFields(
                        randomAlphaOfLength(5),
                        ImmutableList.of(randomAlphaOfLength(5), randomAlphaOfLength(5))
                    )
            );
        String entityValue = adTaskManager.convertEntityToString(adTask);
        Entity entity = adTaskManager.parseEntityFromString(entityValue, adTask);
        assertEquals(entity, adTask.getEntity());
    }
}
