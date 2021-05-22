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

package org.opensearch.ad.transport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonMessageAttributes;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.handler.MultiEntityResultHandler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.transport.TransportService;

import test.org.opensearch.ad.util.JsonDeserializer;

public class EntityResultTransportActionTests extends AbstractADTest {
    EntityResultTransportAction entityResult;
    ActionFilters actionFilters;
    TransportService transportService;
    ModelManager manager;
    ADCircuitBreakerService adCircuitBreakerService;
    MultiEntityResultHandler anomalyResultHandler;
    CheckpointDao checkpointDao;
    CacheProvider provider;
    EntityCache entityCache;
    NodeStateManager stateManager;
    Settings settings;
    Clock clock;
    EntityResultRequest request;
    String detectorId;
    long timeoutMs;
    AnomalyDetector detector;
    String cacheMissEntity;
    String cacheHitEntity;
    long start;
    long end;
    Map<String, double[]> entities;
    double[] cacheMissData;
    double[] cacheHitData;
    String tooLongEntity;
    double[] tooLongData;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        actionFilters = mock(ActionFilters.class);
        transportService = mock(TransportService.class);

        adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        anomalyResultHandler = mock(MultiEntityResultHandler.class);
        checkpointDao = mock(CheckpointDao.class);

        detectorId = "123";
        entities = new HashMap<>();

        cacheMissEntity = "0.0.0.1";
        cacheMissData = new double[] { 0.1 };
        cacheHitEntity = "0.0.0.2";
        cacheHitData = new double[] { 0.2 };
        entities.put(cacheMissEntity, cacheMissData);
        entities.put(cacheHitEntity, cacheHitData);
        tooLongEntity = randomAlphaOfLength(AnomalyDetectorSettings.MAX_ENTITY_LENGTH + 1);
        tooLongData = new double[] { 0.3 };
        entities.put(tooLongEntity, tooLongData);
        start = 10L;
        end = 20L;
        request = new EntityResultRequest(detectorId, entities, start, end);

        manager = mock(ModelManager.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            // return entity name
            return args[1];
        }).when(manager).getEntityModelId(anyString(), anyString());
        when(manager.getAnomalyResultForEntity(anyString(), any(), anyString(), any(), anyString()))
            .thenReturn(new ThresholdingResult(1, 1, 1));

        provider = mock(CacheProvider.class);
        entityCache = mock(EntityCache.class);
        when(provider.get()).thenReturn(entityCache);
        when(entityCache.get(eq(cacheMissEntity), any(), any(), anyString())).thenReturn(null);

        ModelState<EntityModel> state = mock(ModelState.class);
        when(entityCache.get(eq(cacheHitEntity), any(), any(), anyString())).thenReturn(state);

        String field = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));
        stateManager = mock(NodeStateManager.class);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(stateManager).getAnomalyDetector(any(String.class), any(ActionListener.class));
        when(stateManager.getLastIndexThrottledTime()).thenReturn(Instant.MIN);

        settings = Settings.builder().put(AnomalyDetectorSettings.COOLDOWN_MINUTES.getKey(), TimeValue.timeValueMinutes(5)).build();
        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());

        AnomalyDetectionIndices indexUtil = mock(AnomalyDetectionIndices.class);
        when(indexUtil.getSchemaVersion(any())).thenReturn(CommonValue.NO_SCHEMA_VERSION);

        ClusterService clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            settings,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.COOLDOWN_MINUTES)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        entityResult = new EntityResultTransportAction(
            actionFilters,
            transportService,
            manager,
            adCircuitBreakerService,
            anomalyResultHandler,
            checkpointDao,
            provider,
            stateManager,
            settings,
            clock,
            indexUtil,
            clusterService
        );

        // timeout in 60 seconds
        timeoutMs = 60000L;
    }

    public void testCircuitBreakerOpen() {
        when(adCircuitBreakerService.isOpen()).thenReturn(true);
        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();

        entityResult.doExecute(null, request, future);

        expectThrows(LimitExceededException.class, () -> future.actionGet(timeoutMs));
    }

    public void testNormal() {
        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();

        entityResult.doExecute(null, request, future);

        future.actionGet(timeoutMs);

        verify(anomalyResultHandler, times(1)).flush(any(), any());
    }

    // test get detector failure
    @SuppressWarnings("unchecked")
    public void testFailtoGetDetector() {
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.empty());
            return null;
        }).when(stateManager).getAnomalyDetector(any(String.class), any(ActionListener.class));

        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();

        entityResult.doExecute(null, request, future);

        expectThrows(EndRunException.class, () -> future.actionGet(timeoutMs));
    }

    // test index pressure high, anomaly grade is 0
    public void testIndexPressureHigh() {
        when(manager.getAnomalyResultForEntity(anyString(), any(), anyString(), any(), anyString()))
            .thenReturn(new ThresholdingResult(0, 1, 1));
        when(stateManager.getLastIndexThrottledTime()).thenReturn(Instant.now());

        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();

        entityResult.doExecute(null, request, future);

        future.actionGet(timeoutMs);

        verify(anomalyResultHandler, never()).flush(any(), any());
    }

    // test rcf score is 0
    public void testNotInitialized() {
        when(manager.getAnomalyResultForEntity(anyString(), any(), anyString(), any(), anyString()))
            .thenReturn(new ThresholdingResult(0, 0, 0));

        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();

        entityResult.doExecute(null, request, future);

        future.actionGet(timeoutMs);

        verify(anomalyResultHandler, never()).flush(any(), any());
    }

    public void testSerialzationRequest() throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        EntityResultRequest readRequest = new EntityResultRequest(streamInput);
        assertThat(detectorId, equalTo(readRequest.getDetectorId()));
        assertThat(start, equalTo(readRequest.getStart()));
        assertThat(end, equalTo(readRequest.getEnd()));
        assertTrue(areEqualWithArrayValue(entities, readRequest.getEntities()));
    }

    public void testValidRequest() {
        ActionRequestValidationException e = request.validate();
        assertThat(e, equalTo(null));
    }

    public void testEmptyId() {
        request = new EntityResultRequest("", entities, start, end);
        ActionRequestValidationException e = request.validate();
        assertThat(e.validationErrors(), hasItem(CommonErrorMessages.AD_ID_MISSING_MSG));
    }

    public void testReverseTime() {
        request = new EntityResultRequest(detectorId, entities, end, start);
        ActionRequestValidationException e = request.validate();
        assertThat(e.validationErrors(), hasItem(startsWith(CommonErrorMessages.INVALID_TIMESTAMP_ERR_MSG)));
    }

    public void testNegativeTime() {
        request = new EntityResultRequest(detectorId, entities, start, -end);
        ActionRequestValidationException e = request.validate();
        assertThat(e.validationErrors(), hasItem(startsWith(CommonErrorMessages.INVALID_TIMESTAMP_ERR_MSG)));
    }

    public void testJsonResponse() throws IOException, JsonPathNotFoundException {
        XContentBuilder builder = jsonBuilder();
        request.toXContent(builder, ToXContent.EMPTY_PARAMS);

        String json = Strings.toString(builder);
        assertEquals(JsonDeserializer.getTextValue(json, CommonMessageAttributes.ID_JSON_KEY), detectorId);
        assertEquals(JsonDeserializer.getLongValue(json, CommonMessageAttributes.START_JSON_KEY), start);
        assertEquals(JsonDeserializer.getLongValue(json, CommonMessageAttributes.END_JSON_KEY), end);
        assertEquals(0, Double.compare(JsonDeserializer.getArrayValue(json, cacheMissEntity).get(0).getAsDouble(), cacheMissData[0]));
        assertEquals(0, Double.compare(JsonDeserializer.getArrayValue(json, cacheHitEntity).get(0).getAsDouble(), cacheHitData[0]));
        assertEquals(0, Double.compare(JsonDeserializer.getArrayValue(json, tooLongEntity).get(0).getAsDouble(), tooLongData[0]));
    }
}
