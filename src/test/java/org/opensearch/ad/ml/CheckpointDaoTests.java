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

package org.opensearch.ad.ml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.ml.CheckpointDao.FIELD_MODEL;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.client.Client;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

import com.amazon.randomcutforest.serialize.RandomCutForestSerDe;
import com.google.gson.Gson;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Gson.class })
public class CheckpointDaoTests {
    private static final Logger logger = LogManager.getLogger(CheckpointDaoTests.class);

    private CheckpointDao checkpointDao;

    // dependencies
    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Client client;

    @Mock
    private ClientUtil clientUtil;

    @Mock
    private GetResponse getResponse;

    @Mock
    private RandomCutForestSerDe rcfSerde;

    @Mock
    private Clock clock;

    @Mock
    private AnomalyDetectionIndices indexUtil;

    // configuration
    private String indexName;

    // test data
    private String modelId;
    private String model;
    private Map<String, Object> docSource;

    private Gson gson;
    private Class<? extends ThresholdingModel> thresholdingModelClass;

    private int maxCheckpointBytes = 1_000_000;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        indexName = "testIndexName";

        gson = PowerMockito.mock(Gson.class);

        thresholdingModelClass = HybridThresholdingModel.class;

        when(clock.instant()).thenReturn(Instant.now());

        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            rcfSerde,
            thresholdingModelClass,
            indexUtil,
            maxCheckpointBytes
        );

        when(indexUtil.doesCheckpointIndexExist()).thenReturn(true);

        modelId = "testModelId";
        model = "testModel";
        docSource = new HashMap<>();
        docSource.put(FIELD_MODEL, model);
    }

    private void verifySuccessfulPutModelCheckpointSync() {
        ArgumentCaptor<IndexRequest> indexRequestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        verify(clientUtil)
            .timedRequest(
                indexRequestCaptor.capture(),
                anyObject(),
                Matchers.<BiConsumer<IndexRequest, ActionListener<IndexResponse>>>anyObject()
            );
        IndexRequest indexRequest = indexRequestCaptor.getValue();
        assertEquals(indexName, indexRequest.index());
        assertEquals(modelId, indexRequest.id());
        Set<String> expectedSourceKeys = new HashSet<String>(Arrays.asList(FIELD_MODEL, CheckpointDao.TIMESTAMP));
        assertEquals(expectedSourceKeys, indexRequest.sourceAsMap().keySet());
        assertEquals(model, indexRequest.sourceAsMap().get(FIELD_MODEL));
        assertNotNull(indexRequest.sourceAsMap().get(CheckpointDao.TIMESTAMP));
    }

    @Test
    public void putModelCheckpoint_getIndexRequest() {
        checkpointDao.putModelCheckpoint(modelId, model);

        verifySuccessfulPutModelCheckpointSync();
    }

    @Test
    public void putModelCheckpoint_no_checkpoint_index() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(true, true, CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        checkpointDao.putModelCheckpoint(modelId, model);

        verifySuccessfulPutModelCheckpointSync();
    }

    @Test
    public void putModelCheckpoint_index_race_condition() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException(CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        checkpointDao.putModelCheckpoint(modelId, model);

        verifySuccessfulPutModelCheckpointSync();
    }

    @Test
    public void putModelCheckpoint_unexpected_exception() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException(""));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        checkpointDao.putModelCheckpoint(modelId, model);

        verify(clientUtil, never()).timedRequest(any(), any(), any());
    }

    @Test
    public void getModelCheckpoint_returnExpected() {
        ArgumentCaptor<GetRequest> getRequestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doReturn(Optional.of(getResponse))
            .when(clientUtil)
            .timedRequest(
                getRequestCaptor.capture(),
                anyObject(),
                Matchers.<BiConsumer<GetRequest, ActionListener<GetResponse>>>anyObject()
            );
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(docSource);

        Optional<String> result = checkpointDao.getModelCheckpoint(modelId);

        assertTrue(result.isPresent());
        assertEquals(model, result.get());
        GetRequest getRequest = getRequestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
    }

    @Test
    public void getModelCheckpoint_returnEmpty_whenDocNotFound() {
        doReturn(Optional.of(getResponse))
            .when(clientUtil)
            .timedRequest(anyObject(), anyObject(), Matchers.<BiConsumer<GetRequest, ActionListener<GetResponse>>>anyObject());
        when(getResponse.isExists()).thenReturn(false);

        Optional<String> result = checkpointDao.getModelCheckpoint(modelId);

        assertFalse(result.isPresent());
    }

    @Test
    public void deleteModelCheckpoint_getDeleteRequest() {
        checkpointDao.deleteModelCheckpoint(modelId);

        ArgumentCaptor<DeleteRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        verify(clientUtil)
            .timedRequest(
                deleteRequestCaptor.capture(),
                anyObject(),
                Matchers.<BiConsumer<DeleteRequest, ActionListener<DeleteResponse>>>anyObject()
            );
        DeleteRequest deleteRequest = deleteRequestCaptor.getValue();
        assertEquals(indexName, deleteRequest.index());
        assertEquals(modelId, deleteRequest.id());
    }

    @SuppressWarnings("unchecked")
    private void verifyPutModelCheckpointAsync() {
        ArgumentCaptor<IndexRequest> requestCaptor = ArgumentCaptor.forClass(IndexRequest.class);
        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        checkpointDao.putModelCheckpoint(modelId, model, listener);

        IndexRequest indexRequest = requestCaptor.getValue();
        assertEquals(indexName, indexRequest.index());
        assertEquals(modelId, indexRequest.id());
        Set<String> expectedSourceKeys = new HashSet<String>(Arrays.asList(FIELD_MODEL, CheckpointDao.TIMESTAMP));
        assertEquals(expectedSourceKeys, indexRequest.sourceAsMap().keySet());
        assertEquals(model, indexRequest.sourceAsMap().get(FIELD_MODEL));
        assertNotNull(indexRequest.sourceAsMap().get(CheckpointDao.TIMESTAMP));

        ArgumentCaptor<Void> responseCaptor = ArgumentCaptor.forClass(Void.class);
        verify(listener).onResponse(responseCaptor.capture());
        Void response = responseCaptor.getValue();
        assertEquals(null, response);
    }

    @Test
    public void putModelCheckpoint_callListener_whenCompleted() {
        verifyPutModelCheckpointAsync();
    }

    @Test
    public void putModelCheckpoint_callListener_no_checkpoint_index() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(true, true, CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        verifyPutModelCheckpointAsync();
    }

    @Test
    public void putModelCheckpoint_callListener_race_condition() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException(CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        verifyPutModelCheckpointAsync();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void putModelCheckpoint_callListener_unexpected_exception() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException(""));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        ActionListener<Void> listener = mock(ActionListener.class);
        checkpointDao.putModelCheckpoint(modelId, model, listener);

        verify(clientUtil, never()).asyncRequest(any(), any(), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getModelCheckpoint_returnExpectedToListener() {
        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(getResponse);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(docSource);

        ActionListener<Optional<String>> listener = mock(ActionListener.class);
        checkpointDao.getModelCheckpoint(modelId, listener);

        GetRequest getRequest = requestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
        ArgumentCaptor<Optional<String>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        Optional<String> result = responseCaptor.getValue();
        assertTrue(result.isPresent());
        assertEquals(model, result.get());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getModelCheckpoint_returnEmptyToListener_whenModelNotFound() {
        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(getResponse);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));
        when(getResponse.isExists()).thenReturn(false);

        ActionListener<Optional<String>> listener = mock(ActionListener.class);
        checkpointDao.getModelCheckpoint(modelId, listener);

        GetRequest getRequest = requestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
        ArgumentCaptor<Optional<String>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        Optional<String> result = responseCaptor.getValue();
        assertFalse(result.isPresent());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void deleteModelCheckpoint_callListener_whenCompleted() {
        ArgumentCaptor<DeleteRequest> requestCaptor = ArgumentCaptor.forClass(DeleteRequest.class);
        doAnswer(invocation -> {
            ActionListener<DeleteResponse> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);
        checkpointDao.deleteModelCheckpoint(modelId, listener);

        DeleteRequest deleteRequest = requestCaptor.getValue();
        assertEquals(indexName, deleteRequest.index());
        assertEquals(modelId, deleteRequest.id());

        ArgumentCaptor<Void> responseCaptor = ArgumentCaptor.forClass(Void.class);
        verify(listener).onResponse(responseCaptor.capture());
        Void response = responseCaptor.getValue();
        assertEquals(null, response);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void restore() throws IOException {
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        EntityModel modelToSave = state.getModel();

        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            new Gson(),
            new RandomCutForestSerDe(),
            thresholdingModelClass,
            indexUtil,
            maxCheckpointBytes
        );

        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(true);
        Map<String, Object> source = new HashMap<>();
        source.put(CheckpointDao.DETECTOR_ID, state.getDetectorId());
        source.put(CheckpointDao.FIELD_MODEL, checkpointDao.toCheckpoint(modelToSave));
        source.put(CheckpointDao.TIMESTAMP, "2020-10-11T22:58:23.610392Z");
        when(getResponse.getSource()).thenReturn(source);

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);

            listener.onResponse(getResponse);
            return null;
        }).when(clientUtil).asyncRequest(any(GetRequest.class), any(BiConsumer.class), any(ActionListener.class));

        ActionListener<Optional<Entry<EntityModel, Instant>>> listener = mock(ActionListener.class);
        checkpointDao.restoreModelCheckpoint(modelId, listener);

        ArgumentCaptor<Optional<Entry<EntityModel, Instant>>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        Optional<Entry<EntityModel, Instant>> response = responseCaptor.getValue();
        assertTrue(response.isPresent());
        Entry<EntityModel, Instant> entry = response.get();
        OffsetDateTime utcTime = entry.getValue().atOffset(ZoneOffset.UTC);
        assertEquals(2020, utcTime.getYear());
        assertEquals(Month.OCTOBER, utcTime.getMonth());
        assertEquals(11, utcTime.getDayOfMonth());
        assertEquals(22, utcTime.getHour());
        assertEquals(58, utcTime.getMinute());
        assertEquals(23, utcTime.getSecond());

        EntityModel model = entry.getKey();
        Queue<double[]> queue = model.getSamples();
        Queue<double[]> samplesToSave = modelToSave.getSamples();
        assertEquals(samplesToSave.size(), queue.size());
        assertTrue(Arrays.equals(samplesToSave.peek(), queue.peek()));
        logger.info(modelToSave.getRcf());
        logger.info(model.getRcf());
        assertEquals(modelToSave.getRcf().getTotalUpdates(), model.getRcf().getTotalUpdates());
        assertTrue(model.getThreshold() != null);
    }
}
