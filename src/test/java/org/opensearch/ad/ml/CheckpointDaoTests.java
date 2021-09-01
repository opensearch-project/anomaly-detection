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

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.ad.ml.CheckpointDao.FIELD_MODELV2;
import static org.opensearch.ad.ml.CheckpointDao.TIMESTAMP;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
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
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetAction;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.client.Client;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.test.OpenSearchTestCase;

import test.org.opensearch.ad.util.JsonDeserializer;
import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

import com.amazon.randomcutforest.ERCF.ERCFMapper;
import com.amazon.randomcutforest.ERCF.ERCFState;
import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV2StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
import com.amazon.randomcutforest.state.RandomCutForestState;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import io.protostuff.LinkedBuffer;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

public class CheckpointDaoTests extends OpenSearchTestCase {
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
    private Clock clock;

    @Mock
    private AnomalyDetectionIndices indexUtil;

    @Mock
    private ERCFMapper ercfMapper;

    @Mock
    private Schema<ERCFState> ercfSchema;

    // configuration
    private String indexName;

    // test data
    private String modelId;
    private String model;
    private Map<String, Object> docSource;

    private Gson gson;
    private Class<? extends ThresholdingModel> thresholdingModelClass;

    private int maxCheckpointBytes = 1_000_000;
    private GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;
    private RandomCutForestMapper mapper;
    private Schema<RandomCutForestState> schema;
    private V1JsonToV2StateConverter converter;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        indexName = "testIndexName";

        // gson = PowerMockito.mock(Gson.class);
        gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();

        thresholdingModelClass = HybridThresholdingModel.class;

        when(clock.instant()).thenReturn(Instant.now());

        mapper = new RandomCutForestMapper();
        mapper.setSaveExecutorContextEnabled(true);
        schema = AccessController
            .doPrivileged((PrivilegedAction<Schema<RandomCutForestState>>) () -> RuntimeSchema.getSchema(RandomCutForestState.class));
        converter = new V1JsonToV2StateConverter();

        long heapSizeBytes = JvmInfo.jvmInfo().getMem().getHeapMax().getBytes();

        serializeRCFBufferPool = spy(AccessController.doPrivileged(new PrivilegedAction<GenericObjectPool<LinkedBuffer>>() {
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
        }));
        serializeRCFBufferPool.setMaxTotal(AnomalyDetectorSettings.MAX_TOTAL_RCF_SERIALIZATION_BUFFERS);
        serializeRCFBufferPool.setMaxIdle(AnomalyDetectorSettings.MAX_TOTAL_RCF_SERIALIZATION_BUFFERS);
        serializeRCFBufferPool.setMinIdle(0);
        serializeRCFBufferPool.setBlockWhenExhausted(false);
        serializeRCFBufferPool.setTimeBetweenEvictionRuns(AnomalyDetectorSettings.HOURLY_MAINTENANCE);

        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            mapper,
            schema,
            converter,
            ercfMapper,
            ercfSchema,
            thresholdingModelClass,
            indexUtil,
            maxCheckpointBytes,
            serializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES
        );

        when(indexUtil.doesCheckpointIndexExist()).thenReturn(true);

        modelId = "testModelId";
        model = "testModel";
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        serializeRCFBufferPool.close();
    }

    private RandomCutForest createRCF() {
        int dimensions = 4;
        int numberOfTrees = 1;
        int sampleSize = 256;
        int dataSize = 10 * sampleSize;
        Random random = new Random();
        long seed = random.nextLong();
        double[][] data = MLUtil.generateShingledData(dataSize, dimensions, 2);
        RandomCutForest forest = RandomCutForest
            .builder()
            .compact(true)
            .dimensions(dimensions)
            .numberOfTrees(numberOfTrees)
            .sampleSize(sampleSize)
            .precision(Precision.FLOAT_32)
            .randomSeed(seed)
            .boundingBoxCacheFraction(0)
            .build();
        for (double[] point : data) {
            forest.update(point);
        }
        return forest;
    }

    @SuppressWarnings("unchecked")
    private void verifyPutModelCheckpointAsync() {
        ArgumentCaptor<UpdateRequest> requestCaptor = ArgumentCaptor.forClass(UpdateRequest.class);
        doAnswer(invocation -> {
            ActionListener<UpdateResponse> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));

        ActionListener<Void> listener = mock(ActionListener.class);

        checkpointDao.putRCFCheckpoint(modelId, createRCF(), listener);

        UpdateRequest updateRequest = requestCaptor.getValue();
        assertEquals(indexName, updateRequest.index());
        assertEquals(modelId, updateRequest.id());
        IndexRequest indexRequest = updateRequest.doc();
        Set<String> expectedSourceKeys = new HashSet<String>(Arrays.asList(FIELD_MODELV2, CheckpointDao.TIMESTAMP));
        assertEquals(expectedSourceKeys, indexRequest.sourceAsMap().keySet());
        assertTrue(!((String) (indexRequest.sourceAsMap().get(FIELD_MODELV2))).isEmpty());
        assertNotNull(indexRequest.sourceAsMap().get(CheckpointDao.TIMESTAMP));

        ArgumentCaptor<Void> responseCaptor = ArgumentCaptor.forClass(Void.class);
        verify(listener).onResponse(responseCaptor.capture());
        Void response = responseCaptor.getValue();
        assertEquals(null, response);
    }

    public void test_putModelCheckpoint_callListener_whenCompleted() {
        verifyPutModelCheckpointAsync();
    }

    public void test_putModelCheckpoint_callListener_no_checkpoint_index() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(true, true, CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        verifyPutModelCheckpointAsync();
    }

    public void test_putModelCheckpoint_callListener_race_condition() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException(CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        verifyPutModelCheckpointAsync();
    }

    @SuppressWarnings("unchecked")
    public void test_putModelCheckpoint_callListener_unexpected_exception() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException(""));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        ActionListener<Void> listener = mock(ActionListener.class);
        checkpointDao.putRCFCheckpoint(modelId, createRCF(), listener);

        verify(clientUtil, never()).asyncRequest(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void test_getModelCheckpoint_returnExpectedToListener() {
        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(getResponse);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));
        when(getResponse.isExists()).thenReturn(true);

        RandomCutForest rcf = createRCF();
        docSource = new HashMap<>();
        docSource.put(FIELD_MODELV2, checkpointDao.rcfModelToCheckpoint(rcf, null));

        when(getResponse.getSource()).thenReturn(docSource);

        ActionListener<Optional<RandomCutForest>> listener = mock(ActionListener.class);
        checkpointDao.getRCFModel(modelId, listener);

        GetRequest getRequest = requestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
        ArgumentCaptor<Optional<RandomCutForest>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        Optional<RandomCutForest> result = responseCaptor.getValue();
        assertTrue(result.isPresent());
        RandomCutForest forest = result.get();
        assertEquals(forest.getDimensions(), rcf.getDimensions());
        assertEquals(forest.getNumberOfTrees(), rcf.getNumberOfTrees());
        assertEquals(forest.getSampleSize(), rcf.getSampleSize());
    }

    @SuppressWarnings("unchecked")
    public void test_getModelCheckpoint_returnEmptyToListener_whenModelNotFound() {
        ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(2);
            listener.onResponse(getResponse);
            return null;
        }).when(clientUtil).asyncRequest(requestCaptor.capture(), any(BiConsumer.class), any(ActionListener.class));
        when(getResponse.isExists()).thenReturn(false);

        ActionListener<Optional<RandomCutForest>> listener = mock(ActionListener.class);
        checkpointDao.getRCFModel(modelId, listener);

        GetRequest getRequest = requestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
        ArgumentCaptor<Exception> responseCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(responseCaptor.capture());
        Exception exception = responseCaptor.getValue();
        assertTrue(exception instanceof ResourceNotFoundException);
    }

    @SuppressWarnings("unchecked")
    public void test_deleteModelCheckpoint_callListener_whenCompleted() {
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
    public void test_restore() throws IOException {
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        EntityModel modelToSave = state.getModel();

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
        checkpointDao.deserializeModelCheckpoint(modelId, listener);

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

    public void test_batch_write_no_index() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);
        checkpointDao.batchWrite(new BulkRequest(), null);
        verify(indexUtil, times(1)).initCheckpointIndex(any());

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(true, true, CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());
        checkpointDao.batchWrite(new BulkRequest(), null);
        verify(clientUtil, times(1)).execute(any(), any(), any());
    }

    public void test_batch_write_index_init_no_ack() throws InterruptedException {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(false, false, CommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        final CountDownLatch processingLatch = new CountDownLatch(1);
        checkpointDao.batchWrite(new BulkRequest(), ActionListener.wrap(response -> assertTrue(false), e -> {
            assertTrue(e.getMessage(), e != null);
            processingLatch.countDown();
        }));

        processingLatch.await(100, TimeUnit.SECONDS);
    }

    public void test_batch_write_index_already_exists() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException("blah"));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        checkpointDao.batchWrite(new BulkRequest(), null);
        verify(clientUtil, times(1)).execute(any(), any(), any());
    }

    public void test_batch_write_init_exception() throws InterruptedException {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new RuntimeException("blah"));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        final CountDownLatch processingLatch = new CountDownLatch(1);
        checkpointDao.batchWrite(new BulkRequest(), ActionListener.wrap(response -> assertTrue(false), e -> {
            assertTrue(e.getMessage(), e != null);
            processingLatch.countDown();
        }));

        processingLatch.await(100, TimeUnit.SECONDS);
    }

    private BulkResponse createBulkResponse(int succeeded, int failed, String[] failedId) {
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[succeeded + failed];

        ShardId shardId = new ShardId(CommonName.CHECKPOINT_INDEX_NAME, "", 1);
        int i = 0;
        for (; i < failed; i++) {
            bulkItemResponses[i] = new BulkItemResponse(
                i,
                DocWriteRequest.OpType.UPDATE,
                new BulkItemResponse.Failure(
                    CommonName.CHECKPOINT_INDEX_NAME,
                    CommonName.MAPPING_TYPE,
                    failedId[i],
                    new VersionConflictEngineException(shardId, "id", "test")
                )
            );
        }

        for (; i < failed + succeeded; i++) {
            bulkItemResponses[i] = new BulkItemResponse(
                i,
                DocWriteRequest.OpType.UPDATE,
                new UpdateResponse(shardId, CommonName.MAPPING_TYPE, "1", 0L, 1L, 1L, DocWriteResponse.Result.CREATED)
            );
        }

        return new BulkResponse(bulkItemResponses, 507);
    }

    @SuppressWarnings("unchecked")
    public void test_batch_write_no_init() throws InterruptedException {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(true);

        doAnswer(invocation -> {
            ActionListener<BulkResponse> listener = invocation.getArgument(2);

            listener.onResponse(createBulkResponse(2, 0, null));
            return null;
        }).when(clientUtil).execute(eq(BulkAction.INSTANCE), any(BulkRequest.class), any(ActionListener.class));

        final CountDownLatch processingLatch = new CountDownLatch(1);
        checkpointDao
            .batchWrite(new BulkRequest(), ActionListener.wrap(response -> processingLatch.countDown(), e -> { assertTrue(false); }));

        // we don't expect the waiting time elapsed before the count reached zero
        assertTrue(processingLatch.await(100, TimeUnit.SECONDS));
        verify(clientUtil, times(1)).execute(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void test_batch_read() throws InterruptedException {
        doAnswer(invocation -> {
            ActionListener<MultiGetResponse> listener = invocation.getArgument(2);

            MultiGetItemResponse[] items = new MultiGetItemResponse[1];
            items[0] = new MultiGetItemResponse(
                null,
                new MultiGetResponse.Failure(
                    CommonName.CHECKPOINT_INDEX_NAME,
                    "_doc",
                    "modelId",
                    new IndexNotFoundException(CommonName.CHECKPOINT_INDEX_NAME)
                )
            );
            listener.onResponse(new MultiGetResponse(items));
            return null;
        }).when(clientUtil).execute(eq(MultiGetAction.INSTANCE), any(MultiGetRequest.class), any(ActionListener.class));

        final CountDownLatch processingLatch = new CountDownLatch(1);
        checkpointDao
            .batchRead(new MultiGetRequest(), ActionListener.wrap(response -> processingLatch.countDown(), e -> { assertTrue(false); }));

        // we don't expect the waiting time elapsed before the count reached zero
        assertTrue(processingLatch.await(100, TimeUnit.SECONDS));
        verify(clientUtil, times(1)).execute(any(), any(), any());
    }

    public void test_too_large_checkpoint() throws IOException {
        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            mapper,
            schema,
            converter,
            ercfMapper,
            ercfSchema,
            thresholdingModelClass,
            indexUtil,
            1, // make the max checkpoint size 1 byte only
            serializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES
        );

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        assertTrue(checkpointDao.toIndexSource(state).isEmpty());
    }

    public void test_to_index_source() throws IOException {
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        assertTrue(!checkpointDao.toIndexSource(state).isEmpty());
    }

    @SuppressWarnings("unchecked")
    public void testBorrowFromPoolFailure() throws Exception {
        GenericObjectPool<LinkedBuffer> mockSerializeRCFBufferPool = mock(GenericObjectPool.class);
        when(mockSerializeRCFBufferPool.borrowObject()).thenThrow(NoSuchElementException.class);
        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            mapper,
            schema,
            converter,
            ercfMapper,
            ercfSchema,
            thresholdingModelClass,
            indexUtil,
            1, // make the max checkpoint size 1 byte only
            mockSerializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES
        );

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        assertTrue(!checkpointDao.toCheckpoint(state.getModel()).isEmpty());
    }

    public void testMapperFailure() throws IOException {
        RandomCutForestMapper mockMapper = mock(RandomCutForestMapper.class);
        when(mockMapper.toState(any())).thenThrow(RuntimeException.class);

        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            mockMapper,
            schema,
            converter,
            ercfMapper,
            ercfSchema,
            thresholdingModelClass,
            indexUtil,
            1, // make the max checkpoint size 1 byte only
            serializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES
        );

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        String json = checkpointDao.toCheckpoint(state.getModel());
        assertEquals(null, JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_RCF));
        assertTrue(null != JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_SAMPLE));
        assertTrue(null != JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_THRESHOLD));
        assertNotNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_ERCF));
    }

    public void testEmptySample() throws IOException {
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel());
        assertTrue(null != JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_RCF));
        assertEquals(null, JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_SAMPLE));
        assertTrue(null != JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_THRESHOLD));
        assertNotNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_ERCF));
    }

    public void testToCheckpointErcfCheckoutFail() throws Exception {
        when(serializeRCFBufferPool.borrowObject()).thenThrow(RuntimeException.class);

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel());

        assertNotNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_ERCF));
    }

    public void testToCheckpointErcfCheckoutBufferFail() throws Exception {
        when(ercfMapper.toState(any())).thenThrow(RuntimeException.class).thenReturn(null);

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel());

        assertNotNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_ERCF));
    }

    public void testToCheckpointErcfFailNewBuffer() throws Exception {
        doReturn(null).when(serializeRCFBufferPool).borrowObject();
        when(ercfMapper.toState(any())).thenThrow(RuntimeException.class);

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel());

        assertNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_ERCF));
    }

    public void testToCheckpointErcfCheckoutBufferInvalidateFail() throws Exception {
        when(ercfMapper.toState(any())).thenThrow(RuntimeException.class).thenReturn(null);
        doThrow(RuntimeException.class).when(serializeRCFBufferPool).invalidateObject(any());

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel());

        assertNotNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_ERCF));
    }

    public void testFromEntityModelCheckpointWithErcf() throws Exception {
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            mapper,
            schema,
            converter,
            new ERCFMapper(),
            AccessController.doPrivileged((PrivilegedAction<Schema<ERCFState>>) () -> RuntimeSchema.getSchema(ERCFState.class)),
            thresholdingModelClass,
            indexUtil,
            maxCheckpointBytes,
            serializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES
        );
        String model = checkpointDao.toCheckpoint(state.getModel());

        Map<String, Object> entity = new HashMap<>();
        entity.put(FIELD_MODELV2, model);
        entity.put(TIMESTAMP, Instant.now().toString());
        Optional<Entry<EntityModel, Instant>> result = checkpointDao.fromEntityModelCheckpoint(entity, this.modelId);

        assertTrue(result.isPresent());
        Entry<EntityModel, Instant> pair = result.get();
        EntityModel entityModel = pair.getKey();
        assertTrue(entityModel.getErcf().isPresent());
    }

    public void testFromEntityModelCheckpointErcfMapperFail() throws Exception {
        when(ercfMapper.toModel(any())).thenThrow(RuntimeException.class);
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        String model = checkpointDao.toCheckpoint(state.getModel());

        Map<String, Object> entity = new HashMap<>();
        entity.put(FIELD_MODELV2, model);
        entity.put(TIMESTAMP, Instant.now().toString());
        Optional<Entry<EntityModel, Instant>> result = checkpointDao.fromEntityModelCheckpoint(entity, this.modelId);

        assertTrue(result.isPresent());
        Entry<EntityModel, Instant> pair = result.get();
        EntityModel entityModel = pair.getKey();
        assertFalse(entityModel.getErcf().isPresent());
    }
}
