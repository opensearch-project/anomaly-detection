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
import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.ad.ml.CheckpointDao.FIELD_MODEL;
import static org.opensearch.ad.ml.CheckpointDao.FIELD_MODELV2;
import static org.opensearch.ad.ml.CheckpointDao.TIMESTAMP;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Clock;
import java.time.Instant;
import java.time.Month;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.tuple.Pair;
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
import org.opensearch.action.ActionRequest;
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
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.client.Client;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import test.org.opensearch.ad.util.JsonDeserializer;
import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestMapper;
import com.amazon.randomcutforest.parkservices.state.ThresholdedRandomCutForestState;
import com.amazon.randomcutforest.serialize.json.v1.V1JsonToV3StateConverter;
import com.amazon.randomcutforest.state.RandomCutForestMapper;
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

    private Schema<ThresholdedRandomCutForestState> trcfSchema;

    // configuration
    private String indexName;

    // test data
    private String modelId;

    private Gson gson;
    private Class<? extends ThresholdingModel> thresholdingModelClass;

    private int maxCheckpointBytes = 1_000_000;
    private GenericObjectPool<LinkedBuffer> serializeRCFBufferPool;
    private RandomCutForestMapper mapper;
    private ThresholdedRandomCutForestMapper trcfMapper;
    private V1JsonToV3StateConverter converter;
    double anomalyRate;

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

        trcfMapper = new ThresholdedRandomCutForestMapper();
        trcfSchema = AccessController
            .doPrivileged(
                (PrivilegedAction<Schema<ThresholdedRandomCutForestState>>) () -> RuntimeSchema
                    .getSchema(ThresholdedRandomCutForestState.class)
            );

        converter = new V1JsonToV3StateConverter();

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

        anomalyRate = 0.005;
        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            mapper,
            converter,
            trcfMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            maxCheckpointBytes,
            serializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate
        );

        when(indexUtil.doesCheckpointIndexExist()).thenReturn(true);

        modelId = "testModelId";
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        serializeRCFBufferPool.close();
    }

    private ThresholdedRandomCutForest createTRCF() {
        int dimensions = 4;
        int numberOfTrees = 1;
        int sampleSize = 256;
        int dataSize = 10 * sampleSize;
        Random random = new Random();
        long seed = random.nextLong();
        double[][] data = MLUtil.generateShingledData(dataSize, dimensions, 2);
        ThresholdedRandomCutForest forest = ThresholdedRandomCutForest
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
            forest.process(point, 0);
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

        checkpointDao.putTRCFCheckpoint(modelId, createTRCF(), listener);

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
        checkpointDao.putTRCFCheckpoint(modelId, createTRCF(), listener);

        verify(clientUtil, never()).asyncRequest(any(), any(), any());
    }

    @SuppressWarnings("unchecked")
    public void test_getModelCheckpoint_returnExpectedToListener() {
        // ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        UpdateResponse updateResponse = new UpdateResponse(
            new ReplicationResponse.ShardInfo(3, 2),
            new ShardId(CommonName.CHECKPOINT_INDEX_NAME, "uuid", 2),
            "1",
            7,
            17,
            2,
            UPDATED
        );
        AtomicReference<GetRequest> getRequest = new AtomicReference<>();
        doAnswer(invocation -> {
            ActionRequest request = invocation.getArgument(0);
            if (request instanceof GetRequest) {
                getRequest.set((GetRequest) request);
                ActionListener<GetResponse> listener = invocation.getArgument(2);
                listener.onResponse(getResponse);
            } else {
                UpdateRequest updateRequest = (UpdateRequest) request;
                when(getResponse.getSource()).thenReturn(updateRequest.doc().sourceAsMap());
                ActionListener<UpdateResponse> listener = invocation.getArgument(2);
                listener.onResponse(updateResponse);
            }
            return null;
        }).when(clientUtil).asyncRequest(any(), any(BiConsumer.class), any(ActionListener.class));
        when(getResponse.isExists()).thenReturn(true);

        ThresholdedRandomCutForest trcf = createTRCF();

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        checkpointDao.putTRCFCheckpoint(modelId, trcf, ActionListener.wrap(response -> { inProgressLatch.countDown(); }, exception -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }));

        ActionListener<Optional<ThresholdedRandomCutForest>> listener = mock(ActionListener.class);
        checkpointDao.getTRCFModel(modelId, listener);

        GetRequest capturedGetRequest = getRequest.get();
        assertEquals(indexName, capturedGetRequest.index());
        assertEquals(modelId, capturedGetRequest.id());
        ArgumentCaptor<Optional<ThresholdedRandomCutForest>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        Optional<ThresholdedRandomCutForest> result = responseCaptor.getValue();
        assertTrue(result.isPresent());
        RandomCutForest deserializedForest = result.get().getForest();
        RandomCutForest serializedForest = trcf.getForest();
        assertEquals(deserializedForest.getDimensions(), serializedForest.getDimensions());
        assertEquals(deserializedForest.getNumberOfTrees(), serializedForest.getNumberOfTrees());
        assertEquals(deserializedForest.getSampleSize(), serializedForest.getSampleSize());
    }

    @SuppressWarnings("unchecked")
    public void test_getModelCheckpoint_Bwc() {
        // ArgumentCaptor<GetRequest> requestCaptor = ArgumentCaptor.forClass(GetRequest.class);
        UpdateResponse updateResponse = new UpdateResponse(
            new ReplicationResponse.ShardInfo(3, 2),
            new ShardId(CommonName.CHECKPOINT_INDEX_NAME, "uuid", 2),
            "1",
            7,
            17,
            2,
            UPDATED
        );
        AtomicReference<GetRequest> getRequest = new AtomicReference<>();
        doAnswer(invocation -> {
            ActionRequest request = invocation.getArgument(0);
            if (request instanceof GetRequest) {
                getRequest.set((GetRequest) request);
                ActionListener<GetResponse> listener = invocation.getArgument(2);
                listener.onResponse(getResponse);
            } else {
                UpdateRequest updateRequest = (UpdateRequest) request;
                when(getResponse.getSource()).thenReturn(updateRequest.doc().sourceAsMap());
                ActionListener<UpdateResponse> listener = invocation.getArgument(2);
                listener.onResponse(updateResponse);
            }
            return null;
        }).when(clientUtil).asyncRequest(any(), any(BiConsumer.class), any(ActionListener.class));
        when(getResponse.isExists()).thenReturn(true);

        ThresholdedRandomCutForest trcf = createTRCF();

        final CountDownLatch inProgressLatch = new CountDownLatch(1);
        checkpointDao.putTRCFCheckpoint(modelId, trcf, ActionListener.wrap(response -> { inProgressLatch.countDown(); }, exception -> {
            assertTrue("Should not reach here ", false);
            inProgressLatch.countDown();
        }));

        ActionListener<Optional<ThresholdedRandomCutForest>> listener = mock(ActionListener.class);
        checkpointDao.getTRCFModel(modelId, listener);

        GetRequest capturedGetRequest = getRequest.get();
        assertEquals(indexName, capturedGetRequest.index());
        assertEquals(modelId, capturedGetRequest.id());
        ArgumentCaptor<Optional<ThresholdedRandomCutForest>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        Optional<ThresholdedRandomCutForest> result = responseCaptor.getValue();
        assertTrue(result.isPresent());
        RandomCutForest deserializedForest = result.get().getForest();
        RandomCutForest serializedForest = trcf.getForest();
        assertEquals(deserializedForest.getDimensions(), serializedForest.getDimensions());
        assertEquals(deserializedForest.getNumberOfTrees(), serializedForest.getNumberOfTrees());
        assertEquals(deserializedForest.getSampleSize(), serializedForest.getSampleSize());
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

        ActionListener<Optional<ThresholdedRandomCutForest>> listener = mock(ActionListener.class);
        checkpointDao.getTRCFModel(modelId, listener);

        GetRequest getRequest = requestCaptor.getValue();
        assertEquals(indexName, getRequest.index());
        assertEquals(modelId, getRequest.id());
        // ArgumentCaptor<Exception> responseCaptor = ArgumentCaptor.forClass(Exception.class);
        // verify(listener).onFailure(responseCaptor.capture());
        // Exception exception = responseCaptor.getValue();
        // assertTrue(exception instanceof ResourceNotFoundException);
        ArgumentCaptor<Optional<ThresholdedRandomCutForest>> responseCaptor = ArgumentCaptor.forClass(Optional.class);
        verify(listener).onResponse(responseCaptor.capture());
        assertTrue(!responseCaptor.getValue().isPresent());
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
        source.put(CheckpointDao.FIELD_MODELV2, checkpointDao.toCheckpoint(modelToSave, modelId).get());
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
        logger.info(modelToSave.getTrcf());
        logger.info(model.getTrcf());
        assertEquals(modelToSave.getTrcf().get().getForest().getTotalUpdates(), model.getTrcf().get().getForest().getTotalUpdates());
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
                    failedId[i],
                    new VersionConflictEngineException(shardId, "id", "test")
                )
            );
        }

        for (; i < failed + succeeded; i++) {
            bulkItemResponses[i] = new BulkItemResponse(
                i,
                DocWriteRequest.OpType.UPDATE,
                new UpdateResponse(shardId, "1", 0L, 1L, 1L, DocWriteResponse.Result.CREATED)
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
            converter,
            trcfMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            1, // make the max checkpoint size 1 byte only
            serializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate
        );

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        assertTrue(checkpointDao.toIndexSource(state).isEmpty());
    }

    public void test_to_index_source() throws IOException {
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        Map<String, Object> source = checkpointDao.toIndexSource(state);
        assertTrue(!source.isEmpty());
        for (Object obj : source.values()) {
            // Opensearch cannot recognize Optional
            assertTrue(!(obj instanceof Optional));
        }
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
            converter,
            trcfMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            1, // make the max checkpoint size 1 byte only
            mockSerializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate
        );

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        assertTrue(!checkpointDao.toCheckpoint(state.getModel(), modelId).get().isEmpty());
    }

    public void testMapperFailure() throws IOException {
        ThresholdedRandomCutForestMapper mockMapper = mock(ThresholdedRandomCutForestMapper.class);
        when(mockMapper.toState(any())).thenThrow(RuntimeException.class);

        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            mapper,
            converter,
            mockMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            1, // make the max checkpoint size 1 byte only
            serializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate
        );

        // make sure sample size is not 0 otherwise sample size won't be written to checkpoint
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(1).build());
        String json = checkpointDao.toCheckpoint(state.getModel(), modelId).get();
        assertEquals(null, JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_TRCF));
        assertTrue(null != JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_SAMPLE));
        // assertTrue(null != JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_THRESHOLD));
        // assertNotNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_TRCF));
    }

    public void testEmptySample() throws IOException {
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel(), modelId).get();
        // assertTrue(null != JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_TRCF));
        assertEquals(null, JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_SAMPLE));
        // assertTrue(null != JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_THRESHOLD));
        assertNotNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_TRCF));
    }

    public void testToCheckpointErcfCheckoutFail() throws Exception {
        when(serializeRCFBufferPool.borrowObject()).thenThrow(RuntimeException.class);

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel(), modelId).get();

        assertNotNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_TRCF));
    }

    @SuppressWarnings("unchecked")
    private void setUpMockTrcf() {
        trcfMapper = mock(ThresholdedRandomCutForestMapper.class);
        trcfSchema = mock(Schema.class);
        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            mapper,
            converter,
            trcfMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            maxCheckpointBytes,
            serializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate
        );
    }

    public void testToCheckpointTrcfCheckoutBufferFail() throws Exception {
        setUpMockTrcf();
        when(trcfMapper.toState(any())).thenThrow(RuntimeException.class).thenReturn(null);

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel(), modelId).get();

        assertNotNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_TRCF));
    }

    public void testToCheckpointTrcfFailNewBuffer() throws Exception {
        setUpMockTrcf();
        doReturn(null).when(serializeRCFBufferPool).borrowObject();
        when(trcfMapper.toState(any())).thenThrow(RuntimeException.class);

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel(), modelId).get();

        assertNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_TRCF));
    }

    public void testToCheckpointTrcfCheckoutBufferInvalidateFail() throws Exception {
        setUpMockTrcf();
        when(trcfMapper.toState(any())).thenThrow(RuntimeException.class).thenReturn(null);
        doThrow(RuntimeException.class).when(serializeRCFBufferPool).invalidateObject(any());

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel(), modelId).get();

        assertNotNull(JsonDeserializer.getChildNode(json, CheckpointDao.ENTITY_TRCF));
    }

    public void testFromEntityModelCheckpointWithTrcf() throws Exception {
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        String model = checkpointDao.toCheckpoint(state.getModel(), modelId).get();

        Map<String, Object> entity = new HashMap<>();
        entity.put(FIELD_MODELV2, model);
        entity.put(TIMESTAMP, Instant.now().toString());
        Optional<Entry<EntityModel, Instant>> result = checkpointDao.fromEntityModelCheckpoint(entity, this.modelId);

        assertTrue(result.isPresent());
        Entry<EntityModel, Instant> pair = result.get();
        EntityModel entityModel = pair.getKey();
        assertTrue(entityModel.getTrcf().isPresent());
    }

    public void testFromEntityModelCheckpointTrcfMapperFail() throws Exception {
        setUpMockTrcf();
        when(trcfMapper.toModel(any())).thenThrow(RuntimeException.class);
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        String model = checkpointDao.toCheckpoint(state.getModel(), modelId).get();

        Map<String, Object> entity = new HashMap<>();
        entity.put(FIELD_MODELV2, model);
        entity.put(TIMESTAMP, Instant.now().toString());
        Optional<Entry<EntityModel, Instant>> result = checkpointDao.fromEntityModelCheckpoint(entity, this.modelId);

        assertTrue(result.isPresent());
        Entry<EntityModel, Instant> pair = result.get();
        EntityModel entityModel = pair.getKey();
        assertFalse(entityModel.getTrcf().isPresent());
    }

    private Pair<Map<String, Object>, Instant> setUp1_0Model(String checkpointFileName) throws FileNotFoundException,
        IOException,
        URISyntaxException {
        String model = null;
        try (
            FileReader v1CheckpointFile = new FileReader(
                new File(getClass().getResource(checkpointFileName).toURI()),
                Charset.defaultCharset()
            );
            BufferedReader rr = new BufferedReader(v1CheckpointFile)
        ) {
            model = rr.readLine();
        }

        Instant now = Instant.now();
        Map<String, Object> entity = new HashMap<>();
        entity.put(FIELD_MODEL, model);
        entity.put(TIMESTAMP, now.toString());
        return Pair.of(entity, now);
    }

    public void testFromEntityModelCheckpointBWC() throws FileNotFoundException, IOException, URISyntaxException {
        Pair<Map<String, Object>, Instant> modelPair = setUp1_0Model("checkpoint_2.json");
        Instant now = modelPair.getRight();

        Optional<Entry<EntityModel, Instant>> result = checkpointDao.fromEntityModelCheckpoint(modelPair.getLeft(), this.modelId);
        assertTrue(result.isPresent());
        Entry<EntityModel, Instant> pair = result.get();
        assertEquals(now, pair.getValue());

        EntityModel entityModel = pair.getKey();

        Queue<double[]> samples = entityModel.getSamples();
        assertEquals(6, samples.size());
        double[] firstSample = samples.peek();
        assertEquals(1, firstSample.length);
        assertEquals(0.6832234717598454, firstSample[0], 1e-10);

        ThresholdedRandomCutForest trcf = entityModel.getTrcf().get();
        RandomCutForest forest = trcf.getForest();
        assertEquals(1, forest.getDimensions());
        assertEquals(10, forest.getNumberOfTrees());
        assertEquals(256, forest.getSampleSize());
        // there are at least 10 scores in the checkpoint
        assertTrue(trcf.getThresholder().getCount() > 10);

        Random random = new Random(0);
        for (int i = 0; i < 100; i++) {
            double[] point = getPoint(forest.getDimensions(), random);
            double score = trcf.process(point, 0).getRCFScore();
            assertTrue(score > 0);
            forest.update(point);
        }
    }

    public void testFromEntityModelCheckpointModelTooLarge() throws FileNotFoundException, IOException, URISyntaxException {
        Pair<Map<String, Object>, Instant> modelPair = setUp1_0Model("checkpoint_2.json");
        checkpointDao = new CheckpointDao(
            client,
            clientUtil,
            indexName,
            gson,
            mapper,
            converter,
            trcfMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            100_000, // checkpoint_2.json is of 224603 bytes.
            serializeRCFBufferPool,
            AnomalyDetectorSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate
        );
        Optional<Entry<EntityModel, Instant>> result = checkpointDao.fromEntityModelCheckpoint(modelPair.getLeft(), this.modelId);
        // checkpoint is only configured to take in 1 MB checkpoint at most. But the checkpoint here is of 1408047 bytes.
        assertTrue(!result.isPresent());
    }

    // test no model is present in checkpoint
    public void testFromEntityModelCheckpointEmptyModel() throws FileNotFoundException, IOException, URISyntaxException {
        Map<String, Object> entity = new HashMap<>();
        entity.put(TIMESTAMP, Instant.now().toString());

        Optional<Entry<EntityModel, Instant>> result = checkpointDao.fromEntityModelCheckpoint(entity, this.modelId);
        assertTrue(!result.isPresent());
    }

    public void testFromEntityModelCheckpointEmptySamples() throws FileNotFoundException, IOException, URISyntaxException {
        Pair<Map<String, Object>, Instant> modelPair = setUp1_0Model("checkpoint_1.json");
        Optional<Entry<EntityModel, Instant>> result = checkpointDao.fromEntityModelCheckpoint(modelPair.getLeft(), this.modelId);
        assertTrue(result.isPresent());
        Queue<double[]> samples = result.get().getKey().getSamples();
        assertEquals(0, samples.size());
    }

    public void testFromEntityModelCheckpointNoRCF() throws FileNotFoundException, IOException, URISyntaxException {
        Pair<Map<String, Object>, Instant> modelPair = setUp1_0Model("checkpoint_3.json");
        Optional<Entry<EntityModel, Instant>> result = checkpointDao.fromEntityModelCheckpoint(modelPair.getLeft(), this.modelId);
        assertTrue(result.isPresent());
        assertTrue(!result.get().getKey().getTrcf().isPresent());
    }

    public void testFromEntityModelCheckpointNoThreshold() throws FileNotFoundException, IOException, URISyntaxException {
        Pair<Map<String, Object>, Instant> modelPair = setUp1_0Model("checkpoint_4.json");
        Optional<Entry<EntityModel, Instant>> result = checkpointDao.fromEntityModelCheckpoint(modelPair.getLeft(), this.modelId);
        assertTrue(result.isPresent());

        ThresholdedRandomCutForest trcf = result.get().getKey().getTrcf().get();
        RandomCutForest forest = trcf.getForest();
        assertEquals(1, forest.getDimensions());
        assertEquals(10, forest.getNumberOfTrees());
        assertEquals(256, forest.getSampleSize());
        // there are no scores in the checkpoint
        assertEquals(0, trcf.getThresholder().getCount());
    }

    public void testFromEntityModelCheckpointWithEntity() throws Exception {
        ModelState<EntityModel> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).entityAttributes(true).build());
        Map<String, Object> content = checkpointDao.toIndexSource(state);
        // Opensearch will convert from java.time.ZonedDateTime to String. Here I am converting to simulate that
        content.put(TIMESTAMP, "2021-09-23T05:00:37.93195Z");

        Optional<Entry<EntityModel, Instant>> result = checkpointDao.fromEntityModelCheckpoint(content, this.modelId);

        assertTrue(result.isPresent());
        Entry<EntityModel, Instant> pair = result.get();
        EntityModel entityModel = pair.getKey();
        assertTrue(entityModel.getEntity().isPresent());
        assertEquals(state.getModel().getEntity().get(), entityModel.getEntity().get());
    }

    private double[] getPoint(int dimensions, Random random) {
        double[] point = new double[dimensions];
        for (int i = 0; i < point.length; i++) {
            point[i] = random.nextDouble();
        }
        return point;
    }

    public void testDeserializeRCFModelPreINIT() throws Exception {
        // Model in file 1_3_0_rcf_model_pre_init.json not passed initialization yet
        String filePath = getClass().getResource("1_3_0_rcf_model_pre_init.json").getPath();
        String json = Files.readString(Paths.get(filePath), Charset.defaultCharset());
        Map map = gson.fromJson(json, Map.class);
        String model = (String) ((Map) ((Map) ((ArrayList) ((Map) map.get("hits")).get("hits")).get(0)).get("_source")).get("modelV2");
        ThresholdedRandomCutForest forest = checkpointDao.toTrcf(model);
        assertEquals(256, forest.getForest().getSampleSize());
        assertEquals(8, forest.getForest().getShingleSize());
        assertEquals(30, forest.getForest().getNumberOfTrees());
    }

    public void testDeserializeRCFModelPostINIT() throws Exception {
        // Model in file rc1_model_single_running is from RCF-3.0-rc1
        String filePath = getClass().getResource("rc1_model_single_running.json").getPath();
        String json = Files.readString(Paths.get(filePath), Charset.defaultCharset());
        Map map = gson.fromJson(json, Map.class);
        String model = (String) ((Map) ((Map) ((ArrayList) ((Map) map.get("hits")).get("hits")).get(0)).get("_source")).get("modelV2");
        ThresholdedRandomCutForest forest = checkpointDao.toTrcf(model);
        assertEquals(256, forest.getForest().getSampleSize());
        assertEquals(8, forest.getForest().getShingleSize());
        assertEquals(30, forest.getForest().getNumberOfTrees());
    }

    public void testDeserializeTRCFModel() throws Exception {
        // Model in file rc1_model_single_running is from RCF-3.0-rc1
        String filePath = getClass().getResource("rc1_trcf_model_direct.json").getPath();
        String json = Files.readString(Paths.get(filePath), Charset.defaultCharset());
        // For the parsing of .toTrcf to work I had to manually change "\u003d" in code back to =.
        // In the byte array it doesn't seem like this is an issue but whenever reading the byte array response into a file it
        // converts "=" to "\u003d" https://groups.google.com/g/google-gson/c/JDHUo9DWyyM?pli=1
        // I also needed to bypass the trcf as it wasn't being read as a key value but instead part of the string
        Map map = gson.fromJson(json, Map.class);
        String model = (String) ((Map) ((Map) ((ArrayList) ((Map) map.get("hits")).get("hits")).get(0)).get("_source")).get("modelV2");
        model = model.split(":")[1].substring(1);
        ThresholdedRandomCutForest forest = checkpointDao.toTrcf(model);

        List<double[]> coldStartData = new ArrayList<>();
        double[] sample1 = new double[] { 57.0 };
        double[] sample2 = new double[] { 1.0 };
        double[] sample3 = new double[] { -19.0 };
        double[] sample4 = new double[] { 13.0 };
        double[] sample5 = new double[] { 41.0 };

        coldStartData.add(sample1);
        coldStartData.add(sample2);
        coldStartData.add(sample3);
        coldStartData.add(sample4);
        coldStartData.add(sample5);

        // This scores were generated with the sample data but on RCF3.0-rc1 and we are comparing them
        // to the scores generated by the imported RCF3.0-rc2.1
        List<Double> scores = new ArrayList<>();
        scores.add(4.814651669367903);
        scores.add(5.566968073093689);
        scores.add(5.919907610660049);
        scores.add(5.770278090352401);
        scores.add(5.319779117320102);

        List<Double> grade = new ArrayList<>();
        grade.add(1.0);
        grade.add(0.0);
        grade.add(0.0);
        grade.add(0.0);
        grade.add(0.0);
        for (int i = 0; i < coldStartData.size(); i++) {
            forest.process(coldStartData.get(i), 0);
            AnomalyDescriptor descriptor = forest.process(coldStartData.get(i), 0);
            assertEquals(descriptor.getRCFScore(), scores.get(i), 1e-9);
            assertEquals(descriptor.getAnomalyGrade(), grade.get(i), 1e-9);
        }
    }
}
