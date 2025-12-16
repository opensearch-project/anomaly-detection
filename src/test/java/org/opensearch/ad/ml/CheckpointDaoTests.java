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
import static org.mockito.ArgumentMatchers.eq;
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
import static org.opensearch.ad.ml.ADCheckpointDao.FIELD_MODELV2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
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
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.secure_sm.AccessController;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.transport.client.Client;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
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
import test.org.opensearch.ad.util.JsonDeserializer;
import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class CheckpointDaoTests extends OpenSearchTestCase {
    private static final Logger logger = LogManager.getLogger(CheckpointDaoTests.class);

    private ADCheckpointDao checkpointDao;

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
    private ADIndexManagement indexUtil;

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
    private Instant now;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);

        indexName = "testIndexName";

        gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();

        thresholdingModelClass = HybridThresholdingModel.class;

        now = Instant.now();
        when(clock.instant()).thenReturn(now);

        mapper = new RandomCutForestMapper();
        mapper.setSaveExecutorContextEnabled(true);

        trcfMapper = new ThresholdedRandomCutForestMapper();
        trcfSchema = AccessController.doPrivileged(() -> RuntimeSchema.getSchema(ThresholdedRandomCutForestState.class));

        converter = new V1JsonToV3StateConverter();

        serializeRCFBufferPool = spy(AccessController.doPrivileged(() -> {
            return new GenericObjectPool<>(new BasePooledObjectFactory<LinkedBuffer>() {
                @Override
                public LinkedBuffer create() throws Exception {
                    return LinkedBuffer.allocate(TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES);
                }

                @Override
                public PooledObject<LinkedBuffer> wrap(LinkedBuffer obj) {
                    return new DefaultPooledObject<>(obj);
                }
            });
        }));
        serializeRCFBufferPool.setMaxTotal(TimeSeriesSettings.MAX_TOTAL_RCF_SERIALIZATION_BUFFERS);
        serializeRCFBufferPool.setMaxIdle(TimeSeriesSettings.MAX_TOTAL_RCF_SERIALIZATION_BUFFERS);
        serializeRCFBufferPool.setMinIdle(0);
        serializeRCFBufferPool.setBlockWhenExhausted(false);
        serializeRCFBufferPool.setTimeBetweenEvictionRuns(TimeSeriesSettings.HOURLY_MAINTENANCE);

        anomalyRate = 0.005;
        checkpointDao = new ADCheckpointDao(
            client,
            clientUtil,
            gson,
            mapper,
            converter,
            trcfMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            maxCheckpointBytes,
            serializeRCFBufferPool,
            TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate,
            clock
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
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .internalShinglingEnabled(false)
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
        assertEquals(ADCommonName.CHECKPOINT_INDEX_NAME, updateRequest.index());
        assertEquals(modelId, updateRequest.id());
        IndexRequest indexRequest = updateRequest.doc();
        Set<String> expectedSourceKeys = new HashSet<String>(Arrays.asList(FIELD_MODELV2, CommonName.TIMESTAMP));
        assertEquals(expectedSourceKeys, indexRequest.sourceAsMap().keySet());
        assertTrue(!((String) (indexRequest.sourceAsMap().get(FIELD_MODELV2))).isEmpty());
        assertNotNull(indexRequest.sourceAsMap().get(CommonName.TIMESTAMP));

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
            listener.onResponse(new CreateIndexResponse(true, true, ADCommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());

        verifyPutModelCheckpointAsync();
    }

    public void test_putModelCheckpoint_callListener_race_condition() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onFailure(new ResourceAlreadyExistsException(ADCommonName.CHECKPOINT_INDEX_NAME));
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
            new ShardId(ADCommonName.CHECKPOINT_INDEX_NAME, "uuid", 2),
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
        assertEquals(ADCommonName.CHECKPOINT_INDEX_NAME, capturedGetRequest.index());
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
            new ShardId(ADCommonName.CHECKPOINT_INDEX_NAME, "uuid", 2),
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
        assertEquals(ADCommonName.CHECKPOINT_INDEX_NAME, capturedGetRequest.index());
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
        assertEquals(ADCommonName.CHECKPOINT_INDEX_NAME, getRequest.index());
        assertEquals(modelId, getRequest.id());
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
        assertEquals(ADCommonName.CHECKPOINT_INDEX_NAME, deleteRequest.index());
        assertEquals(modelId, deleteRequest.id());

        ArgumentCaptor<Void> responseCaptor = ArgumentCaptor.forClass(Void.class);
        verify(listener).onResponse(responseCaptor.capture());
        Void response = responseCaptor.getValue();
        assertEquals(null, response);
    }

    // @SuppressWarnings("unchecked")
    public void test_restore() throws IOException {
        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        ThresholdedRandomCutForest modelToSave = state.getModel().get();

        Map<String, Object> source = checkpointDao.toIndexSource(state);
        ModelState<ThresholdedRandomCutForest> modelState = checkpointDao
            .processHCGetResponse(TestHelpers.createGetResponse(source, modelId, "blah"), modelId, "123");
        // ModelState<ThresholdedRandomCutForest> modelState = checkpointDao
        // .processHCGetResponse(TestHelpers.createGetResponse(source, modelId, "blah"), modelId, ADCheckpointDao.DETECTOR_ID);

        Instant utcTime = modelState.getLastCheckpointTime();
        // Oct 11, 2020 22:58:23 UTC
        assertEquals(now, utcTime);// Instant.ofEpochSecond(1602457103)

        ThresholdedRandomCutForest model = modelState.getModel().get();
        assertEquals(modelToSave.getForest().getTotalUpdates(), model.getForest().getTotalUpdates());

        Deque<Sample> queue = modelState.getSamples();
        Deque<Sample> samplesToSave = state.getSamples();
        assertEquals(samplesToSave.size(), queue.size());
        assertEquals(samplesToSave.peek(), queue.peek());
    }

    public void test_batch_write_no_index() {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);
        checkpointDao.batchWrite(new BulkRequest(), null);
        verify(indexUtil, times(1)).initCheckpointIndex(any());

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(true, true, ADCommonName.CHECKPOINT_INDEX_NAME));
            return null;
        }).when(indexUtil).initCheckpointIndex(any());
        checkpointDao.batchWrite(new BulkRequest(), null);
        verify(clientUtil, times(1)).execute(any(), any(), any());
    }

    public void test_batch_write_index_init_no_ack() throws InterruptedException {
        when(indexUtil.doesCheckpointIndexExist()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(0);
            listener.onResponse(new CreateIndexResponse(false, false, ADCommonName.CHECKPOINT_INDEX_NAME));
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

        ShardId shardId = new ShardId(ADCommonName.CHECKPOINT_INDEX_NAME, "", 1);
        int i = 0;
        for (; i < failed; i++) {
            bulkItemResponses[i] = new BulkItemResponse(
                i,
                DocWriteRequest.OpType.UPDATE,
                new BulkItemResponse.Failure(
                    ADCommonName.CHECKPOINT_INDEX_NAME,
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
                    ADCommonName.CHECKPOINT_INDEX_NAME,
                    "modelId",
                    new IndexNotFoundException(ADCommonName.CHECKPOINT_INDEX_NAME)
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
        checkpointDao = new ADCheckpointDao(
            client,
            clientUtil,
            gson,
            mapper,
            converter,
            trcfMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            1, // make the max checkpoint size 1 byte only
            serializeRCFBufferPool,
            TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate,
            clock
        );

        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        assertTrue(checkpointDao.toIndexSource(state).isEmpty());
    }

    public void test_to_index_source() throws IOException {
        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

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
        checkpointDao = new ADCheckpointDao(
            client,
            clientUtil,
            gson,
            mapper,
            converter,
            trcfMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            1, // make the max checkpoint size 1 byte only
            mockSerializeRCFBufferPool,
            TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate,
            clock
        );

        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        assertTrue(!checkpointDao.toCheckpoint(state.getModel().get(), modelId).get().isEmpty());
    }

    public void testMapperFailure() throws IOException {
        ThresholdedRandomCutForestMapper mockMapper = mock(ThresholdedRandomCutForestMapper.class);
        when(mockMapper.toState(any())).thenThrow(RuntimeException.class);

        checkpointDao = new ADCheckpointDao(
            client,
            clientUtil,
            gson,
            mapper,
            converter,
            mockMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            1, // make the max checkpoint size 1 byte only
            serializeRCFBufferPool,
            TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate,
            clock
        );

        // make sure sample size is not 0 otherwise sample size won't be written to checkpoint
        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(1).build());
        String json = checkpointDao.toCheckpoint(state.getModel().get(), modelId).get();
        assertEquals(null, JsonDeserializer.getChildNode(json, ADCheckpointDao.ENTITY_TRCF));
    }

    public void testEmptySample() throws IOException {
        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel().get(), modelId).get();
        assertEquals(null, JsonDeserializer.getChildNode(json, CommonName.ENTITY_SAMPLE));
        assertNotNull(JsonDeserializer.getChildNode(json, ADCheckpointDao.ENTITY_TRCF));
    }

    public void testToCheckpointErcfCheckoutFail() throws Exception {
        when(serializeRCFBufferPool.borrowObject()).thenThrow(RuntimeException.class);

        ModelState<ThresholdedRandomCutForest> state = MLUtil
                .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel().get(), modelId).get();

        assertNotNull(JsonDeserializer.getChildNode(json, ADCheckpointDao.ENTITY_TRCF));
    }

    @SuppressWarnings("unchecked")
    private void setUpMockTrcf() {
        trcfMapper = mock(ThresholdedRandomCutForestMapper.class);
        trcfSchema = mock(Schema.class);
        checkpointDao = new ADCheckpointDao(
            client,
            clientUtil,
            gson,
            mapper,
            converter,
            trcfMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            maxCheckpointBytes,
            serializeRCFBufferPool,
            TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate,
            clock
        );
    }

    public void testToCheckpointTrcfCheckoutBufferFail() throws Exception {
        setUpMockTrcf();
        when(trcfMapper.toState(any())).thenThrow(RuntimeException.class).thenReturn(null);

        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel().get(), modelId).get();

        assertNotNull(JsonDeserializer.getChildNode(json, ADCheckpointDao.ENTITY_TRCF));
    }

    public void testToCheckpointTrcfFailNewBuffer() throws Exception {
        setUpMockTrcf();
        doReturn(null).when(serializeRCFBufferPool).borrowObject();
        when(trcfMapper.toState(any())).thenThrow(RuntimeException.class);

        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel().get(), modelId).get();

        assertNull(JsonDeserializer.getChildNode(json, ADCheckpointDao.ENTITY_TRCF));
    }

    public void testToCheckpointTrcfCheckoutBufferInvalidateFail() throws Exception {
        setUpMockTrcf();
        when(trcfMapper.toState(any())).thenThrow(RuntimeException.class).thenReturn(null);
        doThrow(RuntimeException.class).when(serializeRCFBufferPool).invalidateObject(any());

        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).sampleSize(0).build());
        String json = checkpointDao.toCheckpoint(state.getModel().get(), modelId).get();

        assertNotNull(JsonDeserializer.getChildNode(json, ADCheckpointDao.ENTITY_TRCF));
    }

    public void testFromEntityModelCheckpointWithTrcf() throws Exception {
        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        String model = checkpointDao.toCheckpoint(state.getModel().get(), modelId).get();

        Map<String, Object> source = new HashMap<>();
        source.put(ADCheckpointDao.DETECTOR_ID, state.getConfigId());
        source.put(FIELD_MODELV2, model);
        source.put(CommonName.TIMESTAMP, Instant.now().toString());

        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(source);

        ModelState<ThresholdedRandomCutForest> result = checkpointDao
            .processHCGetResponse(getResponse, this.modelId, ADCheckpointDao.DETECTOR_ID);

        assertTrue(result != null);
        assertTrue(result.getModel().isPresent());
    }

    public void testFromEntityModelCheckpointTrcfMapperFail() throws Exception {
        setUpMockTrcf();
        when(trcfMapper.toModel(any())).thenThrow(RuntimeException.class);
        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        String model = checkpointDao.toCheckpoint(state.getModel().get(), modelId).get();

        Map<String, Object> source = new HashMap<>();
        source.put(FIELD_MODELV2, model);
        source.put(CommonName.TIMESTAMP, Instant.now().toString());

        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(source);

        ModelState<ThresholdedRandomCutForest> result = checkpointDao
            .processHCGetResponse(getResponse, this.modelId, ADCheckpointDao.DETECTOR_ID);

        assertTrue(result != null);
        assertTrue(result.getModel().isEmpty());
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
        entity.put(CommonName.FIELD_MODEL, model);
        entity.put(CommonName.TIMESTAMP, now.toString());
        return Pair.of(entity, now);
    }

    public void testFromEntityModelCheckpointBWC() throws FileNotFoundException, IOException, URISyntaxException {
        Pair<Map<String, Object>, Instant> modelPair = setUp1_0Model("checkpoint_2.json");
        Instant now = modelPair.getRight();

        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(modelPair.getLeft());

        ModelState<ThresholdedRandomCutForest> result = checkpointDao
            .processHCGetResponse(getResponse, this.modelId, ADCheckpointDao.DETECTOR_ID);
        assertTrue(result != null);
        assertEquals(now, result.getLastCheckpointTime());

        Deque<Sample> samples = result.getSamples();

        assertEquals(6, samples.size());
        double[] firstSample = samples.peek().getValueList();
        assertEquals(1, firstSample.length);
        assertEquals(0.6832234717598454, firstSample[0], 1e-10);

        ThresholdedRandomCutForest trcf = result.getModel().get();
        RandomCutForest forest = trcf.getForest();
        assertEquals(1, forest.getDimensions());
        assertEquals(10, forest.getNumberOfTrees());
        assertEquals(256, forest.getSampleSize());

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
        checkpointDao = new ADCheckpointDao(
            client,
            clientUtil,
            gson,
            mapper,
            converter,
            trcfMapper,
            trcfSchema,
            thresholdingModelClass,
            indexUtil,
            100_000, // checkpoint_2.json is of 224603 bytes.
            serializeRCFBufferPool,
            TimeSeriesSettings.SERIALIZATION_BUFFER_BYTES,
            anomalyRate,
            clock
        );
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(modelPair.getLeft());
        ModelState<ThresholdedRandomCutForest> result = checkpointDao
            .processHCGetResponse(getResponse, this.modelId, ADCheckpointDao.DETECTOR_ID);
        // checkpoint is only configured to take in 1 MB checkpoint at most. But the checkpoint here is of 1408047 bytes.
        assertTrue(result == null);
    }

    // test no model is present in checkpoint
    public void testFromEntityModelCheckpointEmptyModel() throws FileNotFoundException, IOException, URISyntaxException {
        Map<String, Object> entity = new HashMap<>();
        entity.put(ADCheckpointDao.DETECTOR_ID, ADCheckpointDao.DETECTOR_ID);
        entity.put(CommonName.TIMESTAMP, Instant.now().toString());
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(entity);

        ModelState<ThresholdedRandomCutForest> result = checkpointDao
            .processHCGetResponse(getResponse, this.modelId, ADCheckpointDao.DETECTOR_ID);
        assertTrue(result == null);
    }

    public void testFromEntityModelCheckpointEmptySamples() throws FileNotFoundException, IOException, URISyntaxException {
        Pair<Map<String, Object>, Instant> modelPair = setUp1_0Model("checkpoint_1.json");
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(modelPair.getLeft());
        ModelState<ThresholdedRandomCutForest> result = checkpointDao
            .processHCGetResponse(getResponse, this.modelId, ADCheckpointDao.DETECTOR_ID);
        assertTrue(result != null);
        Deque<Sample> samples = result.getSamples();
        assertEquals(0, samples.size());
    }

    public void testFromEntityModelCheckpointNoRCF() throws FileNotFoundException, IOException, URISyntaxException {
        Pair<Map<String, Object>, Instant> modelPair = setUp1_0Model("checkpoint_3.json");
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(modelPair.getLeft());
        ModelState<ThresholdedRandomCutForest> result = checkpointDao
            .processHCGetResponse(getResponse, this.modelId, ADCheckpointDao.DETECTOR_ID);
        assertTrue(result != null);
        assertTrue(result.getModel().isEmpty());
    }

    public void testFromEntityModelCheckpointNoThreshold() throws FileNotFoundException, IOException, URISyntaxException {
        Pair<Map<String, Object>, Instant> modelPair = setUp1_0Model("checkpoint_4.json");
        when(getResponse.isExists()).thenReturn(true);
        when(getResponse.getSource()).thenReturn(modelPair.getLeft());
        ModelState<ThresholdedRandomCutForest> result = checkpointDao
            .processHCGetResponse(getResponse, this.modelId, ADCheckpointDao.DETECTOR_ID);
        assertTrue(result != null);

        ThresholdedRandomCutForest trcf = result.getModel().get();
        RandomCutForest forest = trcf.getForest();
        assertEquals(1, forest.getDimensions());
        assertEquals(10, forest.getNumberOfTrees());
        assertEquals(256, forest.getSampleSize());
    }

    public void testFromEntityModelCheckpointWithEntity() throws Exception {
        ModelState<ThresholdedRandomCutForest> state = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).entityAttributes(true).build());
        Map<String, Object> content = checkpointDao.toIndexSource(state);
        // Opensearch will convert from java.time.ZonedDateTime to String. Here I am converting to simulate that
        content.put(CommonName.TIMESTAMP, "2021-09-23T05:00:37.93195Z");

        ModelState<ThresholdedRandomCutForest> result = checkpointDao
            .processHCGetResponse(TestHelpers.createGetResponse(content, modelId, "blah"), this.modelId, ADCheckpointDao.DETECTOR_ID);

        assertTrue(result != null);
        assertTrue(result.getEntity().isPresent());
        assertEquals(state.getEntity().get(), result.getEntity().get());
    }

    private double[] getPoint(int dimensions, Random random) {
        double[] point = new double[dimensions];
        for (int i = 0; i < point.length; i++) {
            point[i] = random.nextDouble();
        }
        return point;
    }

    // The checkpoint used for this test is from a single-stream detector
    public void testDeserializeRCFModelPreINIT() throws Exception {
        // Model in file 1_3_0_rcf_model_pre_init.json not passed initialization yet
        URI uri = ClassLoader.getSystemResource("org/opensearch/ad/ml/1_3_0_rcf_model_pre_init.json").toURI();
        String filePath = Paths.get(uri).toString();
        String json = Files.readString(Paths.get(filePath), Charset.defaultCharset());
        Map map = gson.fromJson(json, Map.class);
        String model = (String) ((Map) ((Map) ((ArrayList) ((Map) map.get("hits")).get("hits")).get(0)).get("_source")).get("modelV2");
        ThresholdedRandomCutForest forest = checkpointDao.toTrcf(model);
        assertEquals(256, forest.getForest().getSampleSize());
        assertEquals(8, forest.getForest().getShingleSize());
        assertEquals(30, forest.getForest().getNumberOfTrees());
    }

    // The checkpoint used for this test is from a single-stream detector
    public void testDeserializeRCFModelPostINIT() throws Exception {
        // Model in file rc1_model_single_running is from RCF-3.0-rc1
        URI uri = ClassLoader.getSystemResource("org/opensearch/ad/ml/rc1_model_single_running.json").toURI();
        String filePath = Paths.get(uri).toString();
        String json = Files.readString(Paths.get(filePath), Charset.defaultCharset());
        Map map = gson.fromJson(json, Map.class);
        String model = (String) ((Map) ((Map) ((ArrayList) ((Map) map.get("hits")).get("hits")).get(0)).get("_source")).get("modelV2");
        ThresholdedRandomCutForest forest = checkpointDao.toTrcf(model);
        assertEquals(256, forest.getForest().getSampleSize());
        assertEquals(8, forest.getForest().getShingleSize());
        assertEquals(30, forest.getForest().getNumberOfTrees());
    }

    // This test is intended to check if given a checkpoint created by RCF-3.0-rc1 ("rc1_trcf_model_direct.json")
    // and given the same sample data will rc1 and current RCF version (this test originally created when 3.0-rc2.1 is in use)
    // will produce the same anomaly scores and grades.
    // The scores and grades in this method were produced from AD running with RCF3.0-rc1 dependency
    // and this test runs with the most recent RCF dependency that is being pulled by this project.
    public void testDeserializeTRCFModel() throws Exception {
        // Model in file rc1_trcf_model_direct is a checkpoint creatd by RCF-3.0-rc1
        URI uri = ClassLoader.getSystemResource("org/opensearch/ad/ml/rc1_trcf_model_direct.json").toURI();
        String filePath = Paths.get(uri).toString();
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

        // This scores were generated with the sample data on RCF4.0. RCF4.0 changed implementation
        // and we are seeing different rcf scores between 4.0 and 3.8. This is verified by switching
        // rcf version between 3.8 and 4.0 while other code in AD unchanged. But we get different scores.
        List<Double> scores = new ArrayList<>();
        scores.add(5.052069275347555);
        scores.add(6.117465704461799);
        scores.add(6.6401649744661055);
        scores.add(6.918514609476484);
        scores.add(6.928318158276434);

        // rcf 3.8 has a number of improvements on thresholder and predictor corrector.
        // We don't expect the results have the same anomaly grade.
        for (int i = 0; i < coldStartData.size(); i++) {
            forest.process(coldStartData.get(i), 0);
            AnomalyDescriptor descriptor = forest.process(coldStartData.get(i), 0);
            assertEquals(scores.get(i), descriptor.getRCFScore(), 1e-9);
        }
    }

    public void testShouldSave() {
        ModelState<ThresholdedRandomCutForest> modelState = new ModelState<ThresholdedRandomCutForest>(
            null,
            modelId,
            "123",
            ModelManager.ModelType.TRCF.getName(),
            clock,
            0.1f,
            Optional.empty(),
            MLUtil.createQueueSamples(1)
        );
        modelState.setLastCheckpointTime(Instant.MIN);
        assertTrue(!checkpointDao.shouldSave(modelState, false, null, clock));
        modelState.setLastCheckpointTime(Instant.ofEpochMilli(Instant.now().toEpochMilli()));
        assertTrue(checkpointDao.shouldSave(modelState, true, Duration.ofHours(6), clock));
        // now + 6 hrs > Instant.now
        modelState.setLastCheckpointTime(Instant.ofEpochMilli(Instant.now().toEpochMilli()));
        assertTrue(!checkpointDao.shouldSave(modelState, false, Duration.ofHours(6), clock));
        // 1658863778000L + 6 hrs < Instant.now
        modelState.setLastCheckpointTime(Instant.ofEpochMilli(1658863778000L));
        assertTrue(checkpointDao.shouldSave(modelState, false, Duration.ofHours(6), clock));
    }

    // This test is intended to check if given a checkpoint created by RCF-3.0-rc3 ("rcf_3_0_rc3_single_stream.json")
    // and given the same sample data will rc3 and current RCF version (this test originally created when 3.0-rc3 is in use)
    // will produce the same anomaly scores.
    // The scores in this method were produced from AD running with RCF3.0-rc3 dependency
    // and this test runs with the most recent RCF dependency that is being pulled by this project.
    public void testDeserialize_rcf3_rc3_single_stream_model() throws Exception {
        // Model in file rcf_3_0_rc3_single_stream.json is a checkpoint creatd by RCF-3.0-rc3
        // I generate the json response file using Postman's Send and Download
        URI uri = ClassLoader.getSystemResource("org/opensearch/ad/ml/rcf_3_0_rc3_single_stream.json").toURI();
        String filePath = Paths.get(uri).toString();
        String json = Files.readString(Paths.get(filePath), Charset.defaultCharset());
        // For the parsing of .toTrcf to work I had to manually change "\u003d" in code back to =.
        // In the byte array it doesn't seem like this is an issue but whenever reading the byte array response into a file it
        // converts "=" to "\u003d" https://groups.google.com/g/google-gson/c/JDHUo9DWyyM?pli=1
        // I also needed to bypass the trcf as it wasn't being read as a key value but instead part of the string
        Map map = gson.fromJson(json, Map.class);
        String model = (String) ((Map) ((Map) ((ArrayList) ((Map) map.get("hits")).get("hits")).get(0)).get("_source")).get("modelV2");
        ThresholdedRandomCutForest forest = checkpointDao.toTrcf(model);

        // single-stream model uses external shingling
        List<double[]> coldStartData = new ArrayList<>();
        double[] sample1 = new double[] { 64, 58, 59, 60, 61, 62, 63, 57.0 };
        double[] sample2 = new double[] { 58, 59, 60, 61, 62, 63, 57.0, 1.0 };
        double[] sample3 = new double[] { 59, 60, 61, 62, 63, 57.0, 1.0, -19.0 };
        double[] sample4 = new double[] { 60, 61, 62, 63, 57.0, 1.0, -19.0, 13.0 };
        double[] sample5 = new double[] { 61, 62, 63, 57.0, 1.0, -19.0, 13.0, 41.0 };

        coldStartData.add(sample1);
        coldStartData.add(sample2);
        coldStartData.add(sample3);
        coldStartData.add(sample4);
        coldStartData.add(sample5);

        // This scores were generated with the sample data on RCF4.0. RCF4.0 changed implementation
        // and we are seeing different rcf scores between 4.0 and 3.8. This is verified by switching
        // rcf version between 3.8 and 4.0 while other code in AD unchanged. But we get different scores.
        List<Double> scores = new ArrayList<>();
        scores.add(3.678754481587072);
        scores.add(3.6809634269790252);
        scores.add(3.683659822587799);
        scores.add(3.6852688612219646);
        scores.add(3.6859330728661064);

        // rcf 3.8 has a number of improvements on thresholder and predictor corrector.
        // We don't expect the results have the same anomaly grade.
        for (int i = 0; i < coldStartData.size(); i++) {
            forest.process(coldStartData.get(i), 0);
            AnomalyDescriptor descriptor = forest.process(coldStartData.get(i), 0);
            assertEquals(scores.get(i), descriptor.getRCFScore(), 1e-9);
        }
    }

    // This test is intended to check if given a checkpoint created by RCF-3.0-rc3 ("rcf_3_0_rc3_hc.json")
    // and given the same sample data will rc3 and current RCF version (this test originally created when 3.0-rc3 is in use)
    // will produce the same anomaly scores.
    // The scores in this method were produced from AD running with RCF3.0-rc3 dependency
    // and this test runs with the most recent RCF dependency that is being pulled by this project.
    public void testDeserialize_rcf3_rc3_hc_model() throws Exception {
        // Model in rcf_3_0_rc3_hc.json is a checkpoint creatd by RCF-3.0-rc3
        // I generate the json response file using Postman's Send and Download
        URI uri = ClassLoader.getSystemResource("org/opensearch/ad/ml/rcf_3_0_rc3_hc.json").toURI();
        String filePath = Paths.get(uri).toString();
        String json = Files.readString(Paths.get(filePath), Charset.defaultCharset());
        // For the parsing of .toTrcf to work I had to manually change "\u003d" in code back to =.
        // In the byte array it doesn't seem like this is an issue but whenever reading the byte array response into a file it
        // converts "=" to "\u003d" https://groups.google.com/g/google-gson/c/JDHUo9DWyyM?pli=1
        // I also needed to bypass the trcf as it wasn't being read as a key value but instead part of the string
        Map map = gson.fromJson(json, Map.class);
        String model = (String) ((Map) ((Map) ((ArrayList) ((Map) map.get("hits")).get("hits")).get(0)).get("_source")).get("modelV2");
        model = model.split(":")[1];
        model = model.substring(1, model.length() - 2);
        // Simulate JSON parsing by replacing Unicode escape sequence with the actual character
        // Without escaping Java string, we experience model corruption exception.
        model = unescapeJavaString(model);

        ThresholdedRandomCutForest forest = checkpointDao.toTrcf(model);

        // hc model uses internal shingling
        List<double[]> coldStartData = new ArrayList<>();
        double[] sample1 = new double[] { 53, 54, 55, 56, 57.0 };
        double[] sample2 = new double[] { 54, 55, 56, 57.0, 1.0 };
        double[] sample3 = new double[] { 55, 56, 57.0, 1.0, -19.0 };
        double[] sample4 = new double[] { 56, 57.0, 1.0, -19.0, 13.0 };
        double[] sample5 = new double[] { 57.0, 1.0, -19.0, 13.0, 41.0 };

        coldStartData.add(sample1);
        coldStartData.add(sample2);
        coldStartData.add(sample3);
        coldStartData.add(sample4);
        coldStartData.add(sample5);

        // This scores were generated with the sample data but on RCF4.0 that changed implementation
        // and we are seeing different rcf scores between 4.0 and 3.8. This is verified by switching
        // rcf version between 3.8 and 4.0 while other code in AD unchanged. But we get different scores.
        List<Double> scores = new ArrayList<>();
        scores.add(2.119532552959117);
        scores.add(2.7347456872746325);
        scores.add(3.066704948143919);
        scores.add(3.2965580521876725);
        scores.add(3.1888920146607047);

        // rcf 3.8 has a number of improvements on thresholder and predictor corrector.
        // We don't expect the results have the same anomaly grade.
        for (int i = 0; i < coldStartData.size(); i++) {
            forest.process(coldStartData.get(i), 0);
            AnomalyDescriptor descriptor = forest.process(coldStartData.get(i), 0);
            assertEquals(scores.get(i), descriptor.getRCFScore(), 1e-9);
        }
    }

    public static String unescapeJavaString(String st) {
        StringBuilder sb = new StringBuilder(st.length());

        for (int i = 0; i < st.length(); i++) {
            char ch = st.charAt(i);
            if (ch == '\\') {
                char nextChar = (i == st.length() - 1) ? '\\' : st.charAt(i + 1);
                switch (nextChar) {
                    case 'u':
                        sb.append((char) Integer.parseInt(st.substring(i + 2, i + 6), 16));
                        i += 5;
                        break;
                    case '\\':
                        sb.append('\\');
                        i++;
                        break;
                    default:
                        sb.append(ch);
                        break;
                }
            } else {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

    public void testProcessEmptyCheckpoint() throws IOException {
        String modelId = "abc";
        ModelState<ThresholdedRandomCutForest> modelState = checkpointDao
            .processHCGetResponse(TestHelpers.createBrokenGetResponse(modelId, "blah"), modelId, "123");
        assertEquals(null, modelState);
    }

    public void testNonEmptyCheckpoint() throws IOException {
        String modelId = "abc";
        ModelState<ThresholdedRandomCutForest> inputModelState = MLUtil
            .randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());

        Map<String, Object> source = checkpointDao.toIndexSource(inputModelState);
        ModelState<ThresholdedRandomCutForest> modelState = checkpointDao
            .processHCGetResponse(TestHelpers.createGetResponse(source, modelId, "blah"), modelId, "123");
        assertEquals(now, modelState.getLastCheckpointTime());
        assertEquals(inputModelState.getSamples().size(), modelState.getSamples().size());
        assertEquals(now, modelState.getLastUsedTime());
    }
}
