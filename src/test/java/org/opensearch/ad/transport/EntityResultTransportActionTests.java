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

package org.opensearch.ad.transport;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.AnomalyDetectorJobRunnerTests;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.caching.CacheProvider;
import org.opensearch.ad.caching.EntityCache;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.JsonPathNotFoundException;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.constant.CommonValue;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.ml.EntityColdStarter;
import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.ratelimit.CheckpointReadWorker;
import org.opensearch.ad.ratelimit.ColdEntityWorker;
import org.opensearch.ad.ratelimit.EntityColdStartWorker;
import org.opensearch.ad.ratelimit.ResultWriteWorker;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStat;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.StatNames;
import org.opensearch.ad.stats.suppliers.CounterSupplier;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.transport.TransportService;

import test.org.opensearch.ad.util.JsonDeserializer;
import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;

public class EntityResultTransportActionTests extends AbstractADTest {
    EntityResultTransportAction entityResult;
    ActionFilters actionFilters;
    TransportService transportService;
    ModelManager manager;
    ADCircuitBreakerService adCircuitBreakerService;
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
    Entity cacheHitEntityObj;
    Entity cacheMissEntityObj;
    long start;
    long end;
    Map<Entity, double[]> entities;
    double[] cacheMissData;
    double[] cacheHitData;
    String tooLongEntity;
    double[] tooLongData;
    ResultWriteWorker resultWriteQueue;
    CheckpointReadWorker checkpointReadQueue;
    int minSamples;
    Instant now;
    EntityColdStarter coldStarter;
    ColdEntityWorker coldEntityQueue;
    EntityColdStartWorker entityColdStartQueue;
    AnomalyDetectionIndices indexUtil;
    ClusterService clusterService;
    ADStats adStats;

    @BeforeClass
    public static void setUpBeforeClass() {
        setUpThreadPool(AnomalyDetectorJobRunnerTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownAfterClass() {
        tearDownThreadPool();
    }

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        actionFilters = mock(ActionFilters.class);
        transportService = mock(TransportService.class);

        adCircuitBreakerService = mock(ADCircuitBreakerService.class);
        when(adCircuitBreakerService.isOpen()).thenReturn(false);

        checkpointDao = mock(CheckpointDao.class);

        detectorId = "123";
        entities = new HashMap<>();

        start = 10L;
        end = 20L;
        request = new EntityResultRequest(detectorId, entities, start, end);

        clock = mock(Clock.class);
        now = Instant.now();
        when(clock.instant()).thenReturn(now);

        settings = Settings
            .builder()
            .put(AnomalyDetectorSettings.COOLDOWN_MINUTES.getKey(), TimeValue.timeValueMinutes(5))
            .put(AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ.getKey(), TimeValue.timeValueHours(12))
            .build();

        clusterService = mock(ClusterService.class);
        ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ)))
        );
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        manager = new ModelManager(
            null,
            clock,
            0,
            0,
            0,
            0,
            0,
            0,
            null,
            AnomalyDetectorSettings.CHECKPOINT_SAVING_FREQ,
            mock(EntityColdStarter.class),
            null,
            null,
            settings,
            clusterService
        );

        provider = mock(CacheProvider.class);
        entityCache = mock(EntityCache.class);
        when(provider.get()).thenReturn(entityCache);

        String field = "a";
        detector = TestHelpers.randomAnomalyDetectorUsingCategoryFields(detectorId, Arrays.asList(field));
        stateManager = mock(NodeStateManager.class);
        doAnswer(invocation -> {
            ActionListener<Optional<AnomalyDetector>> listener = invocation.getArgument(1);
            listener.onResponse(Optional.of(detector));
            return null;
        }).when(stateManager).getAnomalyDetector(any(String.class), any(ActionListener.class));

        cacheMissEntity = "0.0.0.1";
        cacheMissData = new double[] { 0.1 };
        cacheHitEntity = "0.0.0.2";
        cacheHitData = new double[] { 0.2 };
        cacheMissEntityObj = Entity.createSingleAttributeEntity(detector.getCategoryField().get(0), cacheMissEntity);
        entities.put(cacheMissEntityObj, cacheMissData);
        cacheHitEntityObj = Entity.createSingleAttributeEntity(detector.getCategoryField().get(0), cacheHitEntity);
        entities.put(cacheHitEntityObj, cacheHitData);
        tooLongEntity = randomAlphaOfLength(AnomalyDetectorSettings.MAX_ENTITY_LENGTH + 1);
        tooLongData = new double[] { 0.3 };
        entities.put(Entity.createSingleAttributeEntity(detector.getCategoryField().get(0), tooLongEntity), tooLongData);

        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        when(entityCache.get(eq(cacheMissEntityObj.getModelId(detectorId).get()), any())).thenReturn(null);
        when(entityCache.get(eq(cacheHitEntityObj.getModelId(detectorId).get()), any())).thenReturn(state);

        List<Entity> coldEntities = new ArrayList<>();
        coldEntities.add(cacheMissEntityObj);
        when(entityCache.selectUpdateCandidate(any(), anyString(), any())).thenReturn(Pair.of(new ArrayList<>(), coldEntities));

        indexUtil = mock(AnomalyDetectionIndices.class);
        when(indexUtil.getSchemaVersion(any())).thenReturn(CommonValue.NO_SCHEMA_VERSION);

        resultWriteQueue = mock(ResultWriteWorker.class);
        checkpointReadQueue = mock(CheckpointReadWorker.class);

        minSamples = 1;

        coldStarter = mock(EntityColdStarter.class);

        doAnswer(invocation -> {
            ModelState<EntityModel> modelState = invocation.getArgument(0);
            modelState.getModel().clear();
            return null;
        }).when(coldStarter).trainModelFromExistingSamples(any(), anyInt());

        coldEntityQueue = mock(ColdEntityWorker.class);
        entityColdStartQueue = mock(EntityColdStartWorker.class);

        Map<String, ADStat<?>> statsMap = new HashMap<String, ADStat<?>>() {
            {
                put(StatNames.MODEL_CORRUTPION_COUNT.getName(), new ADStat<>(false, new CounterSupplier()));
            }
        };

        adStats = new ADStats(statsMap);

        entityResult = new EntityResultTransportAction(
            actionFilters,
            transportService,
            manager,
            adCircuitBreakerService,
            provider,
            stateManager,
            indexUtil,
            resultWriteQueue,
            checkpointReadQueue,
            coldEntityQueue,
            threadPool,
            entityColdStartQueue,
            adStats
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

        verify(resultWriteQueue, times(1)).put(any());
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

    // test rcf score is 0
    public void testNoResultsToSave() {
        ModelState<EntityModel> state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(false).build());
        when(entityCache.get(eq(cacheHitEntityObj.getModelId(detectorId).get()), any())).thenReturn(state);

        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();

        entityResult.doExecute(null, request, future);

        future.actionGet(timeoutMs);

        verify(resultWriteQueue, never()).put(any());
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
        assertEquals(JsonDeserializer.getTextValue(json, CommonName.ID_JSON_KEY), detectorId);
        assertEquals(JsonDeserializer.getLongValue(json, CommonName.START_JSON_KEY), start);
        assertEquals(JsonDeserializer.getLongValue(json, CommonName.END_JSON_KEY), end);
        JsonArray array = JsonDeserializer.getArrayValue(json, CommonName.ENTITIES_JSON_KEY);
        assertEquals(3, array.size());
        for (int i = 0; i < 3; i++) {
            JsonElement element = array.get(i);
            JsonElement entity = JsonDeserializer.getChildNode(element, CommonName.ENTITY_KEY);
            JsonArray entityArray = entity.getAsJsonArray();
            assertEquals(1, entityArray.size());

            JsonElement attribute = entityArray.get(0);
            String entityValue = JsonDeserializer.getChildNode(attribute, Entity.ATTRIBUTE_VALUE_FIELD).getAsString();

            double value = JsonDeserializer.getChildNode(element, CommonName.VALUE_JSON_KEY).getAsJsonArray().get(0).getAsDouble();

            if (entityValue.equals(cacheMissEntity)) {
                assertEquals(0, Double.compare(cacheMissData[0], value));
            } else if (entityValue.equals(cacheHitEntity)) {
                assertEquals(0, Double.compare(cacheHitData[0], value));
            } else {
                assertEquals(0, Double.compare(tooLongData[0], value));
            }
        }
    }

    public void testFailToScore() {
        ModelManager spyModelManager = spy(manager);
        doThrow(new IllegalArgumentException()).when(spyModelManager).getAnomalyResultForEntity(any(), any(), anyString(), any(), anyInt());
        entityResult = new EntityResultTransportAction(
            actionFilters,
            transportService,
            spyModelManager,
            adCircuitBreakerService,
            provider,
            stateManager,
            indexUtil,
            resultWriteQueue,
            checkpointReadQueue,
            coldEntityQueue,
            threadPool,
            entityColdStartQueue,
            adStats
        );

        PlainActionFuture<AcknowledgedResponse> future = PlainActionFuture.newFuture();

        entityResult.doExecute(null, request, future);

        future.actionGet(timeoutMs);

        verify(resultWriteQueue, never()).put(any());
        verify(entityCache, times(1)).removeEntityModel(anyString(), anyString());
        verify(entityColdStartQueue, times(1)).put(any());
        Object val = adStats.getStat(StatNames.MODEL_CORRUTPION_COUNT.getName()).getValue();
        assertEquals(1L, ((Long) val).longValue());
    }
}
