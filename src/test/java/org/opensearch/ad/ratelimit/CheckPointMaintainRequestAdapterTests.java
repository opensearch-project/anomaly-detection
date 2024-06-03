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

package org.opensearch.ad.ratelimit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import org.opensearch.action.update.UpdateRequest;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.caching.ADPriorityCache;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.ml.ADCheckpointDao;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ratelimit.CheckPointMaintainRequestAdapter;
import org.opensearch.timeseries.ratelimit.CheckpointMaintainRequest;
import org.opensearch.timeseries.ratelimit.CheckpointWriteRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

import test.org.opensearch.ad.util.MLUtil;
import test.org.opensearch.ad.util.RandomModelStateConfig;

public class CheckPointMaintainRequestAdapterTests extends AbstractRateLimitingTest {
    private ADCacheProvider cache;
    private ADCheckpointDao checkpointDao;
    private String indexName;
    private Setting<TimeValue> checkpointInterval;
    private CheckPointMaintainRequestAdapter adapter;
    private ModelState<ThresholdedRandomCutForest> state;
    private CheckpointMaintainRequest request;
    private ClusterService clusterService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        cache = mock(ADCacheProvider.class);
        checkpointDao = mock(ADCheckpointDao.class);
        indexName = ADCommonName.CHECKPOINT_INDEX_NAME;
        checkpointInterval = AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ;
        ADPriorityCache entityCache = mock(ADPriorityCache.class);
        when(cache.get()).thenReturn(entityCache);
        state = MLUtil.randomModelState(new RandomModelStateConfig.Builder().fullModel(true).build());
        when(entityCache.getForMaintainance(anyString(), anyString())).thenReturn(Optional.of(state));
        clusterService = mock(ClusterService.class);
        ClusterSettings settings = new ClusterSettings(
            Settings.EMPTY,
            Collections.unmodifiableSet(new HashSet<>(Arrays.asList(AnomalyDetectorSettings.AD_CHECKPOINT_SAVING_FREQ)))
        );
        when(clusterService.getClusterSettings()).thenReturn(settings);
        adapter = new CheckPointMaintainRequestAdapter(
            checkpointDao,
            indexName,
            checkpointInterval,
            clock,
            clusterService,
            Settings.EMPTY,
            cache
        );
        request = new CheckpointMaintainRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, entity.getModelId(detectorId).get());

    }

    public void testShouldNotSave() {
        when(checkpointDao.shouldSave(any(), anyBoolean(), any(), any())).thenReturn(false);
        assertTrue(adapter.convert(request).isEmpty());
    }

    public void testIndexSourceNull() throws IOException {
        when(checkpointDao.shouldSave(any(), anyBoolean(), any(), any())).thenReturn(true);
        when(checkpointDao.toIndexSource(any())).thenReturn(null);
        assertTrue(adapter.convert(request).isEmpty());
    }

    public void testIndexSourceEmpty() throws IOException {
        when(checkpointDao.shouldSave(any(), anyBoolean(), any(), any())).thenReturn(true);
        when(checkpointDao.toIndexSource(any())).thenReturn(new HashMap<String, Object>());
        assertTrue(adapter.convert(request).isEmpty());
    }

    public void testModelIdEmpty() throws IOException {
        when(checkpointDao.shouldSave(any(), anyBoolean(), any(), any())).thenReturn(true);
        Map<String, Object> content = new HashMap<String, Object>();
        content.put("a", "b");
        when(checkpointDao.toIndexSource(any())).thenReturn(content);
        assertTrue(adapter.convert(new CheckpointMaintainRequest(Integer.MAX_VALUE, detectorId, RequestPriority.MEDIUM, null)).isEmpty());
    }

    public void testNormal() throws IOException {
        when(checkpointDao.shouldSave(any(), anyBoolean(), any(), any())).thenReturn(true);
        Map<String, Object> content = new HashMap<String, Object>();
        content.put("a", "b");
        when(checkpointDao.toIndexSource(any())).thenReturn(content);
        Optional<CheckpointWriteRequest> converted = adapter.convert(request);
        assertTrue(!converted.isEmpty());
        UpdateRequest updateRequest = converted.get().getUpdateRequest();
        UpdateRequest expectedRequest = new UpdateRequest(indexName, entity.getModelId(detectorId).get()).docAsUpsert(true).doc(content);
        assertEquals(updateRequest.docAsUpsert(), expectedRequest.docAsUpsert());
        assertEquals(updateRequest.detectNoop(), expectedRequest.detectNoop());
        assertEquals(updateRequest.fetchSource(), expectedRequest.fetchSource());
    }

    public void testIndexSourceException() throws IOException {
        doThrow(IllegalArgumentException.class).when(checkpointDao).toIndexSource(any());
        assertTrue(adapter.convert(request).isEmpty());
    }
}
