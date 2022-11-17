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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;

import org.opensearch.action.ActionListener;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.ml.ModelManager.ModelType;
import org.opensearch.common.util.concurrent.OpenSearchRejectedExecutionException;

import test.org.opensearch.ad.util.MLUtil;

import com.amazon.randomcutforest.RandomCutForest;

public class EntityColdStarterTests extends AbstractCosineDataTest {
    // train using samples directly
    public void testTrainUsingSamples() {
        Queue<double[]> samples = MLUtil.createQueueSamples(numMinSamples);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        assertEquals(numMinSamples, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);
    }

    public void testColdStart() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        coldStartSamples.add(Optional.of(new double[] { -19.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        waitForColdStartFinish();
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        // maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 2 + 1 = 129
        assertEquals(129, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);

        // sleep 1 secs to give time for the last timestamp record to expire when superShortLastColdStartTimeState = true
        Thread.sleep(1000L);

        // too frequent cold start of the same detector will fail
        samples = MLUtil.createQueueSamples(1);
        model = new EntityModel(modelId, samples, null, null);
        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);
        waitForColdStartFinish();

        forest = model.getRcf();

        assertTrue(forest == null);
        assertTrue(model.getThreshold() == null);
    }

    // cold start running, return immediately
    public void testColdStartRunning() {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        NodeStateManager spyNodeStateManager = spy(stateManager);
        spyNodeStateManager.markColdStartRunning(detectorId);
        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        verify(spyNodeStateManager, never()).getAnomalyDetector(any(), any());
    }

    // min max: miss one
    public void testMissMin() throws IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.empty(), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        verify(searchFeatureDao, never()).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        RandomCutForest forest = model.getRcf();
        assertTrue(forest == null);
        assertTrue(model.getThreshold() == null);
    }

    // two segments of samples, one segment has 3 samples, while another one has only 1
    public void testTwoSegmentsWithSingleSample() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        coldStartSamples.add(Optional.of(new double[] { -19.0 }));
        coldStartSamples.add(Optional.empty());
        coldStartSamples.add(Optional.of(new double[] { -17.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        int maxWaitTimes = 20;
        int i = 0;
        while (stateManager.isColdStartRunning(detectorId) && i < maxWaitTimes) {
            // wait for 1 second
            Thread.sleep(500L);
            i++;
        }
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        // 1st segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 2 + 1 = 129
        // 2nd segment: 1
        assertEquals(130, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);
    }

    // two segments of samples, one segment has 3 samples, while another one 2 samples
    public void testTwoSegments() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        coldStartSamples.add(Optional.of(new double[] { -19.0 }));
        coldStartSamples.add(Optional.empty());
        coldStartSamples.add(Optional.of(new double[] { -17.0 }));
        coldStartSamples.add(Optional.of(new double[] { -38.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        int maxWaitTimes = 20;
        int i = 0;
        while (stateManager.isColdStartRunning(detectorId) && i < maxWaitTimes) {
            // wait for 1 second
            Thread.sleep(500L);
            i++;
        }
        RandomCutForest forest = model.getRcf();
        assertTrue(forest != null);
        // 1st segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 2 + 1 = 129
        // 2nd segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 1 + 1 = 65
        assertEquals(194, forest.getTotalUpdates());
        assertTrue(model.getThreshold() != null);
    }

    public void testThrottledColdStart() {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onFailure(new OpenSearchRejectedExecutionException(""));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        entityColdStarter.trainModel(samples, modelId, entityName, "456", modelState);

        // only the first one makes the call
        verify(searchFeatureDao, times(1)).getEntityMinMaxDataTime(any(), any(), any());
    }

    public void testColdStartException() {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onFailure(new AnomalyDetectionException(detectorId, ""));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        assertTrue(stateManager.getLastDetectionError(detectorId) != null);
    }

    public void testNotEnoughSamples() throws InterruptedException, IOException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(1602269260000L), Optional.of(1602401500000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        List<Optional<double[]>> coldStartSamples = new ArrayList<>();
        coldStartSamples.add(Optional.of(new double[] { 57.0 }));
        coldStartSamples.add(Optional.of(new double[] { 1.0 }));
        doAnswer(invocation -> {
            ActionListener<List<Optional<double[]>>> listener = invocation.getArgument(4);
            listener.onResponse(coldStartSamples);
            return null;
        }).when(searchFeatureDao).getColdStartSamplesForPeriods(any(), any(), any(), anyBoolean(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);

        int maxWaitTimes = 20;
        int i = 0;
        while (stateManager.isColdStartRunning(detectorId) && i < maxWaitTimes) {
            // wait for 1 second
            Thread.sleep(500L);
            i++;
        }
        assertTrue(model.getRcf() == null);
        assertTrue(model.getThreshold() == null);
        // 1st segment: maxSampleStride * (continuousSampledArray.length - 1) + 1 = 64 * 1 + 1 = 65
        // 65 + origin 1 data points
        assertEquals("real sample size is " + model.getSamples().size(), 66, model.getSamples().size());
    }

    public void testEmptyDataRange() throws InterruptedException {
        Queue<double[]> samples = MLUtil.createQueueSamples(1);
        EntityModel model = new EntityModel(modelId, samples, null, null);
        modelState = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);

        doAnswer(invocation -> {
            ActionListener<Entry<Optional<Long>, Optional<Long>>> listener = invocation.getArgument(2);
            listener.onResponse(new SimpleImmutableEntry<>(Optional.of(894056973000L), Optional.of(894057860000L)));
            return null;
        }).when(searchFeatureDao).getEntityMinMaxDataTime(any(), any(), any());

        entityColdStarter.trainModel(samples, modelId, entityName, detectorId, modelState);
        waitForColdStartFinish();

        assertTrue(model.getRcf() == null);
        assertTrue(model.getThreshold() == null);
        // the min-max range is too small and thus no data range can be found
        assertEquals("real sample size is " + model.getSamples().size(), 1, model.getSamples().size());
    }
}
