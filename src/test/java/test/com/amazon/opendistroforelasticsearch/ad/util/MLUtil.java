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

package test.com.amazon.opendistroforelasticsearch.ad.util;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.Random;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.HybridThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager.ModelType;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.ml.ThresholdingModel;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.amazon.randomcutforest.RandomCutForest;

/**
 * Cannot use TestUtil inside ML tests since it uses com.carrotsearch.randomizedtesting.RandomizedRunner
 * and using it causes Exception in ML tests.
 * Most of ML tests are not a subclass if ES base test case.
 *
 */
public class MLUtil {
    private static Random random = new Random(42);
    private static int minSampleSize = AnomalyDetectorSettings.NUM_MIN_SAMPLES;

    private static String randomString(int targetStringLength) {
        int leftLimit = 97; // letter 'a'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();

        return random
            .ints(leftLimit, rightLimit + 1)
            .limit(targetStringLength)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }

    public static Queue<double[]> createQueueSamples(int size) {
        Queue<double[]> res = new ArrayDeque<>();
        IntStream.range(0, size).forEach(i -> res.offer(new double[] { random.nextDouble() }));
        return res;
    }

    public static ModelState<EntityModel> randomModelState() {
        return randomModelState(random.nextBoolean(), random.nextFloat(), randomString(15), random.nextInt(minSampleSize));
    }

    public static ModelState<EntityModel> randomModelState(boolean fullModel, float priority, String modelId, int sampleSize) {
        String detectorId = randomString(5);
        EntityModel model = null;
        if (fullModel) {
            model = createNonEmptyModel(modelId, sampleSize);
        } else {
            model = createEmptyModel(modelId, sampleSize);
        }

        return new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), Clock.systemUTC(), priority);
    }

    public static ModelState<EntityModel> randomNonEmptyModelState() {
        return randomModelState(true, random.nextFloat(), randomString(15), random.nextInt(minSampleSize));
    }

    public static ModelState<EntityModel> randomEmptyModelState() {
        return randomModelState(false, random.nextFloat(), randomString(15), random.nextInt(minSampleSize));
    }

    public static ModelState<EntityModel> randomModelState(float priority, String modelId) {
        return randomModelState(random.nextBoolean(), priority, modelId, random.nextInt(minSampleSize));
    }

    public static ModelState<EntityModel> randomModelStateWithSample(boolean fullModel, int sampleSize) {
        return randomModelState(fullModel, random.nextFloat(), randomString(15), sampleSize);
    }

    public static EntityModel createEmptyModel(String modelId, int sampleSize) {
        Queue<double[]> samples = createQueueSamples(sampleSize);
        return new EntityModel(modelId, samples, null, null);
    }

    public static EntityModel createEmptyModel(String modelId) {
        return createEmptyModel(modelId, random.nextInt(minSampleSize));
    }

    public static EntityModel createNonEmptyModel(String modelId, int sampleSize) {
        Queue<double[]> samples = createQueueSamples(sampleSize);
        RandomCutForest rcf = RandomCutForest
            .builder()
            .dimensions(1)
            .sampleSize(AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE)
            .numberOfTrees(AnomalyDetectorSettings.MULTI_ENTITY_NUM_TREES)
            .lambda(AnomalyDetectorSettings.TIME_DECAY)
            .outputAfter(AnomalyDetectorSettings.NUM_MIN_SAMPLES)
            .parallelExecutionEnabled(false)
            .build();
        int numDataPoints = random.nextInt(1000) + AnomalyDetectorSettings.NUM_MIN_SAMPLES;
        double[] scores = new double[numDataPoints];
        for (int j = 0; j < numDataPoints; j++) {
            double[] dataPoint = new double[] { random.nextDouble() };
            scores[j] = rcf.getAnomalyScore(dataPoint);
            rcf.update(dataPoint);
        }

        double[] nonZeroScores = DoubleStream.of(scores).filter(score -> score > 0).toArray();
        ThresholdingModel threshold = new HybridThresholdingModel(
            AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE,
            AnomalyDetectorSettings.THRESHOLD_MAX_RANK_ERROR,
            AnomalyDetectorSettings.THRESHOLD_MAX_SCORE,
            AnomalyDetectorSettings.THRESHOLD_NUM_LOGNORMAL_QUANTILES,
            AnomalyDetectorSettings.THRESHOLD_DOWNSAMPLES,
            AnomalyDetectorSettings.THRESHOLD_MAX_SAMPLES
        );
        threshold.train(nonZeroScores);
        return new EntityModel(modelId, samples, rcf, threshold);
    }

    public static EntityModel createNonEmptyModel(String modelId) {
        return createNonEmptyModel(modelId, random.nextInt(minSampleSize));
    }
}
