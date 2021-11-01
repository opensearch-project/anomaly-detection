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

package test.org.opensearch.ad.util;

import static java.lang.Math.PI;

import java.time.Clock;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.stream.IntStream;

import org.opensearch.ad.ml.EntityModel;
import org.opensearch.ad.ml.ModelManager.ModelType;
import org.opensearch.ad.ml.ModelState;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.collect.Tuple;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

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

    public static ModelState<EntityModel> randomModelState(RandomModelStateConfig config) {
        boolean fullModel = config.getFullModel() != null && config.getFullModel().booleanValue() ? true : false;
        float priority = config.getPriority() != null ? config.getPriority() : random.nextFloat();
        String detectorId = config.getDetectorId() != null ? config.getDetectorId() : randomString(15);
        int sampleSize = config.getSampleSize() != null ? config.getSampleSize() : random.nextInt(minSampleSize);
        Clock clock = config.getClock() != null ? config.getClock() : Clock.systemUTC();

        Entity entity = null;
        if (config.hasEntityAttributes()) {
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("a", "a1");
            attributes.put("b", "b1");
            entity = Entity.createEntityByReordering(attributes);
        } else {
            entity = Entity.createSingleAttributeEntity("", "");
        }
        EntityModel model = null;
        if (fullModel) {
            model = createNonEmptyModel(detectorId, sampleSize, entity);
        } else {
            model = createEmptyModel(entity, sampleSize);
        }

        return new ModelState<>(model, detectorId, detectorId, ModelType.ENTITY.getName(), clock, priority);
    }

    public static EntityModel createEmptyModel(Entity entity, int sampleSize) {
        Queue<double[]> samples = createQueueSamples(sampleSize);
        return new EntityModel(entity, samples, null);
    }

    public static EntityModel createEmptyModel(Entity entity) {
        return createEmptyModel(entity, random.nextInt(minSampleSize));
    }

    public static EntityModel createNonEmptyModel(String detectorId, int sampleSize, Entity entity) {
        Queue<double[]> samples = createQueueSamples(sampleSize);
        int numDataPoints = random.nextInt(1000) + AnomalyDetectorSettings.NUM_MIN_SAMPLES;
        ThresholdedRandomCutForest trcf = new ThresholdedRandomCutForest(
            ThresholdedRandomCutForest
                .builder()
                .dimensions(1)
                .sampleSize(AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE)
                .numberOfTrees(AnomalyDetectorSettings.NUM_TREES)
                .timeDecay(AnomalyDetectorSettings.TIME_DECAY)
                .outputAfter(AnomalyDetectorSettings.NUM_MIN_SAMPLES)
                .initialAcceptFraction(0.125d)
                .parallelExecutionEnabled(false)
                .internalShinglingEnabled(true)
                .anomalyRate(1 - AnomalyDetectorSettings.THRESHOLD_MIN_PVALUE)
        );
        for (int i = 0; i < numDataPoints; i++) {
            trcf.process(new double[] { random.nextDouble() }, i);
        }
        EntityModel entityModel = new EntityModel(entity, samples, trcf);
        return entityModel;
    }

    public static EntityModel createNonEmptyModel(String detectorId) {
        return createNonEmptyModel(detectorId, random.nextInt(minSampleSize), Entity.createSingleAttributeEntity("", ""));
    }

    /**
     * Generate shingled data
     * @param size the number of data points
     * @param dimensions the dimensions of a point
     * @param seed random seed
     * @return the shingled data
     */
    public static double[][] generateShingledData(int size, int dimensions, long seed) {
        double[][] answer = new double[size][];
        int entryIndex = 0;
        boolean filledShingleAtleastOnce = false;
        double[] history = new double[dimensions];
        int count = 0;
        double[] data = getDataD(size + dimensions - 1, 100, 5, seed);
        for (int j = 0; j < size + dimensions - 1; ++j) {
            history[entryIndex] = data[j];
            entryIndex = (entryIndex + 1) % dimensions;
            if (entryIndex == 0) {
                filledShingleAtleastOnce = true;
            }
            if (filledShingleAtleastOnce) {
                answer[count++] = getShinglePoint(history, entryIndex, dimensions);
            }
        }
        return answer;
    }

    private static double[] getShinglePoint(double[] recentPointsSeen, int indexOfOldestPoint, int shingleLength) {
        double[] shingledPoint = new double[shingleLength];
        int i = 0;
        for (int j = 0; j < shingleLength; ++j) {
            double point = recentPointsSeen[(j + indexOfOldestPoint) % shingleLength];
            shingledPoint[i++] = point;

        }
        return shingledPoint;
    }

    static double[] getDataD(int num, double amplitude, double noise, long seed) {

        double[] data = new double[num];
        Random noiseprg = new Random(seed);
        for (int i = 0; i < num; i++) {
            data[i] = amplitude * Math.cos(2 * PI * (i + 50) / 1000) + noise * noiseprg.nextDouble();
        }

        return data;
    }

    /**
     * Prepare models and return training samples
     * @param inputDimension Input dimension
     * @param rcfConfig RCF config
     * @return models and return training samples
     */
    public static Tuple<Queue<double[]>, ThresholdedRandomCutForest> prepareModel(
        int inputDimension,
        ThresholdedRandomCutForest.Builder<?> rcfConfig
    ) {
        Queue<double[]> samples = new ArrayDeque<>();

        Random r = new Random();
        ThresholdedRandomCutForest rcf = new ThresholdedRandomCutForest(rcfConfig);

        int trainDataNum = 1000;

        for (int i = 0; i < trainDataNum; i++) {
            double[] point = r.ints(inputDimension, 0, 50).asDoubleStream().toArray();
            samples.add(point);
            rcf.process(point, 0);
        }

        return Tuple.tuple(samples, rcf);
    }
}
