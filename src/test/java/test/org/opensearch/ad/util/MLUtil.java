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
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.common.collect.Tuple;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Cannot use TestUtil inside ML tests since it uses com.carrotsearch.randomizedtesting.RandomizedRunner
 * and using it causes Exception in ML tests.
 * Most of ML tests are not a subclass if ES base test case.
 *
 */
public class MLUtil {
    private static Random random = new Random(42);
    private static int minSampleSize = TimeSeriesSettings.NUM_MIN_SAMPLES;

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

    public static Deque<Sample> createQueueSamples(int size) {
        Deque<Sample> res = new ArrayDeque<>();
        IntStream.range(0, size).forEach(i -> res.offer(new Sample(new double[] { random.nextDouble() }, Instant.now(), Instant.now())));
        return res;
    }

    public static ModelState<ThresholdedRandomCutForest> randomModelState(RandomModelStateConfig config) {
        boolean fullModel = config.getFullModel() != null && config.getFullModel().booleanValue() ? true : false;
        float priority = config.getPriority() != null ? config.getPriority() : random.nextFloat();
        String detectorId = config.getId() != null ? config.getId() : randomString(15);
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
        Pair<ThresholdedRandomCutForest, Deque<Sample>> model = null;
        if (fullModel) {
            model = createNonEmptyModel(detectorId, sampleSize, entity);
        } else {
            model = createEmptyModel(entity, sampleSize);
        }

        return new ModelState<ThresholdedRandomCutForest>(
            model.getLeft(),
            detectorId,
            detectorId,
            ModelManager.ModelType.TRCF.getName(),
            clock,
            priority,
            Optional.of(entity),
            model.getRight()
        );
    }

    public static Pair<ThresholdedRandomCutForest, Deque<Sample>> createEmptyModel(Entity entity, int sampleSize) {
        Deque<Sample> samples = createQueueSamples(sampleSize);
        return Pair.of(null, samples);
    }

    public static Pair<ThresholdedRandomCutForest, Deque<Sample>> createEmptyModel(Entity entity) {
        return createEmptyModel(entity, random.nextInt(minSampleSize));
    }

    public static Pair<ThresholdedRandomCutForest, Deque<Sample>> createNonEmptyModel(String detectorId, int sampleSize, Entity entity) {
        Deque<Sample> samples = createQueueSamples(sampleSize);
        int numDataPoints = random.nextInt(1000) + TimeSeriesSettings.NUM_MIN_SAMPLES;
        ThresholdedRandomCutForest trcf = new ThresholdedRandomCutForest(
            ThresholdedRandomCutForest
                .builder()
                .dimensions(1)
                .sampleSize(TimeSeriesSettings.NUM_SAMPLES_PER_TREE)
                .numberOfTrees(TimeSeriesSettings.NUM_TREES)
                .timeDecay(0.0001)
                .outputAfter(TimeSeriesSettings.NUM_MIN_SAMPLES)
                .initialAcceptFraction(0.125d)
                .parallelExecutionEnabled(false)
                .internalShinglingEnabled(true)
                .anomalyRate(1 - TimeSeriesSettings.THRESHOLD_MIN_PVALUE)
                .transformMethod(TransformMethod.NORMALIZE)
                .alertOnce(true)
                .autoAdjust(true)
        );
        for (int i = 0; i < numDataPoints; i++) {
            trcf.process(new double[] { random.nextDouble() }, i);
        }
        return Pair.of(trcf, samples);
    }

    public static Pair<ThresholdedRandomCutForest, Deque<Sample>> createNonEmptyModel(String detectorId) {
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
     * @param intervalMillis detector interval in milliseconds
     * @return models and return training samples
     */
    public static Tuple<Deque<Sample>, ThresholdedRandomCutForest> prepareModel(
        int inputDimension,
        ThresholdedRandomCutForest.Builder<?> rcfConfig,
        long intervalMillis
    ) {
        Deque<Sample> samples = new ArrayDeque<>();

        Random r = new Random();
        ThresholdedRandomCutForest rcf = new ThresholdedRandomCutForest(rcfConfig);

        int trainDataNum = 1000;

        Instant currentTime = Instant.now();
        for (int i = 0; i < trainDataNum; i++) {
            double[] point = r.ints(inputDimension, 0, 50).asDoubleStream().toArray();
            samples.add(new Sample(point, currentTime.minusMillis(intervalMillis), currentTime));
            rcf.process(point, currentTime.getEpochSecond());
            currentTime = currentTime.plusMillis(intervalMillis);
        }

        return Tuple.tuple(samples, rcf);
    }
}
