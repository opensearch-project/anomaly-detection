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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class HybridThresholdingModelTests {

    /*
     * Returns samples from the log-normal distribution.
     *
     * Given random Gaussian samples X ~ N(mu, sigma), samples from the
     * log-normal distribution are given by Y ~ e^X.
     *
     * @param sampleSize  number of log-normal samples to generate
     * @param mu          mean
     * @param sigma       standard deviation
     */
    private double[] logNormalSamples(int sampleSize, double mu, double sigma) {
        NormalDistribution distribution = new NormalDistribution(mu, sigma);
        distribution.reseedRandomGenerator(0L);

        double[] samples = new double[sampleSize];
        for (int i = 0; i < sampleSize; i++) {
            samples[i] = Math.exp(distribution.sample());
        }
        return samples;
    }

    private Object[] getTestGettersParameters() {
        double minPvalueThreshold = 0.8;
        double maxRankError = 0.001;
        double maxScore = 10;
        int numLogNormalQuantiles = 0;
        int downsampleNumSamples = 100_000;
        long downsampleMaxNumObservations = 10_000_000L;
        HybridThresholdingModel model = new HybridThresholdingModel(
            minPvalueThreshold,
            maxRankError,
            maxScore,
            numLogNormalQuantiles,
            downsampleNumSamples,
            downsampleMaxNumObservations
        );

        return new Object[] {
            new Object[] {
                model,
                minPvalueThreshold,
                maxRankError,
                maxScore,
                numLogNormalQuantiles,
                downsampleNumSamples,
                downsampleMaxNumObservations } };
    }

    @Test
    @Parameters(method = "getTestGettersParameters")
    public void testGetters(
        HybridThresholdingModel model,
        double minPvalueThreshold,
        double maxRankError,
        double maxScore,
        int numLogNormalQuantiles,
        int downsampleNumSamples,
        long downsampleMaxNumObservations
    ) {
        double delta = 1e-4;
        assertEquals(minPvalueThreshold, model.getMinPvalueThreshold(), delta);
        assertEquals(maxRankError, model.getMaxRankError(), delta);
        assertEquals(maxScore, model.getMaxScore(), delta);
        assertEquals(numLogNormalQuantiles, model.getNumLogNormalQuantiles());
        assertEquals(downsampleNumSamples, model.getDownsampleNumSamples());
        assertEquals(downsampleMaxNumObservations, model.getDownsampleMaxNumObservations());
    }

    @Test
    public void emptyConstructor_returnNonNullInstance() {
        assertTrue(new HybridThresholdingModel() != null);
    }

    private Object[] getThrowsExpectedInitializationExceptionParameters() {
        return new Object[] {
            new Object[] { 0.0, 0.001, 10, 10, 100, 1000 },
            new Object[] { 1.0, 0.001, 10, 10, 100, 1000 },
            new Object[] { 0.9, 0.123, 10, 10, 100, 1000 },
            new Object[] { 0.9, -0.01, 10, 10, 100, 1000 },
            new Object[] { 0.9, 0.001, -8, 10, 100, 1000 },
            new Object[] { 0.9, 0.001, 10, -1, 100, 1000 },
            new Object[] { 0.9, 0.001, 10, 10, 1, 1000 },
            new Object[] { 0.9, 0.001, 10, 10, 0, 1000 },
            new Object[] { 0.9, 0.001, 10, 10, 10_000, 1000 }, };
    }

    @Test(expected = IllegalArgumentException.class)
    @Parameters(method = "getThrowsExpectedInitializationExceptionParameters")
    public void throwsExpectedInitializationExceptions(
        double minPvalueThreshold,
        double maxRankError,
        double maxScore,
        int numLogNormalQuantiles,
        int downsampleNumSamples,
        int downsampleMaxNumObservations
    ) {
        HybridThresholdingModel invalidModel = new HybridThresholdingModel(
            minPvalueThreshold,
            maxRankError,
            maxScore,
            numLogNormalQuantiles,
            downsampleNumSamples,
            downsampleMaxNumObservations
        );
    }

    private Object[] getTestExpectedGradesWithUpdateParameters() {
        double mu = 1.2;
        double sigma = 3.4;
        double[] trainingAnomalyScores = logNormalSamples(1_000_000, 1.2, 3.4);
        double maxScore = Arrays.stream(trainingAnomalyScores).max().getAsDouble();
        HybridThresholdingModel model = new HybridThresholdingModel(1e-8, 1e-5, maxScore, 10_000, 2, 5_000_000);
        model.train(trainingAnomalyScores);

        return new Object[] {
            new Object[] {
                model,
                new double[] {},
                new double[] { 0.0, Math.exp(mu), Math.exp(mu + sigma), maxScore, HybridThresholdingModel.MIN_SCORE },
                new double[] { 0.0, 0.5, 0.84134, 1.0, 0 }, },
            new Object[] {
                model,
                trainingAnomalyScores,
                new double[] { 0.0, Math.exp(mu), Math.exp(mu + sigma), maxScore },
                new double[] { 0.0, 0.5, 0.84134, 1.0 }, },
            new Object[] {
                new HybridThresholdingModel(1e-8, 1e-5, maxScore, 10_000, 2, 5_000_000),
                new double[0],
                new double[] { 1.0 },
                new double[] { 0.0 }, } };
    }

    @Test
    @Parameters(method = "getTestExpectedGradesWithUpdateParameters")
    public void testExpectedGradesWithUpdate(
        HybridThresholdingModel model,
        double[] updateAnomalyScores,
        double[] testAnomalyScores,
        double[] expectedGrades
    ) {
        double delta = 1e-3;
        for (double anomalyScore : updateAnomalyScores) {
            model.update(anomalyScore);
        }

        for (int i = 0; i < testAnomalyScores.length; i++) {
            double expectedGrade = expectedGrades[i];
            double actualGrade = model.grade(testAnomalyScores[i]);
            assertEquals(expectedGrade, actualGrade, delta);
        }
    }

    private Object[] getTestConfidenceParameters() {
        double maxRankError = 0.001;
        double[] trainingAnomalyScores = logNormalSamples(1_000_000, 1.2, 3.4);
        double maxScore = Arrays.stream(trainingAnomalyScores).max().getAsDouble();
        HybridThresholdingModel model = new HybridThresholdingModel(0.8, maxRankError, maxScore, 1000, 2, 5_000_000);
        model.train(trainingAnomalyScores);
        for (double anomalyScore : trainingAnomalyScores) {
            model.update(anomalyScore);
        }

        HybridThresholdingModel newModel = new HybridThresholdingModel(0.8, maxRankError, maxScore, 1000, 2, 5_000_000);

        return new Object[] { new Object[] { model, 0.99 }, new Object[] { newModel, 0.99 } };
    }

    @Test
    @Parameters(method = "getTestConfidenceParameters")
    public void testConfidence(HybridThresholdingModel model, double expectedConfidence) {
        double delta = 1e-2;
        assertEquals(expectedConfidence, model.confidence(), delta);
    }

    private Object[] getTestDownsamplingParameters() {
        double[] scores = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        HybridThresholdingModel model = new HybridThresholdingModel(1e-8, 1e-4, 9, 0, 5, 9);
        for (double score : scores)
            model.update(score);

        return new Object[] {
            new Object[] {
                model, // model ECDF should be equal to [1, 3, 5, 7, 9]
                new double[] { 1.0, 1.1, 2.0, 3.0, 4.0, 4.1, 5.0, 5.1, 7.1, 10.1 },
                new double[] { 0.0, 0.2, 0.2, 0.2, 0.4, 0.4, 0.4, 0.6, 0.8, 1.0 }, }, };
    }

    @Test
    @Parameters(method = "getTestDownsamplingParameters")
    public void testDownsampling(HybridThresholdingModel model, double[] scores, double[] expectedGrades) {
        double[] actualGrades = new double[scores.length];
        for (int i = 0; i < actualGrades.length; i++)
            actualGrades[i] = model.grade(scores[i]);

        final double delta = 0.001;
        assertArrayEquals(expectedGrades, actualGrades, delta);
    }
}
