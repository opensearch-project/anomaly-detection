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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.special.Erf;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.JsonAdapter;
import com.yahoo.sketches.kll.KllFloatsSketch;
import com.yahoo.sketches.kll.KllFloatsSketchIterator;

/**
 * A model for converting raw anomaly scores into anomaly grades.
 *
 * The hybrid thresholding model combines a log-normal distribution model/CDF as
 * well as an empirical model/CDF for determining anomalous scores. The
 * log-normal CDF is used to initialize the empirical CDF. This is done because
 * the training set often does not include anomalies and predictions need to be
 * made as to how often a large anomaly score will occur given the training
 * data. The empirical model uses the technique described in "Optimal Quantile
 * Approximation in Streams" by Karnin, Lang, and Liberty. The KLL model is
 * implemented in {@code KllFloatSketchSerDe}.
 *
 * @see KllFloatsSketchSerDe
 */
public class HybridThresholdingModel implements ThresholdingModel {

    /**
     * The minimum anomaly score for labeling anomalies.
     * Scores below this value result into anomaly grade 0.
     */
    public static final double MIN_SCORE = 0.4;

    private static final boolean USE_DOUBLE_SIDED_ERROR = true;
    private static final double CONFIDENCE = 0.99;

    @Expose
    @JsonAdapter(KllFloatsSketchSerDe.class)
    private KllFloatsSketch quantileSketch;
    private double maxScore;
    private int numLogNormalQuantiles;
    private double minPvalueThreshold;
    private int downsampleNumSamples;
    private long downsampleMaxNumObservations;

    /**
     * Initializes a HybridThresholdingModel.
     *
     * The primary parameters to a HybridThresholdingModel are {@code
     * minPvalueThreshold} and {@code maxRankError}. These two parameters define
     * the p-value threshold at which anomalies are reported and how accurately
     * this threshold can be measured. Note that in order to accurately measure
     * the given minimum p-value threshold the maximum rank error needs to be
     * small enough. In particular, {@code maxRankError} should be less than
     * {@code minPvalueThreshold}.
     *
     * The maximum possible anomaly score is provided so that large anomaly
     * scores can be estimated during training; even when they are not available
     * in the training set. The quantile sketch is initialized using {@code
     * numLogNormalQuantiles} quantile results from fitting a log-normal
     * distribution to the training data.
     *
     * The size of the empirical CDF, as modeled by the KLL algorithm, is
     * determined by the parameter, {@code maxRankError}. When a certain number
     * of updates/observations have been processed, as set by {@code
     * downsampleMaxNumObservations}, the ECDF model will be automatically
     * downsampled to a number of scores equal to {@code downsampleNumSamples}.
     *
     * @param minPvalueThreshold            the p-value threshold beyond which an
     *                                      anomaly score is classified as an
     *                                      panomaly. A value of 0.995 is
     *                                      recommended. (Between: 0 and 1)
     * @param maxRankError                  desired maximum double-sided normalized
     *                                      rank error to use in the quantile
     *                                      sketch approximation. A value of 0.0001
     *                                      is recommended.
     * @param maxScore                      the largest observable anomaly score
     * @param numLogNormalQuantiles         number of quantiles of the log-normal
     *                                      distribution to compute training.
     *                                      (Min: 0)
     * @param downsampleNumSamples          the number of scores to keep when
     *                                      downsampling the model. A value of
     *                                      10_000 is recommended.
     * @param downsampleMaxNumObservations  the threshold number of observations /
     *                                      updates at which the model will be
     *                                      automatically downsampled. A value of
     *                                      1_000_000 is recommended.
     * @throws IllegalArgumentException     if {@code minPvalueThreshold} is not
     *                                      strictly between 0 and 1, if {@code
     *                                      maxRankError} is larger than 1 -
     *                                      {@code minPvalueThreshold}, or if
     *                                      {@code numLogNormalQuantiles}
     *                                      negative
     *
     * @see KllFloatsSketchSerDe
     */
    public HybridThresholdingModel(
        double minPvalueThreshold,
        double maxRankError,
        double maxScore,
        int numLogNormalQuantiles,
        int downsampleNumSamples,
        long downsampleMaxNumObservations
    ) {
        if ((minPvalueThreshold <= 0.0) || (1.0 <= minPvalueThreshold)) {
            throw new IllegalArgumentException("minPvalueThreshold must be strictly between 0 and 1.");
        }
        if (maxRankError > (1.0 - minPvalueThreshold)) {
            throw new IllegalArgumentException(
                "maxRankError must be smaller than 1 - minPvalueThreshold in order to accurately " + "estimate that threshold."
            );
        }
        if (maxRankError <= 0.0) {
            throw new IllegalArgumentException("maxRankError must be positive.");
        }
        if (maxScore <= 0.0) {
            throw new IllegalArgumentException("maxScore must be positive.");
        }
        if (numLogNormalQuantiles < 0) {
            throw new IllegalArgumentException("The maximum number of log-normal quantiles to compute must be non-negative.");
        }
        if (downsampleNumSamples <= 1) {
            throw new IllegalArgumentException("Number of downsamples must be greater than one.");
        }
        if (downsampleNumSamples >= downsampleMaxNumObservations) {
            throw new IllegalArgumentException(
                "The number of samples to downsample to must be less than the number of observations " + "before downsampling is triggered."
            );
        }

        this.minPvalueThreshold = minPvalueThreshold;
        this.quantileSketch = new KllFloatsSketch(KllFloatsSketch.getKFromEpsilon(maxRankError, USE_DOUBLE_SIDED_ERROR));
        this.maxScore = maxScore;
        this.numLogNormalQuantiles = numLogNormalQuantiles;
        this.downsampleNumSamples = downsampleNumSamples;
        this.downsampleMaxNumObservations = downsampleMaxNumObservations;
    }

    /**
     * Empty constructor only for serialization purpose - DO NOT USE.
     *
     * WARNING. All clients should avoid using this constructor
     * for the objects from this constructor have undefined behaviors.
     * This constructor is exclusively used for serialization.
     */
    public HybridThresholdingModel() {}

    /**
     * Returns the minimum p-value threshold for anomaly classification.
     *
     * @return minPvalueThreshold
     */
    public double getMinPvalueThreshold() {
        return minPvalueThreshold;
    }

    /**
     * Returns the approximate double-sided normalized rank error of the quantile sketch.
     *
     * @return MaxRankError
     */
    public double getMaxRankError() {
        return quantileSketch.getNormalizedRankError(USE_DOUBLE_SIDED_ERROR);
    }

    /**
     * Returns the maximum possible anomaly score of the thresholding model.
     *
     * @return maxScore
     */
    public double getMaxScore() {
        return maxScore;
    }

    /**
     * Returns the number of log-normal quantiles used to initialize the
     * quantile sketch.
     *
     * @return numLogNormalQuantiles
     */
    public int getNumLogNormalQuantiles() {
        return numLogNormalQuantiles;
    }

    /**
     * Returns the number of samples to retain when downsampling the model.
     *
     * @return downsampleNumSamples
     */
    public int getDownsampleNumSamples() {
        return downsampleNumSamples;
    }

    /**
     * Returns the number of observations that triggers a model downsampling.
     *
     * @return downsampleMaxNumObservations
     */
    public long getDownsampleMaxNumObservations() {
        return downsampleMaxNumObservations;
    }

    /**
     * Initializes the model using a training set of anomaly scores.
     *
     * The hybrid model initialization has several steps. First, a log-normal
     * distribution is fit to the training set scores. Next, the quantile sketch
     * is initialized with at {@code numLogNormalQuantiles} samples from the
     * log-normal model up to {@code maxScore}.
     *
     * @param anomalyScores  an array of anomaly scores with which to train the model.
     */
    @Override
    public void train(double[] anomalyScores) {
        /*
          We assume the anomaly scores are fit to a log-normal distribution.
          Equivalent to fitting a Gaussian to the logs of the anomaly scores.
        */
        SummaryStatistics stats = new SummaryStatistics();
        for (int i = 0; i < anomalyScores.length; i++) {
            stats.addValue(Math.log(anomalyScores[i]));
        }
        final double mu = stats.getMean();
        final double sigma = stats.getStandardDeviation();

        /*
          Compute the 1/R quantiles for R = `numLogNormalQuantiles` of the
          corresponding log-normal distribution and use these to initialize the
          model. We only compute p-values up to the p-value of the known maximum
          possible score. Finally, we do not compute the p=0.0 quantile because
          raw anomaly scores are positive and non-zero.
        */
        final double maxScorePvalue = computeLogNormalCdf(maxScore, mu, sigma);
        final double pvalueStep = maxScorePvalue / (numLogNormalQuantiles + 1.0);
        for (double pvalue = pvalueStep; pvalue < maxScorePvalue; pvalue += pvalueStep) {
            double currentScore = computeLogNormalQuantile(pvalue, mu, sigma);
            update(currentScore);
        }
    }

    /**
     * The log-normal cumulative distribution function.
     *
     * Given and anomaly score compute the corresponding p-value.
     *
     * @param anomalyScore  an anomaly score
     * @param mu            mean parameter of the log-normal distribution
     * @param sigma         standard deviation of the log-normal distribution
     * @return              the p-value of the input anomaly score
     */
    private double computeLogNormalCdf(double anomalyScore, double mu, double sigma) {
        return (1.0 + Erf.erf((Math.log(anomalyScore) - mu) / (Math.sqrt(2.0) * sigma))) / 2.0;
    }

    /**
     * The log-normal quantile function.
     *
     * Given a p-value and log-normal distribution parameters compute the
     * corresponding anomaly score.
     *
     * @param pvalue  a p-value between 0 and 1
     * @param mu      mean parameter of the log-normal distribution
     * @param sigma   standard deviation of the log-normal distribution
     * @return        anomaly score at the given p-value quantile
     */
    private double computeLogNormalQuantile(double pvalue, double mu, double sigma) {
        return Math.exp(mu + Math.sqrt(2.0) * sigma * Erf.erfInv(2.0 * pvalue - 1.0));
    }

    /**
     * Updates the model with a new anomaly score.
     *
     * Note that once we initialize the hybrid model we only update the
     * empirical CDF. The model is downsampled when the total number of
     * observations/updates exceeds {@code downsampleMaxNumObservations}.
     *
     * @param anomalyScore  an anomaly score.
     * @see                 HybridThresholdingModel
     */
    @Override
    public void update(double anomalyScore) {
        quantileSketch.update((float) anomalyScore);

        long totalNumObservations = quantileSketch.getN();
        if (totalNumObservations >= downsampleMaxNumObservations) {
            downsample();
        }
    }

    /**
     * Computes the anomaly grade associated with the given anomaly score. A
     * non-zero grade implies that the given score is anomalous. The magnitude
     * of the grade, a value between 0 and 1, indicates the severity of the
     * anomaly.
     *
     * @param anomalyScore  an anomaly score
     * @return              the associated anomaly grade
     */
    @Override
    public double grade(double anomalyScore) {
        double anomalyGrade = 0.;
        if (anomalyScore > MIN_SCORE) {
            final double scale = 1.0 / (1.0 - minPvalueThreshold);
            final double pvalue = quantileSketch.getRank((float) anomalyScore);
            anomalyGrade = scale * (pvalue - minPvalueThreshold);
            anomalyGrade = Double.isNaN(anomalyGrade) ? 0. : Math.max(0., anomalyGrade);
        }
        return anomalyGrade;
    }

    /**
     * Returns the confidence of the model in predicting anomaly grades; that
     * is, the probability that the reported anomaly grade is correct according
     * to the underlying model.
     *
     * For the HybridThresholdingModel the model confidence is from underlying Sketch.
     *
     * @return  the model confidence.
     * @see  <a href="https://datasketches.github.io/docs/Quantiles/KLLSketch.html"></a>
     */
    @Override
    public double confidence() {
        return CONFIDENCE;
    }

    /**
     * Replaces the model's ECDF sketch with a downsampled version.
     *
     * Periodic downsampling of the sketch is primarily useful for allowing the
     * model to more easily adapt to changes in the score distribution. This is
     * because, with fewer retained points in the sketch, new scores will have a
     * larger impact on the distribution. A secondary benefit to the
     * downsampling process is to prevent out of memory errors; although the
     * memory requirements grows like log(log(N)) there is the chance of an
     * allocation bug which we wish to mitigate.
     *
     * Uses the initialization parameter {@code downsampleNumSamples}.
     *
     */
    private void downsample() {
        KllFloatsSketch downsampledQuantileSketch = new KllFloatsSketch(quantileSketch.getK());
        double pvalueStep = 1.0 / (downsampleNumSamples - 1.0);
        for (double pvalue = 0.0; pvalue < 1.0; pvalue += pvalueStep) {
            float score = quantileSketch.getQuantile(pvalue);
            downsampledQuantileSketch.update(score);
        }
        downsampledQuantileSketch.update((float) maxScore);
        this.quantileSketch = downsampledQuantileSketch;
    }

    @Override
    public List<Double> extractScores() {
        KllFloatsSketchIterator iter = quantileSketch.iterator();
        List<Double> scores = new ArrayList<>();
        while (iter.next()) {
            scores.add((double) iter.getValue());
        }
        return scores;
    }
}
