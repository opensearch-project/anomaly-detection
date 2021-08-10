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
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.randomcutforest.ERCF;

import static com.amazon.randomcutforest.CommonUtils.checkArgument;

import java.util.Arrays;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.returntypes.DiVector;
import com.amazon.randomcutforest.threshold.BasicThresholder;
import com.amazon.randomcutforest.threshold.CorrectorThresholder;

public class ExtendedRandomCutForest {

    public static int MINIMUM_OBSERVATIONS_FOR_EXPECTED = 100;

    private CorrectorThresholder correctorThresholder;

    private RandomCutForest forest;

    private int count = 0;

    private int baseDimensions;

    private int shingleSize;

    // for anomaly description we would only look at these may top attributors
    // note that expected value is not well defined when this number is greater than 1
    private int numberOfAttributors = 2;

    // the number of scores we should see before vending judgements of anomaly/anomaly
    // more is better, the setting below is an optimistic setting
    private int minimumScores = 10;

    // the score below which we will ignore anomalies; there can be examples where the setting of 1.0
    // is large -- but the setting below (and increasing it) should reduce the number of anomalies flagged.
    private double lowerThreshold = 1.0;

    // used for filtering shingles
    private double[] lastAnomalyPoint;

    private int lastAnomalyTimeStamp;

    public ExtendedRandomCutForest(RandomCutForest.Builder<?> builder, double anomalyRate) {
        forest = new RandomCutForest(builder);
        checkArgument(!forest.isInternalShinglingEnabled(), "internal shingling not suported");
        baseDimensions = forest.getDimensions() / forest.getShingleSize();
        shingleSize = forest.getShingleSize();
        if (forest.getDimensions() <= 5) {
            lowerThreshold = 1.1;
        }
        correctorThresholder = new CorrectorThresholder(
            anomalyRate,
            baseDimensions,
            shingleSize,
            true,
            lowerThreshold,
            minimumScores,
            false
        );
    }

    public ExtendedRandomCutForest(
        RandomCutForest forest,
        CorrectorThresholder thresholder,
        int count,
        int numberOfAttributors,
        double[] lastAnomalyPoint,
        int lastAnomalyTimeStamp
    ) {
        checkArgument(!forest.isInternalShinglingEnabled(), "internal shingling not suported");
        this.forest = forest;
        baseDimensions = forest.getDimensions() / forest.getShingleSize();
        shingleSize = forest.getShingleSize();
        this.correctorThresholder = thresholder;
        this.numberOfAttributors = numberOfAttributors;
        this.count = count;
        this.lastAnomalyTimeStamp = lastAnomalyTimeStamp;
        this.lastAnomalyPoint = (lastAnomalyPoint == null) ? null : Arrays.copyOf(lastAnomalyPoint, lastAnomalyPoint.length);
    }

    public AnomalyDescriptor process(double[] point) {
        AnomalyDescriptor result = new AnomalyDescriptor();
        result.setRcfScore(forest.getAnomalyScore(point));
        result.setTimeStamp(count);
        result.setForestSize(forest.getNumberOfTrees());
        result.setAttribution(forest.getAnomalyAttribution(point));
        result.setCurrentValues(new double[baseDimensions]);
        int startPosition = (shingleSize - 1) * baseDimensions;
        boolean reasonableForecast = (count > MINIMUM_OBSERVATIONS_FOR_EXPECTED) && (shingleSize * baseDimensions >= 4);

        for (int i = 0; i < baseDimensions; i++) {
            result.getCurrentValues()[i] = point[startPosition + i];
        }

        if (result.getRcfScore() <= 0 || correctorThresholder.process(result.getRcfScore(), count) != BasicThresholder.MORE_INFORMATION) {
            result.setAnomalyGrade(0);
            ++count;
            forest.update(point);
            return result;
        }

        // note that MORE_INFORMATION would not process the current score yet

        /**
         * we consider what the most recent values should have been, reflected in newPoint;
         * and then pass the score, score of newPoint, attribution and attribution of newPoint
         * to the thresholder. The idea is that "if the most likely least anomalous score is high"
         * (reflected in forest.getAnomalyScore(newPoint) then the most recent observations are not
         * an anomaly. If the still are considered an anomaly, then we look at the most egregious
         * subobservations in the shingle, given by maxContribution() and predict those values
         * -- note that this may correspond to anomalies being detecting late; and deciding on the
         * values when we detect anomalies (based on what we know now, as opposed to pure forecasting)
         *
         * The parameter CONFIG_NUMBER_OF_ATTRIBUTORS determines the maximum number of different attributors
         * we could consider; note that larger number of contributors are difficult to visualize/control
         */

        if (reasonableForecast && lastAnomalyPoint != null && count - lastAnomalyTimeStamp < shingleSize) {
            double[] correctedPoint = Arrays.copyOf(point, point.length);
            for (int i = 0; i < point.length - (count - lastAnomalyTimeStamp) * baseDimensions; i++) {
                correctedPoint[i] = lastAnomalyPoint[i + (count - lastAnomalyTimeStamp) * baseDimensions];
            }
            double correctedScore = forest.getAnomalyScore(correctedPoint);
            if (!correctorThresholder.isPotentialAnomaly(correctedScore)) {
                // fixing the past makes this anomaly go away; nothing to do but process the score
                correctorThresholder.process(result.getRcfScore(), correctedScore, result.getAttribution(), null, count);
                result.setAnomalyGrade(0);
                ++count;
                forest.update(point);
                return result;
            }
        }

        double[] newPoint = null;
        double newScore = 0;
        DiVector newAttribution = null;
        if (reasonableForecast) {
            int[] likelyMissingIndices = largestFeatures(result.getAttribution(), startPosition, baseDimensions, numberOfAttributors);
            newPoint = forest.imputeMissingValues(point, likelyMissingIndices.length, likelyMissingIndices);
            newAttribution = forest.getAnomalyAttribution(newPoint);
            newScore = forest.getAnomalyScore(newPoint);
        }

        int index = maxContribution(result.getAttribution(), baseDimensions, -shingleSize) + 1;
        boolean inAnomaly = correctorThresholder.isInAnomaly();
        int signal = correctorThresholder.process(result.getRcfScore(), newScore, result.getAttribution(), newAttribution, count);

        if (signal > 0) {
            result.setAnomalyGrade(0.01 * signal);
            result.setRelativeIndex(index);
            result.setStartOfAnomaly(!inAnomaly);
            result.setExpectedValuesPresent((count > MINIMUM_OBSERVATIONS_FOR_EXPECTED) && (shingleSize * baseDimensions >= 4));
            if (result.getRelativeIndex() < 0 && result.isStartOfAnomaly()) {
                // anomaly in the past and detected late
                startPosition = result.getAttribution().getDimensions() + (result.getRelativeIndex() - 1) * baseDimensions;
                if (result.isExpectedValuesPresent()) {
                    int[] missingIndices = largestFeatures(result.getAttribution(), startPosition, baseDimensions, numberOfAttributors);
                    newPoint = forest.imputeMissingValues(point, missingIndices.length, missingIndices);
                    result.setOldValues(new double[baseDimensions]);
                    for (int i = 0; i < baseDimensions; i++) {
                        result.getOldValues()[i] = point[startPosition + i];
                    }
                }
            }
            if (result.isExpectedValuesPresent()) {
                result.setExpectedValuesList(new double[1][]);
                result.getExpectedValuesList()[0] = new double[baseDimensions];
                for (int i = 0; i < baseDimensions; i++) {
                    result.getExpectedValuesList()[0][i] = newPoint[startPosition + i];
                }
                result.setLikelihoodOfValues(new double[] { 1.0 });
                lastAnomalyPoint = newPoint;
                lastAnomalyTimeStamp = count;
            }
            result.setFlattenedAttribution(new double[baseDimensions]);
            for (int i = 0; i < baseDimensions; i++) {
                result.getFlattenedAttribution()[i] = result.getAttribution().getHighLowSum(startPosition + i);
            }

        } else {
            result.setAnomalyGrade(0);
        }

        ++count;
        forest.update(point);
        return result;

    }

    private int maxContribution(DiVector diVector, int baseDimension, int startIndex) {
        double val = 0;
        int index = startIndex;
        int position = diVector.getDimensions() + startIndex * baseDimension;
        for (int i = 0; i < baseDimension; i++) {
            val += diVector.getHighLowSum(i + position);
        }
        for (int i = position + baseDimension; i < diVector.getDimensions(); i += baseDimension) {
            double sum = 0;
            for (int j = 0; j < baseDimension; j++) {
                sum += diVector.getHighLowSum(i + j);
            }
            if (sum > val) {
                val = sum;
                index = (i - diVector.getDimensions()) / baseDimension;
            }
        }
        return index;
    }

    private int[] largestFeatures(DiVector diVector, int position, int baseDimension, int max_number) {
        if (baseDimension == 1) {
            return new int[] { position };
        }
        double sum = 0;
        double[] values = new double[baseDimension];
        for (int i = 0; i < baseDimension; i++) {
            sum += values[i] = diVector.getHighLowSum(i + position);
        }
        Arrays.sort(values);
        double cutoff = values[baseDimension - Math.min(max_number, baseDimension)];
        int[] answer = new int[Math.min(max_number, baseDimension)];
        int count = 0;
        for (int i = 0; i < baseDimension; i++) {
            if (diVector.getHighLowSum(i + position) >= cutoff && diVector.getHighLowSum(i + position) > sum * 0.1) {
                answer[count++] = position + i;
            }
        }
        return Arrays.copyOf(answer, count);
    }

    public RandomCutForest getForest() {
        return forest;
    }

    public CorrectorThresholder getCorrectorThresholder() {
        return correctorThresholder;
    }

    public int getCount() {
        return count;
    }

    public int getNumberOfAttributors() {
        return numberOfAttributors;
    }

    public double[] getLastAnomalyPoint() {
        return (lastAnomalyPoint == null) ? null : Arrays.copyOf(lastAnomalyPoint, lastAnomalyPoint.length);
    }

    public int getLastAnomalyTimeStamp() {
        return lastAnomalyTimeStamp;
    }
}
