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

package com.amazon.opendistroforelasticsearch.ad.ml;

/**
 * A model for converting raw anomaly scores into anomaly grades.
 *
 * A thresholding model is trained on a set of raw anomaly scores like those
 * output from the Random Cut Forest algorithm. The fundamental assumption of
 * anomaly scores is that the larger the score the more anomalous the
 * corresponding data point. Based on this training set an internal threshold is
 * computed to determine if a given score is anomalous. The thresholding model
 * can be updated with new anomaly scores such as in a streaming context.
 *
 */
public interface ThresholdingModel {

    /**
     * Initializes the model using a training set of anomaly scores.
     *
     * @param anomalyScores  array of anomaly scores with which to train the model
     */
    void train(double[] anomalyScores);

    /**
     * Update the model with a new anomaly score.
     *
     * @param anomalyScore  an anomaly score
     */
    void update(double anomalyScore);

    /**
     * Computes the anomaly grade associated with the given anomaly score. A
     * non-zero grade implies that the given score is anomalous. The magnitude
     * of the grade, a value between 0 and 1, indicates the severity of the
     * anomaly.
     *
     * @param anomalyScore  an anomaly score
     * @return              the associated anomaly grade
     */
    double grade(double anomalyScore);

    /**
     * Returns the confidence of the model in predicting anomaly grades; that
     * is, the probability that the reported anomaly grade is correct according
     * to the underlying model.
     *
     * @return  the model confidence
     */
    double confidence();
}
