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

import lombok.Data;

import com.amazon.randomcutforest.returntypes.DiVector;

@Data
public class AnomalyDescriptor {

    // anomalies should have score for postprocessing
    private double rcfScore;

    // the following describes the grade of the anomaly in the range [0:1] where
    // 0 is not an anomaly
    private double anomalyGrade;

    // same for attribution; this is basic RCF attribution which has high/low information
    private DiVector attribution;

    // timestamp (basically a sequence index); kept as long for potential future use
    private long timeStamp;

    // number of trees in the forest
    private int forestSize;

    // flag indicating of expected values are present -- one reason for them not being present
    // is that forecasting can requires more values than anomaly detection,
    private boolean expectedValuesPresent;

    // flag indicating if the anomaly is the start of an anomaly or part of a run of anomalies
    private boolean startOfAnomaly;

    /**
     * position of the anomaly vis a vis the current time (can be -ve) if anomaly is detected late, which can
     * and should happen sometime; for shingle size 1; this is always 0
     * */
    private int relativeIndex;

    // a flattened version denoting the basic contribution of each input variable (not shingled) for the
    // time slice indicated by relativeIndex
    private double[] flattenedAttribution;

    // current values
    private double[] currentValues;

    // the values being replaced; may correspond to past
    private double[] oldValues;

    private double[][] expectedValuesList;

    private double[] likelihoodOfValues;
}
