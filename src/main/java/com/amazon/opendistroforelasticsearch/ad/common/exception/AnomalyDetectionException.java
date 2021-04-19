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

package com.amazon.opendistroforelasticsearch.ad.common.exception;

/**
 * Base exception for exceptions thrown from Anomaly Detection.
 */
public class AnomalyDetectionException extends RuntimeException {

    private String anomalyDetectorId;
    // countedInStats will be used to tell whether the exception should be
    // counted in failure stats.
    private boolean countedInStats = true;

    public AnomalyDetectionException(String message) {
        super(message);
    }

    /**
     * Constructor with an anomaly detector ID and a message.
     *
     * @param anomalyDetectorId anomaly detector ID
     * @param message message of the exception
     */
    public AnomalyDetectionException(String anomalyDetectorId, String message) {
        super(message);
        this.anomalyDetectorId = anomalyDetectorId;
    }

    public AnomalyDetectionException(String adID, String message, Throwable cause) {
        super(message, cause);
        this.anomalyDetectorId = adID;
    }

    public AnomalyDetectionException(Throwable cause) {
        super(cause);
    }

    public AnomalyDetectionException(String adID, Throwable cause) {
        super(cause);
        this.anomalyDetectorId = adID;
    }

    /**
     * Returns the ID of the anomaly detector.
     *
     * @return anomaly detector ID
     */
    public String getAnomalyDetectorId() {
        return this.anomalyDetectorId;
    }

    /**
     * Returns if the exception should be counted in stats.
     *
     * @return true if should count the exception in stats; otherwise return false
     */
    public boolean isCountedInStats() {
        return countedInStats;
    }

    /**
     * Set if the exception should be counted in stats.
     *
     * @param countInStats count the exception in stats
     * @return the exception itself
     */
    public AnomalyDetectionException countedInStats(boolean countInStats) {
        this.countedInStats = countInStats;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Anomaly Detector ");
        sb.append(anomalyDetectorId);
        sb.append(' ');
        sb.append(super.toString());
        return sb.toString();
    }
}
