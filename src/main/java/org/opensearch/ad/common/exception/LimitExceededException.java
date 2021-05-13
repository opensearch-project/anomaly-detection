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

package org.opensearch.ad.common.exception;

/**
 * This exception is thrown when a user/system limit is exceeded.
 */
public class LimitExceededException extends EndRunException {

    /**
     * Constructor with an anomaly detector ID and an explanation.
     *
     * @param anomalyDetectorId ID of the anomaly detector for which the limit is exceeded
     * @param message explanation for the limit
     */
    public LimitExceededException(String anomalyDetectorId, String message) {
        super(anomalyDetectorId, message, true);
        this.countedInStats(false);
    }

    /**
     * Constructor with error message.
     *
     * @param message explanation for the limit
     */
    public LimitExceededException(String message) {
        super(message, true);
    }

    /**
     * Constructor with error message.
     *
     * @param message explanation for the limit
     * @param endRun end detector run or not
     */
    public LimitExceededException(String message, boolean endRun) {
        super(null, message, endRun);
    }

    /**
     * Constructor with an anomaly detector ID and an explanation, and a flag for stopping.
     *
     * @param anomalyDetectorId ID of the anomaly detector for which the limit is exceeded
     * @param message explanation for the limit
     * @param stopNow whether to stop detector immediately
     */
    public LimitExceededException(String anomalyDetectorId, String message, boolean stopNow) {
        super(anomalyDetectorId, message, stopNow);
        this.countedInStats(false);
    }
}
