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

package org.opensearch.timeseries.common.exception;

/**
 * This exception is thrown when a user/system limit is exceeded.
 */
public class LimitExceededException extends EndRunException {

    /**
     * Constructor with a config ID and an explanation.
     *
     * @param id ID of the time series analysis for which the limit is exceeded
     * @param message explanation for the limit
     */
    public LimitExceededException(String id, String message) {
        super(id, message, true);
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
     * Constructor with a config ID and an explanation, and a flag for stopping.
     *
     * @param id ID of the time series analysis for which the limit is exceeded
     * @param message explanation for the limit
     * @param stopNow whether to stop time series analysis immediately
     */
    public LimitExceededException(String id, String message, boolean stopNow) {
        super(id, message, stopNow);
        this.countedInStats(false);
    }
}
