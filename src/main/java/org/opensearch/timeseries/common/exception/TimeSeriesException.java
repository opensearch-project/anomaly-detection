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
 * Base exception for exceptions thrown.
 */
public class TimeSeriesException extends RuntimeException {

    private String configId;
    // countedInStats will be used to tell whether the exception should be
    // counted in failure stats.
    private boolean countedInStats = true;

    public TimeSeriesException(String message) {
        super(message);
    }

    /**
     * Constructor with a config ID and a message.
     *
     * @param configId config ID
     * @param message message of the exception
     */
    public TimeSeriesException(String configId, String message) {
        super(message);
        this.configId = configId;
    }

    public TimeSeriesException(String configID, String message, Throwable cause) {
        super(message, cause);
        this.configId = configID;
    }

    public TimeSeriesException(Throwable cause) {
        super(cause);
    }

    public TimeSeriesException(String configID, Throwable cause) {
        super(cause);
        this.configId = configID;
    }

    /**
     * Returns the ID of the analysis config.
     *
     * @return config ID
     */
    public String getConfigId() {
        return this.configId;
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
    public TimeSeriesException countedInStats(boolean countInStats) {
        this.countedInStats = countInStats;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(configId);
        sb.append(' ');
        sb.append(super.toString());
        return sb.toString();
    }
}
