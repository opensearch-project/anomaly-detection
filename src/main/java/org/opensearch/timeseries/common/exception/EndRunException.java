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
 * Exception for failures that might impact the customer.
 *
 */
public class EndRunException extends ClientException {
    private boolean endNow;

    public EndRunException(String message, boolean endNow) {
        super(message);
        this.endNow = endNow;
    }

    public EndRunException(String configId, String message, boolean endNow) {
        super(configId, message);
        this.endNow = endNow;
    }

    public EndRunException(String configId, String message, Throwable throwable, boolean endNow) {
        super(configId, message, throwable);
        this.endNow = endNow;
    }

    /**
     * @return true for "unrecoverable issue". We want to terminate the detector run immediately.
     *         false for "maybe unrecoverable issue but worth retrying a few more times." We want
     *          to wait for a few more times on different requests before terminating the detector run.
     */
    public boolean isEndNow() {
        return endNow;
    }
}
