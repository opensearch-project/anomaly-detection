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
 * Exception for root cause unknown failure. Maybe transient. Client can continue the detector running.
 *
 */
public class InternalFailure extends ClientException {

    public InternalFailure(String configId, String message) {
        super(configId, message);
    }

    public InternalFailure(String configId, String message, Throwable cause) {
        super(configId, message, cause);
    }

    public InternalFailure(String configId, Throwable cause) {
        super(configId, cause);
    }

    public InternalFailure(TimeSeriesException cause) {
        super(cause.getConfigId(), cause);
    }
}
