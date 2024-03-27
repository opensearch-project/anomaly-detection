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
 * All exception visible to transport layer's client is under ClientException.
 */
public class ClientException extends TimeSeriesException {

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String configId, String message) {
        super(configId, message);
    }

    public ClientException(String configId, String message, Throwable throwable) {
        super(configId, message, throwable);
    }

    public ClientException(String configId, Throwable cause) {
        super(configId, cause);
    }
}
