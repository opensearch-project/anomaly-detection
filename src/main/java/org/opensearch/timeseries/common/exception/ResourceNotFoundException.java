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
 * This exception is thrown when a resource is not found.
 */
public class ResourceNotFoundException extends TimeSeriesException {

    /**
     * Constructor with a config ID and a message.
     *
     * @param configId ID of the config related to the resource
     * @param message explains which resource is not found
     */
    public ResourceNotFoundException(String configId, String message) {
        super(configId, message);
        countedInStats(false);
    }

    public ResourceNotFoundException(String message) {
        super(message);
        countedInStats(false);
    }
}
