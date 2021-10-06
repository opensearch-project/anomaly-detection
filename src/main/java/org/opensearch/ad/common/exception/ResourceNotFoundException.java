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

package org.opensearch.ad.common.exception;

/**
 * This exception is thrown when a resource is not found.
 */
public class ResourceNotFoundException extends AnomalyDetectionException {

    /**
     * Constructor with an anomaly detector ID and a message.
     *
     * @param detectorId ID of the detector related to the resource
     * @param message explains which resource is not found
     */
    public ResourceNotFoundException(String detectorId, String message) {
        super(detectorId, message);
        countedInStats(false);
    }

    public ResourceNotFoundException(String message) {
        super(message);
        countedInStats(false);
    }
}
