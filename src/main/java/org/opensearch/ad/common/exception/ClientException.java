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
 * All exception visible to AD transport layer's client is under ClientException.
 */
public class ClientException extends AnomalyDetectionException {

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String anomalyDetectorId, String message) {
        super(anomalyDetectorId, message);
    }

    public ClientException(String anomalyDetectorId, String message, Throwable throwable) {
        super(anomalyDetectorId, message, throwable);
    }

    public ClientException(String anomalyDetectorId, Throwable cause) {
        super(anomalyDetectorId, cause);
    }
}
