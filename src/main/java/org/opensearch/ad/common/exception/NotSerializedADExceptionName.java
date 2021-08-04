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

import org.opensearch.OpenSearchException;

public enum NotSerializedADExceptionName {

    RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE(OpenSearchException.getExceptionName(new ResourceNotFoundException("", ""))),
    LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE(OpenSearchException.getExceptionName(new LimitExceededException("", "", false))),
    END_RUN_EXCEPTION_NAME_UNDERSCORE(OpenSearchException.getExceptionName(new EndRunException("", "", false))),
    ANOMALY_DETECTION_EXCEPTION_NAME_UNDERSCORE(OpenSearchException.getExceptionName(new AnomalyDetectionException("", ""))),
    INTERNAL_FAILURE_NAME_UNDERSCORE(OpenSearchException.getExceptionName(new InternalFailure("", ""))),
    CLIENT_EXCEPTION_NAME_UNDERSCORE(OpenSearchException.getExceptionName(new ClientException("", ""))),
    CANCELLATION_EXCEPTION_NAME_UNDERSCORE(OpenSearchException.getExceptionName(new ADTaskCancelledException("", ""))),
    DUPLICATE_TASK_EXCEPTION_NAME_UNDERSCORE(OpenSearchException.getExceptionName(new DuplicateTaskException("")));

    private final String name;

    NotSerializedADExceptionName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
