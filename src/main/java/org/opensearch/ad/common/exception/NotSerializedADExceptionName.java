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

import static org.opensearch.OpenSearchException.getExceptionName;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.io.stream.NotSerializableExceptionWrapper;

/**
 * OpenSearch restricts the kind of exceptions that can be thrown over the wire
 * (Read OpenSearchException.OpenSearchExceptionHandle https://tinyurl.com/wv6c6t7x).
 * Since we cannot add our own exception like ResourceNotFoundException without modifying
 * OpenSearch's code, we have to unwrap the NotSerializableExceptionWrapper and
 * check its root cause message.
 *
 */
public enum NotSerializedADExceptionName {

    RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new ResourceNotFoundException("", ""))),
    LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new LimitExceededException("", "", false))),
    END_RUN_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new EndRunException("", "", false))),
    ANOMALY_DETECTION_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new AnomalyDetectionException("", ""))),
    INTERNAL_FAILURE_NAME_UNDERSCORE(getExceptionName(new InternalFailure("", ""))),
    CLIENT_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new ClientException("", ""))),
    CANCELLATION_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new ADTaskCancelledException("", ""))),
    DUPLICATE_TASK_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new DuplicateTaskException(""))),
    AD_VERSION_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new ADVersionException(""))),
    AD_VALIDATION_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new ADValidationException("", null, null)));

    private static final Logger LOG = LogManager.getLogger(NotSerializedADExceptionName.class);
    private final String name;

    NotSerializedADExceptionName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Convert from a NotSerializableExceptionWrapper to an AnomalyDetectionException.
     * Since NotSerializableExceptionWrapper does not keep some details we need, we
     * initialize the exception with default values.
     * @param exception an NotSerializableExceptionWrapper exception.
     * @param adID Detector Id.
     * @return converted AnomalyDetectionException
     */
    public static Optional<AnomalyDetectionException> convertWrappedAnomalyDetectionException(
        NotSerializableExceptionWrapper exception,
        String adID
    ) {
        String exceptionMsg = exception.getMessage().trim();

        AnomalyDetectionException convertedException = null;
        for (NotSerializedADExceptionName adException : values()) {
            if (exceptionMsg.startsWith(adException.getName())) {
                switch (adException) {
                    case RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new ResourceNotFoundException(adID, exceptionMsg);
                        break;
                    case LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new LimitExceededException(adID, exceptionMsg, false);
                        break;
                    case END_RUN_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new EndRunException(adID, exceptionMsg, false);
                        break;
                    case ANOMALY_DETECTION_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new AnomalyDetectionException(adID, exceptionMsg);
                        break;
                    case INTERNAL_FAILURE_NAME_UNDERSCORE:
                        convertedException = new InternalFailure(adID, exceptionMsg);
                        break;
                    case CLIENT_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new ClientException(adID, exceptionMsg);
                        break;
                    case CANCELLATION_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new ADTaskCancelledException(exceptionMsg, "");
                        break;
                    case DUPLICATE_TASK_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new DuplicateTaskException(exceptionMsg);
                        break;
                    case AD_VERSION_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new ADVersionException(exceptionMsg);
                        break;
                    case AD_VALIDATION_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new ADValidationException(exceptionMsg, null, null);
                        break;
                    default:
                        LOG.warn(new ParameterizedMessage("Unexpected AD exception {}", adException));
                        break;
                }
            }
        }
        return Optional.ofNullable(convertedException);
    }
}
