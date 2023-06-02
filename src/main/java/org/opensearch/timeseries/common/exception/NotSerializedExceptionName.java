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

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.BaseExceptionsHelper;
import org.opensearch.common.io.stream.NotSerializableExceptionWrapper;

/**
 * OpenSearch restricts the kind of exceptions that can be thrown over the wire
 * (Read OpenSearchException.OpenSearchExceptionHandle https://tinyurl.com/wv6c6t7x).
 * Since we cannot add our own exception like ResourceNotFoundException without modifying
 * OpenSearch's code, we have to unwrap the NotSerializableExceptionWrapper and
 * check its root cause message.
 *
 */
public enum NotSerializedExceptionName {

    RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE(BaseExceptionsHelper.getExceptionName(new ResourceNotFoundException("", ""))),
    LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE(BaseExceptionsHelper.getExceptionName(new LimitExceededException("", "", false))),
    END_RUN_EXCEPTION_NAME_UNDERSCORE(BaseExceptionsHelper.getExceptionName(new EndRunException("", "", false))),
    ANOMALY_DETECTION_EXCEPTION_NAME_UNDERSCORE(BaseExceptionsHelper.getExceptionName(new TimeSeriesException("", ""))),
    INTERNAL_FAILURE_NAME_UNDERSCORE(BaseExceptionsHelper.getExceptionName(new InternalFailure("", ""))),
    CLIENT_EXCEPTION_NAME_UNDERSCORE(BaseExceptionsHelper.getExceptionName(new ClientException("", ""))),
    CANCELLATION_EXCEPTION_NAME_UNDERSCORE(BaseExceptionsHelper.getExceptionName(new TaskCancelledException("", ""))),
    DUPLICATE_TASK_EXCEPTION_NAME_UNDERSCORE(BaseExceptionsHelper.getExceptionName(new DuplicateTaskException(""))),
    VERSION_EXCEPTION_NAME_UNDERSCORE(BaseExceptionsHelper.getExceptionName(new VersionException(""))),
    VALIDATION_EXCEPTION_NAME_UNDERSCORE(BaseExceptionsHelper.getExceptionName(new ValidationException("", null, null)));

    private static final Logger LOG = LogManager.getLogger(NotSerializedExceptionName.class);
    private final String name;

    NotSerializedExceptionName(String name) {
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
    public static Optional<TimeSeriesException> convertWrappedAnomalyDetectionException(
        NotSerializableExceptionWrapper exception,
        String adID
    ) {
        String exceptionMsg = exception.getMessage().trim();

        TimeSeriesException convertedException = null;
        for (NotSerializedExceptionName adException : values()) {
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
                        convertedException = new TimeSeriesException(adID, exceptionMsg);
                        break;
                    case INTERNAL_FAILURE_NAME_UNDERSCORE:
                        convertedException = new InternalFailure(adID, exceptionMsg);
                        break;
                    case CLIENT_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new ClientException(adID, exceptionMsg);
                        break;
                    case CANCELLATION_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new TaskCancelledException(exceptionMsg, "");
                        break;
                    case DUPLICATE_TASK_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new DuplicateTaskException(exceptionMsg);
                        break;
                    case VERSION_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new VersionException(exceptionMsg);
                        break;
                    case VALIDATION_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new ValidationException(exceptionMsg, null, null);
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
