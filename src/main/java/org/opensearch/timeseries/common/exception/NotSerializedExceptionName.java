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

import static org.opensearch.OpenSearchException.getExceptionName;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.core.common.io.stream.NotSerializableExceptionWrapper;

/**
 * OpenSearch restricts the kind of exceptions that can be thrown over the wire
 * (Read OpenSearchException.OpenSearchExceptionHandle https://tinyurl.com/wv6c6t7x).
 * Since we cannot add our own exception like ResourceNotFoundException without modifying
 * OpenSearch's code, we have to unwrap the NotSerializableExceptionWrapper and
 * check its root cause message.
 *
 */
public enum NotSerializedExceptionName {

    RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new ResourceNotFoundException("", ""))),
    LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new LimitExceededException("", "", false))),
    END_RUN_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new EndRunException("", "", false))),
    TIME_SERIES_DETECTION_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new TimeSeriesException("", ""))),
    INTERNAL_FAILURE_NAME_UNDERSCORE(getExceptionName(new InternalFailure("", ""))),
    CLIENT_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new ClientException("", ""))),
    CANCELLATION_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new TaskCancelledException("", ""))),
    DUPLICATE_TASK_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new DuplicateTaskException(""))),
    VERSION_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new VersionException(""))),
    VALIDATION_EXCEPTION_NAME_UNDERSCORE(getExceptionName(new ValidationException("", null, null)));

    private static final Logger LOG = LogManager.getLogger(NotSerializedExceptionName.class);
    private final String name;

    NotSerializedExceptionName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Convert from a NotSerializableExceptionWrapper to a TimeSeriesException.
     * Since NotSerializableExceptionWrapper does not keep some details we need, we
     * initialize the exception with default values.
     * @param exception an NotSerializableExceptionWrapper exception.
     * @param configID Config Id.
     * @return converted TimeSeriesException
     */
    public static Optional<TimeSeriesException> convertWrappedTimeSeriesException(
        NotSerializableExceptionWrapper exception,
        String configID
    ) {
        String exceptionMsg = exception.getMessage().trim();

        TimeSeriesException convertedException = null;
        for (NotSerializedExceptionName timeseriesException : values()) {
            if (exceptionMsg.startsWith(timeseriesException.getName())) {
                switch (timeseriesException) {
                    case RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new ResourceNotFoundException(configID, exceptionMsg);
                        break;
                    case LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new LimitExceededException(configID, exceptionMsg, false);
                        break;
                    case END_RUN_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new EndRunException(configID, exceptionMsg, false);
                        break;
                    case TIME_SERIES_DETECTION_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new TimeSeriesException(configID, exceptionMsg);
                        break;
                    case INTERNAL_FAILURE_NAME_UNDERSCORE:
                        convertedException = new InternalFailure(configID, exceptionMsg);
                        break;
                    case CLIENT_EXCEPTION_NAME_UNDERSCORE:
                        convertedException = new ClientException(configID, exceptionMsg);
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
                        LOG.warn(new ParameterizedMessage("Unexpected exception {}", timeseriesException));
                        break;
                }
            }
        }
        return Optional.ofNullable(convertedException);
    }
}
