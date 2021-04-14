/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.util;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.opensearch.OpenSearchException;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.common.io.stream.NotSerializableExceptionWrapper;

import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.common.exception.ResourceNotFoundException;

public class ExceptionUtil {
    public static final String RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE = OpenSearchException
        .getExceptionName(new ResourceNotFoundException("", ""));

    /**
     * OpenSearch restricts the kind of exceptions can be thrown over the wire
     * (See OpenSearchException.OpenSearchExceptionHandle). Since we cannot
     * add our own exception like ResourceNotFoundException without modifying
     * OpenSearch's code, we have to unwrap the remote transport exception and
     * check its root cause message.
     *
     * @param exception exception thrown locally or over the wire
     * @param expected  expected root cause
     * @param expectedExceptionName expected exception name
     * @return whether the exception wraps the expected exception as the cause
     */
    public static boolean isException(Throwable exception, Class<? extends Exception> expected, String expectedExceptionName) {
        if (exception == null) {
            return false;
        }

        if (expected.isAssignableFrom(exception.getClass())) {
            return true;
        }

        // all exception that has not been registered to sent over wire can be wrapped
        // inside NotSerializableExceptionWrapper.
        // see StreamOutput.writeException
        // OpenSearchException.getExceptionName(exception) returns exception
        // separated by underscore. For example, ResourceNotFoundException is converted
        // to "resource_not_found_exception".
        if (exception instanceof NotSerializableExceptionWrapper && exception.getMessage().trim().startsWith(expectedExceptionName)) {
            return true;
        }
        return false;
    }

    /**
     * Get failure of all shards.
     *
     * @param response index response
     * @return composite failures of all shards
     */
    public static String getShardsFailure(IndexResponse response) {
        StringBuilder failureReasons = new StringBuilder();
        if (response.getShardInfo() != null && response.getShardInfo().getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : response.getShardInfo().getFailures()) {
                failureReasons.append(failure.reason());
            }
            return failureReasons.toString();
        }
        return null;
    }

    /**
     * Count exception in AD failure stats of not.
     *
     * @param e exception
     * @return true if should count in AD failure stats; otherwise return false
     */
    public static boolean countInStats(Exception e) {
        if (!(e instanceof AnomalyDetectionException) || ((AnomalyDetectionException) e).isCountedInStats()) {
            return true;
        }
        return false;
    }

    /**
     * Get error message from exception.
     *
     * @param e exception
     * @return readable error message or full stack trace
     */
    public static String getErrorMessage(Exception e) {
        if (e instanceof IllegalArgumentException || e instanceof AnomalyDetectionException) {
            return e.getMessage();
        } else if (e instanceof OpenSearchException) {
            return ((OpenSearchException) e).getDetailedMessage();
        } else {
            return ExceptionUtils.getFullStackTrace(e);
        }
    }
}
