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

import java.util.Optional;

import org.opensearch.core.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.test.OpenSearchTestCase;

public class NotSerializedADExceptionNameTests extends OpenSearchTestCase {
    public void testConvertAnomalyDetectionException() {
        Optional<AnomalyDetectionException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new AnomalyDetectionException("", "")), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof AnomalyDetectionException);
    }

    public void testConvertInternalFailure() {
        Optional<AnomalyDetectionException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new InternalFailure("", "")), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof InternalFailure);
    }

    public void testConvertClientException() {
        Optional<AnomalyDetectionException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new ClientException("", "")), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof ClientException);
    }

    public void testConvertADTaskCancelledException() {
        Optional<AnomalyDetectionException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new ADTaskCancelledException("", "")), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof ADTaskCancelledException);
    }

    public void testConvertDuplicateTaskException() {
        Optional<AnomalyDetectionException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new DuplicateTaskException("")), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof DuplicateTaskException);
    }

    public void testConvertADValidationException() {
        Optional<AnomalyDetectionException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new ADValidationException("", null, null)), "");
        assertTrue(converted.isPresent());
        assertTrue(converted.get() instanceof ADValidationException);
    }

    public void testUnknownException() {
        Optional<AnomalyDetectionException> converted = NotSerializedADExceptionName
            .convertWrappedAnomalyDetectionException(new NotSerializableExceptionWrapper(new RuntimeException("")), "");
        assertTrue(!converted.isPresent());
    }
}
