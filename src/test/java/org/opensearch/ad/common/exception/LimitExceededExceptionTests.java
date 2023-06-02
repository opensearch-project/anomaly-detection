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

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.opensearch.timeseries.common.exception.LimitExceededException;

public class LimitExceededExceptionTests {

    @Test
    public void testConstructorWithIdAndExplanation() {
        String id = "test id";
        String message = "test message";
        LimitExceededException limitExceeded = new LimitExceededException(id, message);
        assertEquals(id, limitExceeded.getAnomalyDetectorId());
        assertEquals(message, limitExceeded.getMessage());
    }
}
