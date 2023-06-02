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

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.common.exception.TaskCancelledException;

public class ADTaskCancelledExceptionTests extends OpenSearchTestCase {

    public void testConstructor() {
        String message = randomAlphaOfLength(5);
        String user = randomAlphaOfLength(5);
        TaskCancelledException exception = new TaskCancelledException(message, user);
        assertEquals(message, exception.getMessage());
        assertEquals(user, exception.getCancelledBy());
    }
}
