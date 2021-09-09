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

package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.ADTask;
import org.opensearch.test.OpenSearchTestCase;

public class ADBatchAnomalyResultRequestTests extends OpenSearchTestCase {

    public void testInvalidRequestWithNullTaskIdAndDetectionDateRange() throws IOException {
        ADTask adTask = TestHelpers.randomAdTask();
        adTask.setTaskId(null);
        adTask.setDetectionDateRange(null);
        ADBatchAnomalyResultRequest request = new ADBatchAnomalyResultRequest(adTask);
        ActionRequestValidationException exception = request.validate();
        assertEquals(
            "Validation Failed: 1: Task id can't be null;2: Detection date range can't be null for batch task;",
            exception.getMessage()
        );
    }
}
