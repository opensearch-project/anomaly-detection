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

package org.opensearch.ad.task;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.opensearch.ad.ADUnitTestCase;

public class ADHCBatchTaskRunStateTests extends ADUnitTestCase {

    private ADHCBatchTaskRunState taskRunState;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        taskRunState = new ADHCBatchTaskRunState();
    }

    public void testExpiredForCancel() {
        taskRunState.setHistoricalAnalysisCancelled(true);
        // not expired if cancelled time is null
        assertFalse(taskRunState.expired());

        // expired if cancelled time is 10 minute ago, default time out is 60s
        taskRunState.setCancelledTimeInMillis(Instant.now().minus(10, ChronoUnit.MINUTES).toEpochMilli());
        assertTrue(taskRunState.expired());

        // not expired if cancelled time is 10 seconds ago, default time out is 60s
        taskRunState.setCancelledTimeInMillis(Instant.now().minus(10, ChronoUnit.SECONDS).toEpochMilli());
        assertFalse(taskRunState.expired());
    }

    public void testExpiredForNotCancelled() {
        taskRunState.setHistoricalAnalysisCancelled(false);
        // not expired if last task run time is null
        assertFalse(taskRunState.expired());

        // expired if last task run time is 10 minute ago, default time out is 60s
        taskRunState.setLastTaskRunTimeInMillis(Instant.now().minus(10, ChronoUnit.MINUTES).toEpochMilli());
        assertTrue(taskRunState.expired());

        // not expired if task run time is 10 seconds ago, default time out is 60s
        taskRunState.setLastTaskRunTimeInMillis(Instant.now().minus(10, ChronoUnit.SECONDS).toEpochMilli());
        assertFalse(taskRunState.expired());
    }
}
