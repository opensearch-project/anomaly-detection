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

package org.opensearch.ad.cluster;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;

import org.opensearch.OpenSearchException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.cluster.DailyCron;
import org.opensearch.timeseries.util.ClientUtil;

public class DailyCronTests extends AbstractTimeSeriesTest {

    enum DailyCronTestExecutionMode {
        NORMAL,
        INDEX_NOT_EXIST,
        FAIL
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(DailyCron.class);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
    }

    @SuppressWarnings("unchecked")
    private void templateDailyCron(DailyCronTestExecutionMode mode) {
        Clock clock = mock(Clock.class);
        ClientUtil clientUtil = mock(ClientUtil.class);
        DailyCron cron = new DailyCron(clock, Duration.ofHours(24), clientUtil);

        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assertTrue(
                String.format(Locale.ROOT, "The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)),
                args.length == 3
            );
            assertTrue(args[2] instanceof ActionListener);

            ActionListener<BulkByScrollResponse> listener = (ActionListener<BulkByScrollResponse>) args[2];

            if (mode == DailyCronTestExecutionMode.INDEX_NOT_EXIST) {
                listener.onFailure(new IndexNotFoundException("foo", "bar"));
            } else if (mode == DailyCronTestExecutionMode.FAIL) {
                listener.onFailure(new OpenSearchException("bar"));
            } else {
                BulkByScrollResponse deleteByQueryResponse = mock(BulkByScrollResponse.class);
                when(deleteByQueryResponse.getDeleted()).thenReturn(10L);
                listener.onResponse(deleteByQueryResponse);
            }

            return null;
        }).when(clientUtil).execute(eq(DeleteByQueryAction.INSTANCE), any(), any());

        cron.run();
    }

    public void testNormal() {
        templateDailyCron(DailyCronTestExecutionMode.NORMAL);
        assertTrue(testAppender.containsMessage(DailyCron.CHECKPOINT_DELETED_MSG));
    }

    public void testCheckpointNotExist() {
        templateDailyCron(DailyCronTestExecutionMode.INDEX_NOT_EXIST);
        assertTrue(testAppender.containsMessage(DailyCron.CHECKPOINT_NOT_EXIST_MSG));
    }

    public void testFail() {
        templateDailyCron(DailyCronTestExecutionMode.FAIL);
        assertTrue(testAppender.containsMessage(DailyCron.CANNOT_DELETE_OLD_CHECKPOINT_MSG));
    }
}
