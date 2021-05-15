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

package org.opensearch.ad.cluster;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;

public class DailyCronTests extends AbstractADTest {

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
            assertTrue(String.format("The size of args is %d.  Its content is %s", args.length, Arrays.toString(args)), args.length == 3);
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
