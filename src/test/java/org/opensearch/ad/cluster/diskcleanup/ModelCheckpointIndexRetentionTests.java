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

package org.opensearch.ad.cluster.diskcleanup;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.time.Clock;
import java.time.Duration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.constant.CommonName;

public class ModelCheckpointIndexRetentionTests extends AbstractADTest {

    Duration defaultCheckpointTtl = Duration.ofDays(3);

    Clock clock = Clock.systemUTC();

    @Mock
    IndexCleanup indexCleanup;

    ModelCheckpointIndexRetention modelCheckpointIndexRetention;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(IndexCleanup.class);
        MockitoAnnotations.initMocks(this);
        modelCheckpointIndexRetention = new ModelCheckpointIndexRetention(defaultCheckpointTtl, clock, indexCleanup);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Long> listener = (ActionListener<Long>) args[2];
            listener.onResponse(1L);
            return null;
        }).when(indexCleanup).deleteDocsByQuery(anyString(), any(), any());
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        super.tearDownLog4jForJUnit();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRunWithCleanupAsNeeded() throws Exception {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Boolean> listener = (ActionListener<Boolean>) args[3];
            listener.onResponse(true);
            return null;
        }).when(indexCleanup).deleteDocsBasedOnShardSize(eq(CommonName.CHECKPOINT_INDEX_NAME), eq(50 * 1024 * 1024 * 1024L), any(), any());

        modelCheckpointIndexRetention.run();
        verify(indexCleanup, times(2))
            .deleteDocsBasedOnShardSize(eq(CommonName.CHECKPOINT_INDEX_NAME), eq(50 * 1024 * 1024 * 1024L), any(), any());
        verify(indexCleanup).deleteDocsByQuery(eq(CommonName.CHECKPOINT_INDEX_NAME), any(), any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRunWithCleanupAsFalse() throws Exception {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Boolean> listener = (ActionListener<Boolean>) args[3];
            listener.onResponse(false);
            return null;
        }).when(indexCleanup).deleteDocsBasedOnShardSize(eq(CommonName.CHECKPOINT_INDEX_NAME), eq(50 * 1024 * 1024 * 1024L), any(), any());

        modelCheckpointIndexRetention.run();
        verify(indexCleanup).deleteDocsBasedOnShardSize(eq(CommonName.CHECKPOINT_INDEX_NAME), eq(50 * 1024 * 1024 * 1024L), any(), any());
        verify(indexCleanup).deleteDocsByQuery(eq(CommonName.CHECKPOINT_INDEX_NAME), any(), any());
    }
}
