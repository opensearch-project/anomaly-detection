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
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.AbstractTimeSeriesTest;
import org.opensearch.timeseries.cluster.diskcleanup.BaseModelCheckpointIndexRetention;
import org.opensearch.timeseries.cluster.diskcleanup.IndexCleanup;

public class ModelCheckpointIndexRetentionTests extends AbstractTimeSeriesTest {

    Duration defaultCheckpointTtl = Duration.ofDays(3);

    Clock clock = Clock.systemUTC();

    @Mock
    IndexCleanup indexCleanup;

    BaseModelCheckpointIndexRetention modelCheckpointIndexRetention;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        super.setUp();
        super.setUpLog4jForJUnit(IndexCleanup.class);
        MockitoAnnotations.initMocks(this);
        modelCheckpointIndexRetention = new BaseModelCheckpointIndexRetention(
            defaultCheckpointTtl,
            clock,
            indexCleanup,
            ADIndex.CHECKPOINT.getIndexName()
        );
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
        })
            .when(indexCleanup)
            .deleteDocsBasedOnShardSize(eq(ADCommonName.CHECKPOINT_INDEX_NAME), eq(50 * 1024 * 1024 * 1024L), any(), any());

        modelCheckpointIndexRetention.run();
        verify(indexCleanup, times(2))
            .deleteDocsBasedOnShardSize(eq(ADCommonName.CHECKPOINT_INDEX_NAME), eq(50 * 1024 * 1024 * 1024L), any(), any());
        verify(indexCleanup).deleteDocsByQuery(eq(ADCommonName.CHECKPOINT_INDEX_NAME), any(), any());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRunWithCleanupAsFalse() throws Exception {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Boolean> listener = (ActionListener<Boolean>) args[3];
            listener.onResponse(false);
            return null;
        })
            .when(indexCleanup)
            .deleteDocsBasedOnShardSize(eq(ADCommonName.CHECKPOINT_INDEX_NAME), eq(50 * 1024 * 1024 * 1024L), any(), any());

        modelCheckpointIndexRetention.run();
        verify(indexCleanup).deleteDocsBasedOnShardSize(eq(ADCommonName.CHECKPOINT_INDEX_NAME), eq(50 * 1024 * 1024 * 1024L), any(), any());
        verify(indexCleanup).deleteDocsByQuery(eq(ADCommonName.CHECKPOINT_INDEX_NAME), any(), any());
    }
}
