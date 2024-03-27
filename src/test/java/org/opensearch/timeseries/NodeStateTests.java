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

package org.opensearch.timeseries;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.common.exception.TimeSeriesException;

public class NodeStateTests extends OpenSearchTestCase {
    private NodeState state;
    private Clock clock;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clock = mock(Clock.class);
        state = new NodeState("123", clock);
    }

    private Duration duration = Duration.ofHours(1);

    public void testMaintenanceNotRemoveSingle() throws IOException {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1000));
        state.setConfigDef(TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null));

        when(clock.instant()).thenReturn(Instant.MIN);
        assertTrue(!state.expired(duration));
    }

    public void testMaintenanceNotRemove() throws IOException {
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(1000));
        state.setConfigDef(TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null));
        state.setException(null);

        when(clock.instant()).thenReturn(Instant.ofEpochSecond(3700));
        assertTrue(!state.expired(duration));
    }

    public void testMaintenanceRemoveLastError() throws IOException {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1000));
        state
            .setConfigDef(

                TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null)
            );
        state.setException(null);

        when(clock.instant()).thenReturn(Instant.ofEpochSecond(3700));
        assertTrue(state.expired(duration));
    }

    public void testMaintenancRemoveDetector() throws IOException {
        when(clock.instant()).thenReturn(Instant.MIN);
        state.setConfigDef(TestHelpers.randomAnomalyDetector(TestHelpers.randomUiMetadata(), null));
        when(clock.instant()).thenReturn(Instant.MAX);
        assertTrue(state.expired(duration));

    }

    public void testMaintenanceFlagNotRemove() throws IOException {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1000));
        state.setCheckpointExists(true);
        when(clock.instant()).thenReturn(Instant.MIN);
        assertTrue(!state.expired(duration));
    }

    public void testMaintenancFlagRemove() throws IOException {
        when(clock.instant()).thenReturn(Instant.MIN);
        state.setCheckpointExists(true);
        when(clock.instant()).thenReturn(Instant.MIN);
        assertTrue(!state.expired(duration));
    }

    public void testMaintenanceLastColdStartRemoved() {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1000));
        state.setException(new TimeSeriesException("123", ""));
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(3700));
        assertTrue(state.expired(duration));
    }

    public void testMaintenanceLastColdStartNotRemoved() {
        when(clock.instant()).thenReturn(Instant.ofEpochMilli(1_000_000L));
        state.setException(new TimeSeriesException("123", ""));
        when(clock.instant()).thenReturn(Instant.ofEpochSecond(3700));
        assertTrue(!state.expired(duration));
    }
}
