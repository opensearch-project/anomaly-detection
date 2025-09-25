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

package org.opensearch.timeseries.ml;

import static org.hamcrest.Matchers.greaterThan;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

import org.opensearch.test.OpenSearchTestCase;

public class ModelStateTests extends OpenSearchTestCase {

    public void testLastUsedTimeAdvancesOnMutations() {
        Instant base = Instant.parse("2024-01-01T00:00:00Z");
        IncrementingClock clock = new IncrementingClock(base);
        ModelState<Object> state = new ModelState<>(new Object(), "model", "config", "type", clock);

        Instant initial = state.getLastUsedTime();

        state.setModel(new Object());
        Instant afterSetModel = state.getLastUsedTime();

        Optional<Object> model = state.getModel();
        assertTrue(model.isPresent());
        Instant afterGetModel = state.getLastUsedTime();

        state.setPriority(1.0f);
        Instant afterSetPriority = state.getLastUsedTime();

        assertThat(afterSetModel, greaterThan(initial));
        assertThat(afterGetModel, greaterThan(afterSetModel));
        assertThat(afterSetPriority, greaterThan(afterGetModel));
    }

    private static class IncrementingClock extends Clock {
        private Instant current;

        IncrementingClock(Instant start) {
            this.current = start;
        }

        @Override
        public ZoneId getZone() {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            Instant result = current;
            current = current.plusMillis(1);
            return result;
        }
    }
}
