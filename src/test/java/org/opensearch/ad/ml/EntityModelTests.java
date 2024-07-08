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

package org.opensearch.ad.ml;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayDeque;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class EntityModelTests extends OpenSearchTestCase {

    private ThresholdedRandomCutForest trcf;
    private Clock clock;

    @Before
    public void setup() {
        this.trcf = new ThresholdedRandomCutForest(ThresholdedRandomCutForest.builder().dimensions(2).internalShinglingEnabled(true));
        this.clock = Clock.systemUTC();
    }

    public void testNullInternalSampleQueue() {
        ModelState<ThresholdedRandomCutForest> model = new ModelState<>(null, null, null, null, clock, 0, null, null);
        model.addSample(new Sample(new double[] { 0.8 }, Instant.now(), Instant.now()));
        assertEquals(1, model.getSamples().size());
    }

    public void testNullInputSample() {
        ModelState<ThresholdedRandomCutForest> model = new ModelState<>(null, null, null, null, clock, 0, null, null);
        model.addSample(null);
        assertEquals(0, model.getSamples().size());
    }

    public void testEmptyInputSample() {
        ModelState<ThresholdedRandomCutForest> model = new ModelState<>(null, null, null, null, clock, 0, null, null);
        model.addSample(new Sample(new double[] {}, Instant.now(), Instant.now()));
        assertEquals(0, model.getSamples().size());
    }

    @Test
    public void trcf_constructor() {
        ModelState<ThresholdedRandomCutForest> em = new ModelState<>(trcf, null, null, null, clock, 0, null, new ArrayDeque<>());
        assertEquals(trcf, em.getModel().get());
    }

    @Test
    public void clear() {
        ModelState<ThresholdedRandomCutForest> em = new ModelState<>(trcf, null, null, null, clock, 0, null, new ArrayDeque<>());

        em.clear();

        assertTrue(em.getSamples().isEmpty());
        assertFalse(em.getModel().isPresent());
    }

    @Test
    public void setTrcf() {
        ModelState<ThresholdedRandomCutForest> em = new ModelState<>(null, null, null, null, clock, 0, null, null);
        assertFalse(em.getModel().isPresent());

        em.setModel(this.trcf);
        assertTrue(em.getModel().isPresent());
    }
}
