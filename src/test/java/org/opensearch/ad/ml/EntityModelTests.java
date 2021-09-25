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

import java.util.ArrayDeque;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.test.OpenSearchTestCase;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class EntityModelTests extends OpenSearchTestCase {

    private ThresholdedRandomCutForest trcf;

    @Before
    public void setup() {
        this.trcf = new ThresholdedRandomCutForest(ThresholdedRandomCutForest.builder().dimensions(2).internalShinglingEnabled(true));
    }

    public void testNullInternalSampleQueue() {
        EntityModel model = new EntityModel(null, null, null);
        model.addSample(new double[] { 0.8 });
        assertEquals(1, model.getSamples().size());
    }

    public void testNullInputSample() {
        EntityModel model = new EntityModel(null, null, null);
        model.addSample(null);
        assertEquals(0, model.getSamples().size());
    }

    public void testEmptyInputSample() {
        EntityModel model = new EntityModel(null, null, null);
        model.addSample(new double[] {});
        assertEquals(0, model.getSamples().size());
    }

    @Test
    public void trcf_constructor() {
        EntityModel em = new EntityModel(null, new ArrayDeque<>(), trcf);
        assertEquals(trcf, em.getTrcf().get());
    }

    @Test
    public void clear() {
        EntityModel em = new EntityModel(null, new ArrayDeque<>(), trcf);

        em.clear();

        assertTrue(em.getSamples().isEmpty());
        assertFalse(em.getTrcf().isPresent());
    }

    @Test
    public void setTrcf() {
        EntityModel em = new EntityModel(null, null, null);
        assertFalse(em.getTrcf().isPresent());

        em.setTrcf(this.trcf);
        assertTrue(em.getTrcf().isPresent());
    }
}
