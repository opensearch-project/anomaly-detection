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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayDeque;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.test.OpenSearchTestCase;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.threshold.ThresholdedRandomCutForest;

public class EntityModelTests extends OpenSearchTestCase {

    private ThresholdedRandomCutForest ercf;

    @Before
    public void setup() {
        this.ercf = new ThresholdedRandomCutForest(ThresholdedRandomCutForest.builder().dimensions(2));
    }

    public void testNullInternalSampleQueue() {
        EntityModel model = new EntityModel(null, null, null, null);
        model.addSample(new double[] { 0.8 });
        assertEquals(1, model.getSamples().size());
    }

    public void testNullInputSample() {
        EntityModel model = new EntityModel(null, null, null, null);
        model.addSample(null);
        assertEquals(0, model.getSamples().size());
    }

    public void testEmptyInputSample() {
        EntityModel model = new EntityModel(null, null, null, null);
        model.addSample(new double[] {});
        assertEquals(0, model.getSamples().size());
    }

    @Test
    public void ercf_constructor() {
        EntityModel em = new EntityModel(null, new ArrayDeque<>(), ercf);
        assertEquals(ercf, em.getErcf().get());
    }

    @Test
    public void clear() {
        EntityModel em = new EntityModel(null, new ArrayDeque<>(), ercf);

        em.clear();

        assertTrue(em.getSamples().isEmpty());
        assertFalse(em.getErcf().isPresent());
    }

    @Test
    public void setErcf() {
        EntityModel em = new EntityModel(null, null, null, null);
        assertFalse(em.getErcf().isPresent());

        em.setErcf(this.ercf);
        assertTrue(em.getErcf().isPresent());
    }
}
