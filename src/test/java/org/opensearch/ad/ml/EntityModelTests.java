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

import org.junit.Test;

import com.amazon.randomcutforest.ERCF.ExtendedRandomCutForest;
import com.amazon.randomcutforest.RandomCutForest;

public class EntityModelTests {

    @Test
    public void ercf_constructor() {
        ExtendedRandomCutForest ercf = new ExtendedRandomCutForest(RandomCutForest.builder().dimensions(2), 0.01);
        EntityModel em = new EntityModel(null, new ArrayDeque<>(), ercf);
        assertEquals(ercf, em.getErcf().get());
    }

    @Test
    public void clear() {
        ExtendedRandomCutForest ercf = new ExtendedRandomCutForest(RandomCutForest.builder().dimensions(2), 0.01);
        EntityModel em = new EntityModel(null, new ArrayDeque<>(), ercf);

        em.clear();

        assertTrue(em.getSamples().isEmpty());
        assertFalse(em.getErcf().isPresent());
    }
}
