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

import org.opensearch.test.OpenSearchTestCase;

public class EntityModelTests extends OpenSearchTestCase {
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
}
