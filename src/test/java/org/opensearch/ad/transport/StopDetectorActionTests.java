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

package org.opensearch.ad.transport;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.test.OpenSearchIntegTestCase;

public class StopDetectorActionTests extends OpenSearchIntegTestCase {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testStopDetectorAction() {
        Assert.assertNotNull(StopDetectorAction.INSTANCE.name());
        Assert.assertEquals(StopDetectorAction.INSTANCE.name(), StopDetectorAction.NAME);
    }
}
