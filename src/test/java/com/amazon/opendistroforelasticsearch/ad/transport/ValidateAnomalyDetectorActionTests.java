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

package com.amazon.opendistroforelasticsearch.ad.transport;

import org.junit.Assert;
import org.junit.Test;

public class ValidateAnomalyDetectorActionTests {
    @Test
    public void testValidateAnomalyDetectorActionTests() {
        Assert.assertNotNull(ValidateAnomalyDetectorAction.INSTANCE.name());
        Assert.assertEquals(ValidateAnomalyDetectorAction.INSTANCE.name(), ValidateAnomalyDetectorAction.NAME);
    }
}
