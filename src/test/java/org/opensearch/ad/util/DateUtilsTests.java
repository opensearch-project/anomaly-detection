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

package org.opensearch.ad.util;

import java.time.Duration;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.util.DateUtils;

public class DateUtilsTests extends OpenSearchTestCase {
    public void testDuration() {
        TimeValue time = TimeValue.timeValueHours(3);
        assertEquals(Duration.ofHours(3), DateUtils.toDuration(time));
    }
}
