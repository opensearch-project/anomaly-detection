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

package org.opensearch.ad.cluster;

import org.opensearch.Version;
import org.opensearch.ad.ADUnitTestCase;
import org.opensearch.timeseries.cluster.VersionUtil;

public class ADVersionUtilTests extends ADUnitTestCase {

    public void testParseVersionFromString() {
        Version version = VersionUtil.fromString("2.1.0.0");
        assertEquals(Version.V_2_1_0, version);

        version = VersionUtil.fromString("2.1.0");
        assertEquals(Version.V_2_1_0, version);
    }

    public void testParseVersionFromStringWithNull() {
        expectThrows(IllegalArgumentException.class, () -> VersionUtil.fromString(null));
    }

    public void testParseVersionFromStringWithWrongFormat() {
        expectThrows(IllegalArgumentException.class, () -> VersionUtil.fromString("1.1"));
    }
}
