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

import static org.opensearch.ad.constant.CommonName.AD_PLUGIN_VERSION_FOR_TEST;

import org.opensearch.Version;

public class ADVersionUtil {

    public static final int VERSION_SEGMENTS = 3;

    public static Version fromString(String adVersion) {
        if (AD_PLUGIN_VERSION_FOR_TEST.equals(adVersion)) {
            return Version.CURRENT;
        }
        return Version.fromString(normalizeVersion(adVersion));
    }

    public static String normalizeVersion(String adVersion) {
        if (adVersion == null) {
            throw new IllegalArgumentException("AD version is null");
        }
        String[] versions = adVersion.split("\\.");
        if (versions.length < VERSION_SEGMENTS) {
            throw new IllegalArgumentException("Wrong AD version " + adVersion);
        }
        StringBuilder normalizedVersion = new StringBuilder();
        normalizedVersion.append(versions[0]);
        for (int i = 1; i < VERSION_SEGMENTS; i++) {
            normalizedVersion.append(".");
            normalizedVersion.append(versions[i]);
        }
        return normalizedVersion.toString();
    }

    public static boolean compatibleWithVersionOnOrAfter1_1(Version adVersion) {
        return adVersion != null && adVersion.onOrAfter(Version.V_1_1_0);
    }
}
