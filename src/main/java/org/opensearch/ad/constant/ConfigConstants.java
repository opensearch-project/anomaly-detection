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

package org.opensearch.ad.constant;

import org.opensearch.Version;

public class ConfigConstants {
    public static final String OPENSEARCH_RESOURCE_SHARING_ENABLED = "plugins.security.resource_sharing.enabled";
    public static final Boolean OPENSEARCH_RESOURCE_SHARING_ENABLED_DEFAULT = true;

    public static final Version RESOURCE_SHARING_MINIMUM_VERSION = Version.V_3_0_0;
}
