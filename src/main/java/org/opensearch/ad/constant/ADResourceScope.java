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

import org.opensearch.security.spi.resources.ResourceAccessScope;

public enum ADResourceScope implements ResourceAccessScope<ADResourceScope> {
    AD_READ_ACCESS("ad_read_access"),
    AD_FULL_ACCESS("ad_full_access");

    private final String scopeName;

    ADResourceScope(String scopeName) {
        this.scopeName = scopeName;
    }

    @Override
    public String value() {
        return scopeName;
    }
}
