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

import org.opensearch.Version;

/**
 * A helper class for various feature backward compatibility test
 *
 */
public class Bwc {
    /**
     * We are gonna start supporting multi-category field since version 1.1.0.
     * Since Opendistro and Elasticsearch will use a version after 1.0.0, we
     * have to check Lucene version as well. Elasticsearch 7.10 uses Lucene 8.7.0
     *
     * @param version test version
     * @return whether the version support multiple category fields
     */
    public static boolean supportMultiCategoryFields(Version version) {
        return version.after(Version.V_1_0_0) && version.luceneVersion.onOrAfter(org.apache.lucene.util.Version.LUCENE_8_8_2);
    }
}
