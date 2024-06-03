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

package org.opensearch.ad.stats.suppliers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.stats.suppliers.IndexStatusSupplier;
import org.opensearch.timeseries.util.IndexUtils;

public class IndexSupplierTests extends OpenSearchTestCase {
    private IndexUtils indexUtils;
    private String indexStatus;
    private String indexName;

    @Before
    public void setup() {
        indexUtils = mock(IndexUtils.class);
        indexStatus = "yellow";
        indexName = "test-index";
        when(indexUtils.getIndexHealthStatus(indexName)).thenReturn(indexStatus);
    }

    @Test
    public void testGet() {
        IndexStatusSupplier indexStatusSupplier1 = new IndexStatusSupplier(indexUtils, indexName);
        assertEquals("Get method for IndexSupplier does not work", indexStatus, indexStatusSupplier1.get());

        String invalidIndex = "invalid";
        when(indexUtils.getIndexHealthStatus(invalidIndex)).thenThrow(IllegalArgumentException.class);
        IndexStatusSupplier indexStatusSupplier2 = new IndexStatusSupplier(indexUtils, invalidIndex);
        assertEquals(
            "Get method does not return correct response onf exception",
            IndexStatusSupplier.UNABLE_TO_RETRIEVE_HEALTH_MESSAGE,
            indexStatusSupplier2.get()
        );
    }
}
