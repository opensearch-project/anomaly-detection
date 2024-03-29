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

import java.io.IOException;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.function.ThrowingSupplierWrapper;

public class ThrowingSupplierWrapperTests extends OpenSearchTestCase {
    private static String foo() throws IOException {
        throw new IOException("blah");
    }

    public void testExceptionThrown() {
        expectThrows(
            RuntimeException.class,
            () -> ThrowingSupplierWrapper.throwingSupplierWrapper(ThrowingSupplierWrapperTests::foo).get()
        );
    }
}
