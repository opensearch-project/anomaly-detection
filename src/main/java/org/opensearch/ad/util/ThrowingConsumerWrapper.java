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

import java.util.function.Consumer;

public class ThrowingConsumerWrapper {
    /**
     * Utility method to use a method throwing checked exception inside a function
     *  that does not throw the corresponding checked exception.  This happens
     *  when we are in a ES function that we have no control over its signature.
     * Convert the checked exception thrown by by throwingConsumer to a RuntimeException
     * so that the compiler won't complain.
     * @param <T> the method's parameter type
     * @param throwingConsumer the method reference that can throw checked exception
     * @return converted method reference
     */
    public static <T> Consumer<T> throwingConsumerWrapper(ThrowingConsumer<T, Exception> throwingConsumer) {

        return i -> {
            try {
                throwingConsumer.accept(i);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }
}
