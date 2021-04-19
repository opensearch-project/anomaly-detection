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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.util;

import java.util.function.Supplier;

public class ThrowingSupplierWrapper {
    /**
     * Utility method to use a method throwing checked exception inside a place
     *  that does not allow throwing the corresponding checked exception (e.g.,
     *  enum initialization).
     * Convert the checked exception thrown by by throwingConsumer to a RuntimeException
     * so that the compiler won't complain.
     * @param <T> the method's return type
     * @param throwingSupplier the method reference that can throw checked exception
     * @return converted method reference
     */
    public static <T> Supplier<T> throwingSupplierWrapper(ThrowingSupplier<T, Exception> throwingSupplier) {

        return () -> {
            try {
                return throwingSupplier.get();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
    }
}
