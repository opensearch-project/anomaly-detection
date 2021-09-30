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

import java.util.Arrays;

import org.mockito.ArgumentMatcher;

/**
 * An argument matcher based on deep equality needed for array types.
 *
 * The default eq or aryEq from Mockito fails on nested array types, such as a matrix.
 * This matcher takes the expected argument and returns a match result based on deep equality.
 */
public class ArrayEqMatcher<T> implements ArgumentMatcher<T> {

    private final T expected;

    /**
     * Constructor with expected value.
     *
     * @param expected the value expected to match by equality
     */
    public ArrayEqMatcher(T expected) {
        this.expected = expected;
    }

    @Override
    public boolean matches(T actual) {
        return Arrays.deepEquals((Object[]) expected, (Object[]) actual);
    }
}
