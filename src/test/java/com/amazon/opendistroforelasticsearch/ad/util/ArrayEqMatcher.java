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
