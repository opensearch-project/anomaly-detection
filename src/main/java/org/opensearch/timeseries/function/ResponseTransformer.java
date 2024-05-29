/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.function;

/**
 * A functional interface for response transformation
 *
 * @param <T> input type
 * @param <R> output type
 */
@FunctionalInterface
public interface ResponseTransformer<T, R> {
    R transform(T input);
}
