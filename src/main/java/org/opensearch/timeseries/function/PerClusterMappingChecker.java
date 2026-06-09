/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.function;

import java.util.List;
import java.util.Optional;

import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;

/**
 * Inspects a single cluster's {@link GetFieldMappingsResponse} and decides whether the requested
 * fields are present and acceptable. Returns {@link Optional#empty()} on success, or the
 * exception to surface on failure (typically a {@code ValidationException}).
 */
@FunctionalInterface
public interface PerClusterMappingChecker {
    Optional<Exception> check(List<String> requestedIndices, GetFieldMappingsResponse response);
}
