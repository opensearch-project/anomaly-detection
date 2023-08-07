/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.model;

import java.util.List;
import java.util.stream.Collectors;

public interface TaskType {
    String name();

    public static List<String> taskTypeToString(List<? extends TaskType> adTaskTypes) {
        return adTaskTypes.stream().map(type -> type.name()).collect(Collectors.toList());
    }
}
