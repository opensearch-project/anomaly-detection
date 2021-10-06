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

package org.opensearch.ad.model;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

public enum ADTaskType {
    @Deprecated
    HISTORICAL,
    REALTIME_SINGLE_ENTITY,
    REALTIME_HC_DETECTOR,
    HISTORICAL_SINGLE_ENTITY,
    // detector level task to track overall state, init progress, error etc. for HC detector
    HISTORICAL_HC_DETECTOR,
    // entity level task to track just one specific entity's state, init progress, error etc.
    HISTORICAL_HC_ENTITY;

    public static List<ADTaskType> HISTORICAL_DETECTOR_TASK_TYPES = ImmutableList
        .of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.HISTORICAL_SINGLE_ENTITY, ADTaskType.HISTORICAL);
    public static List<ADTaskType> ALL_HISTORICAL_TASK_TYPES = ImmutableList
        .of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.HISTORICAL_SINGLE_ENTITY, ADTaskType.HISTORICAL_HC_ENTITY, ADTaskType.HISTORICAL);
    public static List<ADTaskType> REALTIME_TASK_TYPES = ImmutableList
        .of(ADTaskType.REALTIME_SINGLE_ENTITY, ADTaskType.REALTIME_HC_DETECTOR);
    public static List<ADTaskType> ALL_DETECTOR_TASK_TYPES = ImmutableList
        .of(
            ADTaskType.REALTIME_SINGLE_ENTITY,
            ADTaskType.REALTIME_HC_DETECTOR,
            ADTaskType.HISTORICAL_SINGLE_ENTITY,
            ADTaskType.HISTORICAL_HC_DETECTOR,
            ADTaskType.HISTORICAL
        );

    public static List<String> taskTypeToString(List<ADTaskType> adTaskTypes) {
        return adTaskTypes.stream().map(type -> type.name()).collect(Collectors.toList());
    }
}
