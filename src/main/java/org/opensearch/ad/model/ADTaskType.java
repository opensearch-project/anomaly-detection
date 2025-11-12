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

import org.opensearch.timeseries.model.TaskType;

import com.google.common.collect.ImmutableList;

// enum names need to start with REALTIME or HISTORICAL we use prefix in TaskManager to check if a task is of certain type (e.g., historical)
public enum ADTaskType implements TaskType {
    @Deprecated
    HISTORICAL,
    REALTIME_SINGLE_ENTITY,
    REALTIME_HC_DETECTOR,
    HISTORICAL_SINGLE_ENTITY,
    // detector level task to track overall state, init progress, error etc. for HC detector
    HISTORICAL_HC_DETECTOR,
    // entity level task to track just one specific entity's state, init progress, error etc.
    HISTORICAL_HC_ENTITY,
    INSIGHTS;

    public static List<ADTaskType> HISTORICAL_DETECTOR_TASK_TYPES = ImmutableList
        .of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.HISTORICAL_SINGLE_ENTITY, ADTaskType.HISTORICAL);
    public static List<ADTaskType> ALL_HISTORICAL_TASK_TYPES = ImmutableList
        .of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.REALTIME_SINGLE_ENTITY, ADTaskType.HISTORICAL_HC_ENTITY, ADTaskType.HISTORICAL);
    public static List<ADTaskType> REALTIME_TASK_TYPES = ImmutableList
        .of(ADTaskType.REALTIME_SINGLE_ENTITY, ADTaskType.REALTIME_HC_DETECTOR);
    public static List<ADTaskType> INSIGHTS_TASK_TYPES = ImmutableList.of(ADTaskType.INSIGHTS);
    public static List<ADTaskType> ALL_DETECTOR_TASK_TYPES = ImmutableList
        .of(
            ADTaskType.REALTIME_SINGLE_ENTITY,
            ADTaskType.REALTIME_HC_DETECTOR,
            ADTaskType.HISTORICAL_SINGLE_ENTITY,
            ADTaskType.HISTORICAL_HC_DETECTOR,
            ADTaskType.HISTORICAL,
            ADTaskType.INSIGHTS
        );
}
