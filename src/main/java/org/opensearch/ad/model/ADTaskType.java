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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.ad.model;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;

public enum ADTaskType {
    @Deprecated
    HISTORICAL,
    REALTIME_SINGLE_FLOW,
    REALTIME_HC_DETECTOR,
    HISTORICAL_SINGLE_FLOW,
    HISTORICAL_HC_DETECTOR,
    HISTORICAL_HC_ENTITY;

    public static List<ADTaskType> HISTORICAL_DETECTOR_TASK_TYPES = ImmutableList
        .of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.HISTORICAL_SINGLE_FLOW, ADTaskType.HISTORICAL);
    public static List<ADTaskType> ALL_HISTORICAL_TASK_TYPES = ImmutableList
        .of(ADTaskType.HISTORICAL_HC_DETECTOR, ADTaskType.HISTORICAL_SINGLE_FLOW, ADTaskType.HISTORICAL_HC_ENTITY, ADTaskType.HISTORICAL);
    public static List<ADTaskType> REALTIME_TASK_TYPES = ImmutableList.of(ADTaskType.REALTIME_SINGLE_FLOW, ADTaskType.REALTIME_HC_DETECTOR);
    public static List<ADTaskType> ALL_DETECTOR_TASK_TYPES = ImmutableList
        .of(
            ADTaskType.REALTIME_SINGLE_FLOW,
            ADTaskType.REALTIME_HC_DETECTOR,
            ADTaskType.HISTORICAL_SINGLE_FLOW,
            ADTaskType.HISTORICAL_HC_DETECTOR,
            ADTaskType.HISTORICAL
        );

    public static List<String> taskTypeToString(List<ADTaskType> adTaskTypes) {
        return adTaskTypes.stream().map(type -> type.name()).collect(Collectors.toList());
    }
}
