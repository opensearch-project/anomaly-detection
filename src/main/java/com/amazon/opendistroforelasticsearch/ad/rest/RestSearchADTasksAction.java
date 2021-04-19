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

package com.amazon.opendistroforelasticsearch.ad.rest;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.ADTask;
import com.amazon.opendistroforelasticsearch.ad.transport.SearchADTasksAction;

/**
 * This class consists of the REST handler to search AD tasks.
 */
public class RestSearchADTasksAction extends AbstractSearchAction<ADTask> {

    private static final String URL_PATH = AnomalyDetectorPlugin.AD_BASE_DETECTORS_URI + "/tasks/_search";
    private final String SEARCH_ANOMALY_DETECTION_TASKS = "search_anomaly_detection_tasks";

    public RestSearchADTasksAction() {
        super(URL_PATH, CommonName.DETECTION_STATE_INDEX, ADTask.class, SearchADTasksAction.INSTANCE);
    }

    @Override
    public String getName() {
        return SEARCH_ANOMALY_DETECTION_TASKS;
    }

}
