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

package org.opensearch.ad.settings;

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.Setting;

public class NumericSetting extends AbstractSetting {

    private static Logger logger = LogManager.getLogger(NumericSetting.class);

    /**
     * Singleton instance
     */
    private static NumericSetting INSTANCE;

    /**
     * Settings name
     */
    public static final String CATEGORY_FIELD_LIMIT = "plugins.anomaly_detection.category_field_limit";

    private static final Map<String, Setting<?>> settings = unmodifiableMap(new HashMap<String, Setting<?>>() {
        {
            // how many categorical fields we support
            // The number of category field won't causes correctness issues for our
            // implementation, but can cause performance issues. The more categorical
            // fields, the larger of the anomaly results, intermediate states, and
            // more expensive entities (e.g., to get top entities in preview API, we need
            // to use scripts in terms aggregation. The more fields, the slower the query).
            put(
                CATEGORY_FIELD_LIMIT,
                Setting.intSetting(CATEGORY_FIELD_LIMIT, 2, 0, 5, Setting.Property.NodeScope, Setting.Property.Dynamic)
            );
        }
    });

    private NumericSetting(Map<String, Setting<?>> settings) {
        super(settings);
    }

    public static synchronized NumericSetting getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new NumericSetting(settings);
        }
        return INSTANCE;
    }

    /**
     * Whether AD plugin is enabled.  If disabled, AD plugin rejects RESTful requests and stop all AD jobs.
     * @return whether AD plugin is enabled.
     */
    public static int maxCategoricalFields() {
        return NumericSetting.getInstance().getSettingValue(NumericSetting.CATEGORY_FIELD_LIMIT);
    }
}
