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

package org.opensearch.ad.settings;

import static java.util.Collections.unmodifiableMap;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.common.settings.Setting;
import org.opensearch.timeseries.settings.DynamicNumericSetting;

public class ADNumericSetting extends DynamicNumericSetting {

    /**
     * Singleton instance
     */
    private static ADNumericSetting INSTANCE;

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

    ADNumericSetting(Map<String, Setting<?>> settings) {
        super(settings);
    }

    public static synchronized ADNumericSetting getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ADNumericSetting(settings);
        }
        return INSTANCE;
    }

    /**
     * @return the max number of categorical fields
     */
    public static int maxCategoricalFields() {
        return ADNumericSetting.getInstance().getSettingValue(ADNumericSetting.CATEGORY_FIELD_LIMIT);
    }
}
