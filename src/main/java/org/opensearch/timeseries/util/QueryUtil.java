/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.util.Collections;

import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

import com.google.common.collect.ImmutableMap;

public class QueryUtil {
    /**
     * Generates the painless script to fetch results that have an entity name matching the passed-in category field.
     *
     * @param categoryField the category field to be used as a source
     * @return the painless script used to get all docs with entity name values matching the category field
     */
    public static Script getScriptForCategoryField(String categoryField) {
        StringBuilder builder = new StringBuilder()
            .append("String value = null;")
            .append("if (params == null || params._source == null || params._source.entity == null) {")
            .append("return \"\"")
            .append("}")
            .append("for (item in params._source.entity) {")
            .append("if (item[\"name\"] == params[\"categoryField\"]) {")
            .append("value = item['value'];")
            .append("break;")
            .append("}")
            .append("}")
            .append("return value;");

        // The last argument contains the K/V pair to inject the categoryField value into the script
        return new Script(
            ScriptType.INLINE,
            "painless",
            builder.toString(),
            Collections.emptyMap(),
            ImmutableMap.of("categoryField", categoryField)
        );
    }
}
