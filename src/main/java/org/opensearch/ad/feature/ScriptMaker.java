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

package org.opensearch.ad.feature;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

public class ScriptMaker {
    private static final String template = "doc['%s'].value";

    /**
     * We use composite aggregation for feature aggregation.  But composite aggregation
     * does not support ordering results based on doc count, which is required by
     * preview and historical related components.  We need to use terms aggregation.
     * Terms aggregation does not support collecting terms from multiple fields in
     * the same document.  Scripts come to the rescue: With a script to retrieve terms
     * from multiple fields, we can still use terms aggregation to partition data.
     * The script disables the global ordinals optimization and will be slower than
     * collecting terms from a single field.  Still, it gives us the flexibility to
     * implement this option at search time. For a simple example, consider the
     * following query about the number field’s sum aggregation on buckets partitioned
     * by category_field_1 and category_field_2 from index test.
     *
     * Query:
     * GET /test/_search
        {
            "aggregations": {
                "term_agg": {
                    "terms": {
                        "script": {
                            "source": "['category_field_1': doc['category_field_1'].value,
                             'category_field_2': doc['category_field_2'].value]",
                            "lang": "painless"
                        }
                    },
                    "aggregations": {
                        "sum_number": {
                            "sum": {
                                "field": "number"
                            }
                        }
                    }
                }
            }
        }
       *
       * Result:
       *"aggregations": {
            "term_agg": {
                "doc_count_error_upper_bound": 0,
                "sum_other_doc_count": 0,
                "buckets": [
                    {
                        "key": "{category_field_1=app_0, category_field_2=server_1}",
                        "doc_count": 1,
                        "sum_number": {
                            "value": 1449.0
                        }
                    },
                    {
                        "key": "{category_field_1=app_1, category_field_2=server_1}",
                        "doc_count": 1,
                        "sum_number": {
                            "value": 5200.0
                        }
                    },
              ...
       *
       * I put two categorical field in a map for parsing the results.  Otherwise,
       * I won't know which categorical value is for which field.
     * @param fields categorical fields
     * @return script to use in terms aggregation
     */
    public static Script makeTermsScript(List<String> fields) {
        StringBuffer format = new StringBuffer();
        // in painless, a map is sth like [a:b, c:d]
        format.append("[");
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) {
                format.append(",");
            }
            format.append("\'");
            format.append(fields.get(i));
            format.append("\':");
            format.append(String.format(Locale.ROOT, template, fields.get(i)));
        }
        format.append("]");
        return new Script(ScriptType.INLINE, "painless", format.toString(), Collections.emptyMap());
    }
}
