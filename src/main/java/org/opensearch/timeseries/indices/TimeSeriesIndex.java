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

package org.opensearch.timeseries.indices;

import org.opensearch.timeseries.constant.CommonName;

public interface TimeSeriesIndex {
    String CUSTOM_RESULT_INDEX = "custom_result_index";

    public String getIndexName();

    public boolean isAlias();

    public String getMapping();

    public default boolean isJobIndex() {
        return CommonName.JOB_INDEX.equals(getIndexName());
    }

    public default boolean isCustomResultIndex() {
        return getIndexName() == CUSTOM_RESULT_INDEX;
    }

    public default boolean isConfigIndex() {
        return CommonName.CONFIG_INDEX.equals(getIndexName());
    }
}
