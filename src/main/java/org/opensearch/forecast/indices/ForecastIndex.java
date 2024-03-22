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

package org.opensearch.forecast.indices;

import java.util.function.Supplier;

import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ThrowingSupplierWrapper;
import org.opensearch.timeseries.indices.TimeSeriesIndex;

public enum ForecastIndex implements TimeSeriesIndex {
    // throw RuntimeException since we don't know how to handle the case when the mapping reading throws IOException
    RESULT(
        ForecastCommonName.FORECAST_RESULT_INDEX_ALIAS,
        true,
        ThrowingSupplierWrapper.throwingSupplierWrapper(ForecastIndexManagement::getResultMappings)
    ),
    CONFIG(CommonName.CONFIG_INDEX, false, ThrowingSupplierWrapper.throwingSupplierWrapper(ADIndexManagement::getConfigMappings)),
    JOB(CommonName.JOB_INDEX, false, ThrowingSupplierWrapper.throwingSupplierWrapper(ADIndexManagement::getJobMappings)),
    CHECKPOINT(
        ForecastCommonName.FORECAST_CHECKPOINT_INDEX_NAME,
        false,
        ThrowingSupplierWrapper.throwingSupplierWrapper(ForecastIndexManagement::getCheckpointMappings)
    ),
    STATE(
        ForecastCommonName.FORECAST_STATE_INDEX,
        false,
        ThrowingSupplierWrapper.throwingSupplierWrapper(ForecastIndexManagement::getStateMappings)
    );

    private final String indexName;
    // whether we use an alias for the index
    private final boolean alias;
    private final String mapping;

    ForecastIndex(String name, boolean alias, Supplier<String> mappingSupplier) {
        this.indexName = name;
        this.alias = alias;
        this.mapping = mappingSupplier.get();
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public boolean isAlias() {
        return alias;
    }

    @Override
    public String getMapping() {
        return mapping;
    }

    @Override
    public boolean isJobIndex() {
        return CommonName.JOB_INDEX.equals(indexName);
    }
}
