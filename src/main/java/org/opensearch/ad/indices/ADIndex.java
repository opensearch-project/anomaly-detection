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

package org.opensearch.ad.indices;

import java.util.function.Supplier;

import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ThrowingSupplierWrapper;
import org.opensearch.timeseries.indices.TimeSeriesIndex;

/**
 * Represent an AD index
 *
 */
public enum ADIndex implements TimeSeriesIndex {

    // throw RuntimeException since we don't know how to handle the case when the mapping reading throws IOException
    RESULT(
        ADCommonName.ANOMALY_RESULT_INDEX_ALIAS,
        true,
        ThrowingSupplierWrapper.throwingSupplierWrapper(ADIndexManagement::getResultMappings)
    ),
    CONFIG(ADCommonName.CONFIG_INDEX, false, ThrowingSupplierWrapper.throwingSupplierWrapper(ADIndexManagement::getConfigMappings)),
    JOB(CommonName.JOB_INDEX, false, ThrowingSupplierWrapper.throwingSupplierWrapper(ADIndexManagement::getJobMappings)),
    CHECKPOINT(
        ADCommonName.CHECKPOINT_INDEX_NAME,
        false,
        ThrowingSupplierWrapper.throwingSupplierWrapper(ADIndexManagement::getCheckpointMappings)
    ),
    STATE(ADCommonName.DETECTION_STATE_INDEX, false, ThrowingSupplierWrapper.throwingSupplierWrapper(ADIndexManagement::getStateMappings)),
    CUSTOM_RESULT(CUSTOM_RESULT_INDEX, true, ThrowingSupplierWrapper.throwingSupplierWrapper(ADIndexManagement::getResultMappings)),
    CUSTOM_INSIGHTS_RESULT(
        ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS,
        true,
        ThrowingSupplierWrapper.throwingSupplierWrapper(ADIndexManagement::getInsightsResultMappings)
    );

    private final String indexName;
    // whether we use an alias for the index
    private final boolean alias;
    private final String mapping;

    ADIndex(String name, boolean alias, Supplier<String> mappingSupplier) {
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
    public boolean isConfigIndex() {
        return ADCommonName.CONFIG_INDEX.equals(getIndexName());
    }
}
