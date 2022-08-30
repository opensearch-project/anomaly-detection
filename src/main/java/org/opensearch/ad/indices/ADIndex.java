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

import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.util.ThrowingSupplierWrapper;

/**
 * Represent an AD index
 *
 */
public enum ADIndex {

    // throw RuntimeException since we don't know how to handle the case when the mapping reading throws IOException
    RESULT(
        CommonName.ANOMALY_RESULT_INDEX_ALIAS,
        true,
        ThrowingSupplierWrapper.throwingSupplierWrapper(AnomalyDetectionIndices::getAnomalyResultMappings)
    ),
    CONFIG(
        AnomalyDetector.ANOMALY_DETECTORS_INDEX,
        false,
        ThrowingSupplierWrapper.throwingSupplierWrapper(AnomalyDetectionIndices::getAnomalyDetectorMappings)
    ),
    // JOB(
    // AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX,
    // false,
    // ThrowingSupplierWrapper.throwingSupplierWrapper(AnomalyDetectionIndices::getAnomalyDetectorJobMappings)
    // ),
    CHECKPOINT(
        CommonName.CHECKPOINT_INDEX_NAME,
        false,
        ThrowingSupplierWrapper.throwingSupplierWrapper(AnomalyDetectionIndices::getCheckpointMappings)
    ),
    STATE(
        CommonName.DETECTION_STATE_INDEX,
        false,
        ThrowingSupplierWrapper.throwingSupplierWrapper(AnomalyDetectionIndices::getDetectionStateMappings)
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

    public String getIndexName() {
        return indexName;
    }

    public boolean isAlias() {
        return alias;
    }

    public String getMapping() {
        return mapping;
    }

}
