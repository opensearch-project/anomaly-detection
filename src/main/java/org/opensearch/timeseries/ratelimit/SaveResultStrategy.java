/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.ratelimit;

import java.time.Instant;
import java.util.Optional;

import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IndexableResult;

public interface SaveResultStrategy<IndexableResultType extends IndexableResult, RCFResultType extends IntermediateResult<IndexableResultType>> {
    void saveResult(RCFResultType result, Config config, FeatureRequest origRequest, String modelId);

    void saveResult(
        RCFResultType result,
        Config config,
        Instant dataStart,
        Instant dataEnd,
        String modelId,
        double[] currentData,
        Optional<Entity> entity,
        String taskId
    );

    void saveResult(IndexableResultType result, Config config);
}
