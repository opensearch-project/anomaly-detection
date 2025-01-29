/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ratelimit;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import org.opensearch.ad.ml.ThresholdingResult;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.SaveResultStrategy;
import org.opensearch.timeseries.util.ParseUtils;

public class ADSaveResultStrategy implements SaveResultStrategy<AnomalyResult, ThresholdingResult> {
    private int resultMappingVersion;
    private ADResultWriteWorker resultWriteWorker;

    public ADSaveResultStrategy(int resultMappingVersion, ADResultWriteWorker resultWriteWorker) {
        this.resultMappingVersion = resultMappingVersion;
        this.resultWriteWorker = resultWriteWorker;
    }

    @Override
    public void saveResult(ThresholdingResult result, Config config, FeatureRequest origRequest, String modelId) {
        // result.getRcfScore() = 0 means the model is not initialized
        // result.getGrade() = 0 means it is not an anomaly
        saveResult(
            result,
            config,
            Instant.ofEpochMilli(origRequest.getDataStartTimeMillis()),
            Instant.ofEpochMilli(origRequest.getDataStartTimeMillis() + config.getIntervalInMilliseconds()),
            modelId,
            origRequest.getCurrentFeature(),
            origRequest.getEntity(),
            origRequest.getTaskId()
        );
    }

    @Override
    public void saveResult(
        ThresholdingResult result,
        Config config,
        Instant dataStart,
        Instant dataEnd,
        String modelId,
        double[] currentData,
        Optional<Entity> entity,
        String taskId
    ) {
        // result.getRcfScore() = 0 means the model is not initialized
        // result.getGrade() = 0 means it is not an anomaly
        if (result != null && result.getRcfScore() > 0) {
            List<AnomalyResult> indexableResults = result
                .toIndexableResults(
                    config,
                    dataStart,
                    dataEnd,
                    Instant.now(),
                    Instant.now(),
                    ParseUtils.getFeatureData(currentData, config),
                    entity,
                    resultMappingVersion,
                    modelId,
                    taskId,
                    null
                );
            for (AnomalyResult r : indexableResults) {
                saveResult(r, config);
            }
        }
    }

    @Override
    public void saveResult(AnomalyResult result, Config config) {
        resultWriteWorker
            .put(
                new ADResultWriteRequest(
                    System.currentTimeMillis() + config.getIntervalInMilliseconds(),
                    config.getId(),
                    result.getAnomalyGrade() > 0 ? RequestPriority.HIGH : RequestPriority.MEDIUM,
                    result,
                    config.getCustomResultIndexOrAlias(),
                    config.getFlattenResultIndexAlias()
                )
            );
    }

}
