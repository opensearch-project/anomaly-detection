/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.ratelimit;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.opensearch.forecast.ml.RCFCasterResult;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.ratelimit.FeatureRequest;
import org.opensearch.timeseries.ratelimit.RequestPriority;
import org.opensearch.timeseries.ratelimit.SaveResultStrategy;
import org.opensearch.timeseries.util.ParseUtils;

public class ForecastSaveResultStrategy implements SaveResultStrategy<ForecastResult, RCFCasterResult> {
    private int resultMappingVersion;
    private ForecastResultWriteWorker resultWriteWorker;

    public ForecastSaveResultStrategy(int resultMappingVersion, ForecastResultWriteWorker resultWriteWorker) {
        this.resultMappingVersion = resultMappingVersion;
        this.resultWriteWorker = resultWriteWorker;
    }

    @Override
    public void saveResult(RCFCasterResult result, Config config, FeatureRequest origRequest, String modelId) {
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
        RCFCasterResult result,
        Config config,
        Instant dataStart,
        Instant dataEnd,
        String modelId,
        double[] currentData,
        Optional<Entity> entity,
        String taskId
    ) {
        if (result != null) {
            List<ForecastResult> indexableResults = result
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

            for (ForecastResult r : indexableResults) {
                saveResult(r, config);
            }
        }
    }

    @Override
    public void saveResult(ForecastResult result, Config config) {
        resultWriteWorker
            .put(
                new ForecastResultWriteRequest(
                    System.currentTimeMillis() + config.getFrequencyInMilliseconds(),
                    config.getId(),
                    RequestPriority.MEDIUM,
                    result,
                    config.getCustomResultIndexOrAlias(),
                    config.getFlattenResultIndexAlias()
                )
            );
    }

    @Override
    public void saveAllResults(
        List<RCFCasterResult> results,
        Config config,
        List<Instant> dataStart,
        List<Instant> dataEnd,
        String modelId,
        List<double[]> currentData,
        Optional<Entity> entity,
        String taskId
    ) {
        List<ForecastResultWriteRequest> writeRequests = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            RCFCasterResult result = results.get(i);
            if (result != null) {
                List<ForecastResult> indexableResults = result
                    .toIndexableResults(
                        config,
                        dataStart.get(i),
                        dataEnd.get(i),
                        Instant.now(),
                        Instant.now(),
                        ParseUtils.getFeatureData(currentData.get(i), config),
                        entity,
                        resultMappingVersion,
                        modelId,
                        taskId,
                        null
                    );
                for (ForecastResult r : indexableResults) {
                    writeRequests
                        .add(
                            new ForecastResultWriteRequest(
                                System.currentTimeMillis() + config.getFrequencyInMilliseconds(),
                                config.getId(),
                                RequestPriority.MEDIUM,
                                r,
                                config.getCustomResultIndexOrAlias(),
                                config.getFlattenResultIndexAlias()
                            )
                        );
                }
            }
        }
        resultWriteWorker.putAll(writeRequests);
    }
}
