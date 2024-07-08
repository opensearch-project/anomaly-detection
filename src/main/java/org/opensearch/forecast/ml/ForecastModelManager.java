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

package org.opensearch.forecast.ml;

import java.time.Clock;
import java.util.Locale;

import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.ratelimit.ForecastCheckpointWriteWorker;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.model.Config;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ForecastDescriptor;
import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastModelManager extends
    ModelManager<RCFCaster, ForecastResult, RCFCasterResult, ForecastIndex, ForecastIndexManagement, ForecastCheckpointDao, ForecastCheckpointWriteWorker, ForecastColdStart> {

    public ForecastModelManager(
        ForecastCheckpointDao checkpointDao,
        Clock clock,
        int rcfNumTrees,
        int rcfNumSamplesInTree,
        int rcfNumMinSamples,
        ForecastColdStart entityColdStarter,
        MemoryTracker memoryTracker,
        FeatureManager featureManager
    ) {
        super(rcfNumTrees, rcfNumSamplesInTree, rcfNumMinSamples, entityColdStarter, memoryTracker, clock, featureManager, checkpointDao);
    }

    @Override
    protected RCFCasterResult createEmptyResult() {
        return new RCFCasterResult(null, 0, 0, 0);
    }

    @Override
    protected <RCFDescriptor extends AnomalyDescriptor> RCFCasterResult toResult(
        RandomCutForest forecast,
        RCFDescriptor castDescriptor,
        double[] point,
        boolean isImputed,
        Config config
    ) {
        if (castDescriptor instanceof ForecastDescriptor) {
            ForecastDescriptor forecastDescriptor = (ForecastDescriptor) castDescriptor;
            // Use forecastDescriptor in the rest of your method
            return new RCFCasterResult(
                forecastDescriptor.getTimedForecast().rangeVector,
                forecastDescriptor.getDataConfidence(),
                forecast.getTotalUpdates(),
                forecastDescriptor.getRCFScore()
            );
        } else {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Unsupported type of AnomalyDescriptor : %s", castDescriptor));
        }
    }
}
