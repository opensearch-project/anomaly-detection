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

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.timeseries.ml.IntermediateResult;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.FeatureData;

import com.amazon.randomcutforest.returntypes.RangeVector;

public class RCFCasterResult extends IntermediateResult<ForecastResult> {
    private final RangeVector forecast;
    private final double dataQuality;

    public RCFCasterResult(RangeVector forecast, double dataQuality, long totalUpdates, double rcfScore) {
        super(totalUpdates, rcfScore);
        this.forecast = forecast;
        this.dataQuality = dataQuality;
    }

    public RangeVector getForecast() {
        return forecast;
    }

    public double getDataQuality() {
        return dataQuality;
    }

    @Override
    public List<ForecastResult> toIndexableResults(
        Config forecaster,
        Instant dataStartInstant,
        Instant dataEndInstant,
        Instant executionStartInstant,
        Instant executionEndInstant,
        List<FeatureData> featureData,
        Optional<Entity> entity,
        Integer schemaVersion,
        String modelId,
        String taskId,
        String error
    ) {
        if (forecast.values == null || forecast.values.length == 0) {
            return Collections.emptyList();
        }
        return ForecastResult
            .fromRawRCFCasterResult(
                forecaster.getId(),
                forecaster.getIntervalInMilliseconds(),
                dataQuality,
                featureData,
                dataStartInstant,
                dataEndInstant,
                executionStartInstant,
                executionEndInstant,
                error,
                entity,
                forecaster.getUser(),
                schemaVersion,
                modelId,
                forecast.values,
                forecast.upper,
                forecast.lower,
                taskId
            );
    }
}
