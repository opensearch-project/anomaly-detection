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

package org.opensearch.forecast;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.forecast.ml.ForecastColdStart;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.Features;
import org.opensearch.timeseries.ml.Sample;

/**
 * Runner to trigger a forecasting preview request.
 */
public final class ForecasterRunner {

    private static final Logger logger = LogManager.getLogger(ForecasterRunner.class);

    private final ForecastColdStart forecastColdStart;
    private final FeatureManager featureManager;

    public ForecasterRunner(ForecastColdStart forecastColdStart, FeatureManager featureManager) {
        this.forecastColdStart = forecastColdStart;
        this.featureManager = featureManager;
    }

    /**
     * Runs forecasting preview and returns preview forecast results.
     *
     * @param forecaster forecaster instance
     * @param startTime preview period start time
     * @param endTime preview period end time
     * @param listener callback with preview forecast results
     * @throws IOException if feature query cannot be built
     */
    public void executeForecaster(Forecaster forecaster, Instant startTime, Instant endTime, ActionListener<List<ForecastResult>> listener)
        throws IOException {
        if (forecaster.getCategoryFields() != null && !forecaster.getCategoryFields().isEmpty()) {
            listener
                .onFailure(
                    new OpenSearchStatusException(
                        "Forecast preview does not support high-cardinality forecasters yet.",
                        RestStatus.BAD_REQUEST
                    )
                );
            return;
        }

        featureManager
            .getPreviewFeatures(
                forecaster,
                AnalysisType.FORECAST,
                startTime.toEpochMilli(),
                endTime.toEpochMilli(),
                ActionListener.wrap(features -> {
                    try {
                        List<Sample> samples = toSamples(features);
                        if (samples.isEmpty()) {
                            listener.onResponse(Collections.emptyList());
                            return;
                        }

                        listener.onResponse(forecastColdStart.preview(samples, forecaster));
                    } catch (Exception e) {
                        logger.info("Fail to preview forecaster {}", forecaster.getId(), e);
                        listener.onFailure(e);
                    }
                }, listener::onFailure)
            );
    }

    private List<Sample> toSamples(Features features) {
        List<Entry<Long, Long>> timeRanges = features.getTimeRanges();
        double[][] unprocessedFeatures = features.getUnprocessedFeatures();
        if (timeRanges == null || unprocessedFeatures == null) {
            return Collections.emptyList();
        }

        int sampleSize = Math.min(timeRanges.size(), unprocessedFeatures.length);
        List<Sample> samples = new ArrayList<>(sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            Entry<Long, Long> timeRange = timeRanges.get(i);
            if (timeRange == null || unprocessedFeatures[i] == null || unprocessedFeatures[i].length == 0) {
                continue;
            }
            samples
                .add(
                    new Sample(unprocessedFeatures[i], Instant.ofEpochMilli(timeRange.getKey()), Instant.ofEpochMilli(timeRange.getValue()))
                );
        }

        return samples;
    }
}
