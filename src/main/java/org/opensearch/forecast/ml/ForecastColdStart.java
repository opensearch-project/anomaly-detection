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
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.forecast.indices.ForecastIndex;
import org.opensearch.forecast.indices.ForecastIndexManagement;
import org.opensearch.forecast.model.ForecastResult;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.ModelUtil;
import org.opensearch.timeseries.util.ParseUtils;

import com.amazon.randomcutforest.config.ForestMode;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ForecastDescriptor;
import com.amazon.randomcutforest.parkservices.RCFCaster;
import com.amazon.randomcutforest.parkservices.config.Calibration;

public class ForecastColdStart extends ModelColdStart<RCFCaster, ForecastIndex, ForecastIndexManagement, ForecastResult> {

    private static final Logger logger = LogManager.getLogger(ForecastColdStart.class);

    public ForecastColdStart(
        Clock clock,
        ThreadPool threadPool,
        NodeStateManager nodeStateManager,
        int rcfSampleSize,
        int numberOfTrees,
        int numMinSamples,
        SearchFeatureDao searchFeatureDao,
        double thresholdMinPvalue,
        FeatureManager featureManager,
        Duration modelTtl,
        int coolDownMinutes,
        long rcfSeed,
        int defaultTrainSamples,
        int maxRoundofColdStart,
        int resultSchemaVersion
    ) {
        // 1 means we sample all real data if possible
        super(
            modelTtl,
            coolDownMinutes,
            clock,
            threadPool,
            numMinSamples,
            rcfSeed,
            numberOfTrees,
            rcfSampleSize,
            thresholdMinPvalue,
            nodeStateManager,
            1,
            defaultTrainSamples,
            searchFeatureDao,
            featureManager,
            maxRoundofColdStart,
            TimeSeriesAnalyticsPlugin.FORECAST_THREAD_POOL_NAME,
            AnalysisType.FORECAST,
            resultSchemaVersion
        );
    }

    @Override
    protected List<ForecastResult> trainModelFromDataSegments(
        List<Sample> pointSamples,
        ModelState<RCFCaster> modelState,
        Config config,
        String taskId
    ) {
        if (pointSamples == null || pointSamples.size() == 0) {
            logger.info("Return early since data points must not be empty.");
            return null;
        }

        double[] firstPoint = pointSamples.get(0).getValueList();
        if (firstPoint == null || firstPoint.length == 0) {
            logger.info("Return early since data points must not be empty.");
            return null;
        }

        int shingleSize = config.getShingleSize();
        int forecastHorizon = ((Forecaster) config).getHorizon();
        int dimensions = firstPoint.length * shingleSize;

        RCFCaster.Builder casterBuilder = RCFCaster
            .builder()
            .dimensions(dimensions)
            .numberOfTrees(numberOfTrees)
            .shingleSize(shingleSize)
            .sampleSize(rcfSampleSize)
            .internalShinglingEnabled(true)
            .precision(Precision.FLOAT_32)
            .anomalyRate(1 - this.thresholdMinPvalue)
            // allow enough samples before emitting scores to park service
            .outputAfter(Math.max(shingleSize, numMinSamples))
            .calibration(Calibration.MINIMAL)
            .timeDecay(config.getTimeDecay())
            .parallelExecutionEnabled(false)
            .boundingBoxCacheFraction(TimeSeriesSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            // the following affects the moving average in many of the transformations
            // the 0.02 corresponds to a half life of 1/0.02 = 50 observations
            // this is different from the timeDecay() of RCF; however it is a similar
            // concept
            .transformDecay(config.getTimeDecay())
            .forecastHorizon(forecastHorizon)
            .initialAcceptFraction(initialAcceptFraction)
            // normalize transform is required to deal with trend change in forecasting
            .transformMethod(TransformMethod.NORMALIZE)
            // for forecasting, we don't support other mode
            .forestMode(ForestMode.STANDARD);

        casterBuilder = applyImputationMethod(config, casterBuilder);

        if (rcfSeed > 0) {
            casterBuilder.randomSeed(rcfSeed);
        }

        RCFCaster caster = casterBuilder.build();

        List<Pair<Instant, Instant>> sequentialTime = new ArrayList<>();
        double[][] sequentialData = new double[pointSamples.size()][firstPoint.length];
        long[] timestamps = new long[pointSamples.size()];
        for (int i = 0; i < pointSamples.size(); i++) {
            Sample dataSample = pointSamples.get(i);
            double[] dataValue = dataSample.getValueList();
            timestamps[i] = dataSample.getDataEndTime().getEpochSecond();
            sequentialData[i] = dataValue;
            sequentialTime.add(Pair.of(dataSample.getDataStartTime(), dataSample.getDataEndTime()));
        }

        List<ForecastResult> res = new ArrayList<>();
        final List<AnomalyDescriptor> descriptors;
        try {
            descriptors = caster.processSequentially(sequentialData, timestamps, x -> true);
        } catch (Exception e) {
            // e.g., out of order timestamps
            logger.error("Error while running processSequentially", e);
            // abort and return no results if the sequence processing fails
            return null;
        }

        if (descriptors.size() != sequentialTime.size()) {
            logger
                .warn(
                    new ParameterizedMessage(
                        "processSequentially returns different size than expected, got [{}], expecting [{}].",
                        descriptors.size(),
                        sequentialTime.size()
                    )
                );
            return null;
        }

        Instant now = Instant.now();
        for (int i = 0; i < descriptors.size(); i++) {
            Pair<Instant, Instant> time = sequentialTime.get(i);
            ForecastDescriptor descriptor = (ForecastDescriptor) descriptors.get(i);
            double[] dataValue = sequentialData[i];
            RCFCasterResult casterResult = ModelUtil.toResult(caster.getForest(), descriptor, dataValue, false);
            List<ForecastResult> resultI = casterResult
                .toIndexableResults(
                    config,
                    time.getLeft(),
                    time.getRight(),
                    now,
                    now,
                    ParseUtils.getFeatureData(dataValue, config),
                    modelState.getEntity(),
                    resultMappingVersion,
                    modelState.getModelId(),
                    taskId,
                    null
                );
            res.addAll(resultI);
        }

        modelState.setModel(caster);

        return res;
    }
}
