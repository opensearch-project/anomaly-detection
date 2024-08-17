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

package org.opensearch.ad.task;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.opensearch.ad.ml.ADColdStart;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.settings.TimeSeriesSettings;

import com.amazon.randomcutforest.config.ForestMode;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * AD batch task cache which will mainly hold these for one task:
 * 1. RCF
 * 2. threshold model
 * 3. shingle
 * 4. training data
 * 5. entity if task is for HC detector
 */
public class ADBatchTaskCache {
    private final String detectorId;
    private final String taskId;
    private final String detectorTaskId;
    private ThresholdedRandomCutForest rcfModel;
    private boolean thresholdModelTrained;
    private AtomicInteger thresholdModelTrainingDataSize = new AtomicInteger(0);
    private AtomicBoolean cancelled = new AtomicBoolean(false);
    private AtomicLong cacheMemorySize = new AtomicLong(0);
    private String cancelReason;
    private String cancelledBy;
    private Entity entity;

    protected ADBatchTaskCache(ADTask adTask) {
        this.detectorId = adTask.getConfigId();
        this.taskId = adTask.getTaskId();
        this.detectorTaskId = adTask.getConfigLevelTaskId();
        this.entity = adTask.getEntity();

        AnomalyDetector detector = adTask.getDetector();
        int numberOfTrees = TimeSeriesSettings.NUM_TREES;
        int shingleSize = detector.getShingleSize();
        int dimensions = detector.getShingleSize() * detector.getEnabledFeatureIds().size();

        ThresholdedRandomCutForest.Builder rcfBuilder = ThresholdedRandomCutForest
            .builder()
            .dimensions(dimensions)
            .numberOfTrees(numberOfTrees)
            .timeDecay(detector.getTimeDecay())
            .sampleSize(TimeSeriesSettings.NUM_SAMPLES_PER_TREE)
            .outputAfter(TimeSeriesSettings.NUM_MIN_SAMPLES)
            .initialAcceptFraction(TimeSeriesSettings.NUM_MIN_SAMPLES * 1.0d / TimeSeriesSettings.NUM_SAMPLES_PER_TREE)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            .shingleSize(shingleSize)
            .anomalyRate(1 - TimeSeriesSettings.THRESHOLD_MIN_PVALUE)
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .internalShinglingEnabled(true);

        if (shingleSize > 1) {
            rcfBuilder.forestMode(ForestMode.STREAMING_IMPUTE);
            rcfBuilder = ModelColdStart.applyImputationMethod(detector, rcfBuilder);
        } else {
            // imputation with shingle size 1 is not meaningful
            rcfBuilder.forestMode(ForestMode.STANDARD);
        }

        ADColdStart.applyRule(rcfBuilder, detector);

        rcfModel = rcfBuilder.build();
        this.thresholdModelTrained = false;
    }

    protected String getId() {
        return detectorId;
    }

    protected String getTaskId() {
        return taskId;
    }

    protected String getDetectorTaskId() {
        return detectorTaskId;
    }

    protected ThresholdedRandomCutForest getTRcfModel() {
        return rcfModel;
    }

    protected void setThresholdModelTrained(boolean thresholdModelTrained) {
        this.thresholdModelTrained = thresholdModelTrained;
    }

    protected boolean isThresholdModelTrained() {
        return thresholdModelTrained;
    }

    public AtomicInteger getThresholdModelTrainingDataSize() {
        return thresholdModelTrainingDataSize;
    }

    protected AtomicLong getCacheMemorySize() {
        return cacheMemorySize;
    }

    protected boolean isCancelled() {
        return cancelled.get();
    }

    protected String getCancelReason() {
        return cancelReason;
    }

    protected String getCancelledBy() {
        return cancelledBy;
    }

    public Entity getEntity() {
        return entity;
    }

    protected void cancel(String reason, String userName) {
        this.cancelled.compareAndSet(false, true);
        this.cancelReason = reason;
        this.cancelledBy = userName;
    }
}
