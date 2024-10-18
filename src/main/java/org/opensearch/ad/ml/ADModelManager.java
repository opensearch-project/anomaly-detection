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

package org.opensearch.ad.ml;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.ImputedFeatureResult;
import org.opensearch.ad.ratelimit.ADCheckpointWriteWorker;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.timeseries.MemoryTracker;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.feature.Features;
import org.opensearch.timeseries.ml.MemoryAwareConcurrentHashmap;
import org.opensearch.timeseries.ml.ModelColdStart;
import org.opensearch.timeseries.ml.ModelManager;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.DateUtils;
import org.opensearch.timeseries.util.ModelUtil;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.ForestMode;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.config.TransformMethod;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A facade managing ML operations and models.
 */
public class ADModelManager extends
    ModelManager<ThresholdedRandomCutForest, AnomalyResult, ThresholdingResult, ADIndex, ADIndexManagement, ADCheckpointDao, ADCheckpointWriteWorker, ADColdStart> {
    protected static final String ENTITY_SAMPLE = "sp";
    protected static final String ENTITY_RCF = "rcf";
    protected static final String ENTITY_THRESHOLD = "th";

    private static final Logger logger = LogManager.getLogger(ADModelManager.class);

    // states
    private MemoryAwareConcurrentHashmap<ThresholdedRandomCutForest> forests;
    private Map<String, ModelState<ThresholdingModel>> thresholds;

    // configuration

    private final double thresholdMinPvalue;
    private final int minPreviewSize;
    private final Duration modelTtl;
    private Duration checkpointInterval;

    private final double initialAcceptFraction;

    /**
     * Constructor.
     *
     * @param checkpointDao model checkpoint storage
     * @param clock clock for system time
     * @param rcfNumTrees number of trees used in RCF
     * @param rcfNumSamplesInTree number of samples in a RCF tree
     * @param rcfNumMinSamples minimum samples for RCF to score
     * @param thresholdMinPvalue min P-value for thresholding
     * @param minPreviewSize minimum number of data points for preview
     * @param modelTtl time to live for hosted models
     * @param checkpointIntervalSetting setting of interval between checkpoints
     * @param entityColdStarter HCAD cold start utility
     * @param featureManager Used to create features for models
     * @param memoryTracker AD memory usage tracker
     * @param settings Node settings
     * @param clusterService Cluster service accessor
     */
    public ADModelManager(
        ADCheckpointDao checkpointDao,
        Clock clock,
        int rcfNumTrees,
        int rcfNumSamplesInTree,
        int rcfNumMinSamples,
        double thresholdMinPvalue,
        int minPreviewSize,
        Duration modelTtl,
        Setting<TimeValue> checkpointIntervalSetting,
        ADColdStart entityColdStarter,
        FeatureManager featureManager,
        MemoryTracker memoryTracker,
        Settings settings,
        ClusterService clusterService
    ) {
        super(rcfNumTrees, rcfNumSamplesInTree, rcfNumMinSamples, entityColdStarter, memoryTracker, clock, featureManager, checkpointDao);

        this.thresholdMinPvalue = thresholdMinPvalue;
        this.minPreviewSize = minPreviewSize;
        this.modelTtl = modelTtl;
        this.checkpointInterval = DateUtils.toDuration(checkpointIntervalSetting.get(settings));
        if (clusterService != null) {
            clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(checkpointIntervalSetting, it -> this.checkpointInterval = DateUtils.toDuration(it));
        }

        this.forests = new MemoryAwareConcurrentHashmap<>(memoryTracker);
        this.thresholds = new ConcurrentHashMap<>();

        this.initialAcceptFraction = rcfNumMinSamples * 1.0d / rcfNumSamplesInTree;
    }

    @Deprecated
    /**
     * used in RCFResultTransportAction to handle request from old node request.
     * In the new logic, we switch to SingleStreamResultAction.
     *
     * Returns to listener the RCF anomaly result using the specified model.
     *
     * @param detectorId ID of the detector
     * @param modelId ID of the model to score the point
     * @param point features of the data point
     * @param listener onResponse is called with RCF result for the input point, including a score
     *                 onFailure is called with ResourceNotFoundException when the model is not found
     *                 onFailure is called with LimitExceededException when a limit is exceeded for the model
     */
    public void getTRcfResult(String detectorId, String modelId, double[] point, ActionListener<ThresholdingResult> listener) {
        if (forests.containsKey(modelId)) {
            getTRcfResult(forests.get(modelId), point, listener);
        } else {
            checkpointDao
                .getTRCFModel(
                    modelId,
                    ActionListener
                        .wrap(
                            restoredModel -> processRestoredTRcf(restoredModel, modelId, detectorId, point, listener),
                            listener::onFailure
                        )
                );
        }
    }

    // used in RCFResultTransportAction to handle request from old node request.
    // In the new logic, we switch to SingleStreamResultAction.
    @Deprecated
    private void getTRcfResult(
        ModelState<ThresholdedRandomCutForest> modelState,
        double[] point,
        ActionListener<ThresholdingResult> listener
    ) {
        modelState.setLastUsedTime(clock.instant());

        Optional<ThresholdedRandomCutForest> trcfOptional = modelState.getModel();
        if (trcfOptional.isEmpty()) {
            listener.onFailure(new TimeSeriesException("empty model"));
            return;
        }
        try {
            AnomalyDescriptor result = trcfOptional.get().process(point, 0);
            double[] attribution = normalizeAttribution(trcfOptional.get().getForest(), result.getRelevantAttribution());
            listener
                .onResponse(
                    new ThresholdingResult(
                        result.getAnomalyGrade(),
                        result.getDataConfidence(),
                        result.getRCFScore(),
                        result.getTotalUpdates(),
                        result.getRelativeIndex(),
                        attribution,
                        result.getPastValues(),
                        result.getExpectedValuesList(),
                        result.getLikelihoodOfValues(),
                        result.getThreshold(),
                        result.getNumberOfTrees(),
                        point,
                        null
                    )
                );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * normalize total attribution to 1
     *
     * @param forest rcf accessor
     * @param rawAttribution raw attribution scores.  Can be null when
     * 1) the anomaly grade is 0;
     * 2) there are missing values and we are using differenced transforms.
     * Read RCF's ImputePreprocessor.postProcess.
     *
     * @return normalized attribution
     */
    public double[] normalizeAttribution(RandomCutForest forest, double[] rawAttribution) {
        if (forest == null) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Empty forest"));
        }
        // rawAttribution is null when anomaly grade is less than or equals to 0
        // need to create an empty array for bwc because the old node expects an non-empty array
        double[] attribution = createEmptyAttribution(forest);
        if (rawAttribution != null && rawAttribution.length > 0) {
            double sum = Arrays.stream(rawAttribution).sum();
            // avoid dividing by zero error
            if (sum > 0) {
                if (rawAttribution.length != attribution.length) {
                    throw new IllegalArgumentException(
                        String
                            .format(
                                Locale.ROOT,
                                "Unexpected attribution array length: expected %d but is %d",
                                attribution.length,
                                rawAttribution.length
                            )
                    );
                }
                int numFeatures = rawAttribution.length;
                attribution = new double[numFeatures];
                for (int i = 0; i < numFeatures; i++) {
                    attribution[i] = rawAttribution[i] / sum;
                }
            }
        }

        return attribution;
    }

    private double[] createEmptyAttribution(RandomCutForest forest) {
        int shingleSize = forest.getShingleSize();
        if (shingleSize <= 0) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "zero shingle size"));
        }
        int baseDimensions = forest.getDimensions() / shingleSize;
        return new double[baseDimensions];
    }

    Optional<ModelState<ThresholdedRandomCutForest>> restoreModelState(
        Optional<ThresholdedRandomCutForest> rcfModel,
        String modelId,
        String detectorId
    ) {
        if (!rcfModel.isPresent()) {
            return Optional.empty();
        }
        return rcfModel
            .filter(rcf -> memoryTracker.isHostingAllowed(detectorId, rcf))
            .map(rcf -> new ModelState<ThresholdedRandomCutForest>(rcf, modelId, detectorId, ModelManager.ModelType.TRCF.getName(), clock));
    }

    private void processRestoredTRcf(
        Optional<ThresholdedRandomCutForest> rcfModel,
        String modelId,
        String detectorId,
        double[] point,
        ActionListener<ThresholdingResult> listener
    ) {
        Optional<ModelState<ThresholdedRandomCutForest>> model = restoreModelState(rcfModel, modelId, detectorId);
        if (model.isPresent()) {
            forests.put(modelId, model.get());
            getTRcfResult(model.get(), point, listener);
        } else {
            throw new ResourceNotFoundException(detectorId, ADCommonMessages.NO_CHECKPOINT_ERR_MSG + modelId);
        }
    }

    /**
     * Process rcf checkpoint for total rcf updates polling
     * @param checkpointModel rcf model restored from its checkpoint
     * @param modelId model Id
     * @param detectorId detector Id
     * @param listener listener to return total updates of rcf
     */
    private void processRestoredCheckpoint(
        Optional<ThresholdedRandomCutForest> checkpointModel,
        String modelId,
        String detectorId,
        ActionListener<Long> listener
    ) {
        logger.info("Restoring checkpoint for {}", modelId);
        Optional<ModelState<ThresholdedRandomCutForest>> model = restoreModelState(checkpointModel, modelId, detectorId);
        model.ifPresentOrElse(modelState -> {
            forests.put(modelId, modelState);
            modelState.getModel().ifPresent(trcf -> {
                if (trcf.getForest() != null) {
                    listener.onResponse(trcf.getForest().getTotalUpdates());
                } else {
                    listener.onFailure(new ResourceNotFoundException(detectorId, ADCommonMessages.NO_CHECKPOINT_ERR_MSG + modelId));
                }
            });
        }, () -> listener.onFailure(new ResourceNotFoundException(detectorId, ADCommonMessages.NO_CHECKPOINT_ERR_MSG + modelId)));
    }

    /**
     * Returns to listener the result using the specified thresholding model.
     *
     * @param detectorId ID of the detector
     * @param modelId ID of the thresholding model
     * @param score raw anomaly score
     * @param listener onResponse is called with the thresholding model result for the raw score
     *                 onFailure is called with ResourceNotFoundException when the model is not found
     */
    public void getThresholdingResult(String detectorId, String modelId, double score, ActionListener<ThresholdingResult> listener) {
        if (thresholds.containsKey(modelId)) {
            getThresholdingResult(thresholds.get(modelId), score, listener);
        } else {
            checkpointDao
                .getThresholdModel(
                    modelId,
                    ActionListener
                        .wrap(model -> processThresholdCheckpoint(model, modelId, detectorId, score, listener), listener::onFailure)
                );
        }
    }

    private void getThresholdingResult(
        ModelState<ThresholdingModel> modelState,
        double score,
        ActionListener<ThresholdingResult> listener
    ) {
        Optional<ThresholdingModel> thresholdOptional = modelState.getModel();
        if (thresholdOptional.isPresent()) {
            ThresholdingModel threshold = thresholdOptional.get();
            double grade = threshold.grade(score);
            double confidence = threshold.confidence();
            if (score > 0) {
                threshold.update(score);
            }
            modelState.setLastUsedTime(clock.instant());
            listener.onResponse(new ThresholdingResult(grade, confidence, score));
        } else {
            listener
                .onFailure(
                    new ResourceNotFoundException(
                        modelState.getConfigId(),
                        ADCommonMessages.NO_CHECKPOINT_ERR_MSG + modelState.getModelId()
                    )
                );
        }

    }

    private void processThresholdCheckpoint(
        Optional<ThresholdingModel> thresholdModel,
        String modelId,
        String detectorId,
        double score,
        ActionListener<ThresholdingResult> listener
    ) {
        Optional<ModelState<ThresholdingModel>> model = thresholdModel
            .map(threshold -> new ModelState<>(threshold, modelId, detectorId, ModelManager.ModelType.THRESHOLD.getName(), clock));
        if (model.isPresent()) {
            thresholds.put(modelId, model.get());
            getThresholdingResult(model.get(), score, listener);
        } else {
            throw new ResourceNotFoundException(detectorId, ADCommonMessages.NO_CHECKPOINT_ERR_MSG + modelId);
        }
    }

    /**
     * Gets ids of all hosted models.
     *
     * @return ids of all hosted models.
     */
    public Set<String> getAllModelIds() {
        return Stream.of(forests.keySet(), thresholds.keySet()).flatMap(set -> set.stream()).collect(Collectors.toSet());
    }

    /**
     * Gets modelStates of all model partitions hosted on a node
     *
     * @return list of modelStates
     */
    public List<ModelState<?>> getAllModels() {
        return Stream.concat(forests.values().stream(), thresholds.values().stream()).collect(Collectors.toList());
    }

    /**
     * Stops hosting the model and creates a checkpoint.
     *
     * Used when adding a OpenSearch node.  We have to stop all models because
     * requests for those model ids would be sent to other nodes. If we don't stop
     * them, there would be memory leak.
     *
     * @param detectorId ID of the detector
     * @param modelId ID of the model to stop hosting
     * @param listener onResponse is called with null when the operation is completed
     */
    public void stopModel(String detectorId, String modelId, ActionListener<Void> listener) {
        logger.info(String.format(Locale.ROOT, "Stopping detector %s model %s", detectorId, modelId));
        stopModel(forests, modelId, ActionListener.wrap(r -> stopModel(thresholds, modelId, listener), listener::onFailure));
    }

    private <T> void stopModel(Map<String, ModelState<T>> models, String modelId, ActionListener<Void> listener) {
        Instant now = clock.instant();
        Optional<ModelState<T>> modelState = Optional
            .ofNullable(models.remove(modelId))
            .filter(model -> model.getLastCheckpointTime().plus(checkpointInterval).isBefore(now));
        if (modelState.isPresent() && modelState.get().getModel().isPresent()) {
            T model = modelState.get().getModel().get();
            if (model instanceof ThresholdedRandomCutForest) {
                checkpointDao
                    .putTRCFCheckpoint(
                        modelId,
                        (ThresholdedRandomCutForest) model,
                        ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
                    );
            } else if (model instanceof ThresholdingModel) {
                checkpointDao
                    .putThresholdCheckpoint(
                        modelId,
                        (ThresholdingModel) model,
                        ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)
                    );
            } else {
                listener.onFailure(new IllegalArgumentException("Unexpected model type"));
            }
        } else {
            listener.onResponse(null);
        }
    }

    /**
     * Permanently deletes models hosted in memory and persisted in index.
     * When stop realtime job, will call this method to clear all model cache
     * and checkpoints.
     *
     * @param detectorId id the of the detector for which models are to be permanently deleted
     * @param listener onResponse is called with null when this operation is completed
     */
    public void clear(String detectorId, ActionListener<Void> listener) {
        clearModels(detectorId, forests, ActionListener.wrap(r -> clearModels(detectorId, thresholds, listener), listener::onFailure));
    }

    /**
     * Does model maintenance.
     *
     * The implementation makes checkpoints for hosted models and stops hosting models not recently used.
     *
     * @param listener onResponse is called with null when this operation is completed.
     */
    public void maintenance(ActionListener<Void> listener) {
        maintenanceForIterator(
            forests,
            forests.entrySet().iterator(),
            ActionListener.wrap(r -> maintenanceForIterator(thresholds, thresholds.entrySet().iterator(), listener), listener::onFailure)
        );
    }

    private <T> void maintenanceForIterator(
        Map<String, ModelState<T>> models,
        Iterator<Entry<String, ModelState<T>>> iter,
        ActionListener<Void> listener
    ) {
        if (iter.hasNext()) {
            Entry<String, ModelState<T>> modelEntry = iter.next();
            String modelId = modelEntry.getKey();
            ModelState<T> modelState = modelEntry.getValue();
            Instant now = clock.instant();
            if (modelState.expired(modelTtl)) {
                models.remove(modelId);
            }
            if (modelState.getLastCheckpointTime().plus(checkpointInterval).isBefore(now)) {
                ActionListener<Void> checkpointListener = ActionListener.wrap(r -> {
                    modelState.setLastCheckpointTime(now);
                    maintenanceForIterator(models, iter, listener);
                }, e -> {
                    logger.warn("Failed to finish maintenance for model id " + modelId, e);
                    maintenanceForIterator(models, iter, listener);
                });
                Optional<T> modelOptional = modelState.getModel();
                if (modelOptional.isPresent()) {
                    T model = modelOptional.get();
                    if (model instanceof ThresholdedRandomCutForest) {
                        checkpointDao.putTRCFCheckpoint(modelId, (ThresholdedRandomCutForest) model, checkpointListener);
                    } else if (model instanceof ThresholdingModel) {
                        checkpointDao.putThresholdCheckpoint(modelId, (ThresholdingModel) model, checkpointListener);
                    } else {
                        checkpointListener.onFailure(new IllegalArgumentException("Unexpected model type"));
                    }
                } else {
                    maintenanceForIterator(models, iter, listener);
                }
            } else {
                maintenanceForIterator(models, iter, listener);
            }
        } else {
            listener.onResponse(null);
        }
    }

    /**
     * Returns computed anomaly results for preview data points.
     *
     * @param features features of preview data points
     * @param detector Anomaly detector
     * @throws IllegalArgumentException when preview data points are not valid
     */
    public List<ThresholdingResult> getPreviewResults(Features features, AnomalyDetector detector) {
        double[][] dataPoints = features.getUnprocessedFeatures();
        if (dataPoints.length < minPreviewSize) {
            throw new IllegalArgumentException("Insufficient data for preview results. Minimum required: " + minPreviewSize);
        }
        List<Entry<Long, Long>> timeRanges = features.getTimeRanges();
        if (timeRanges.size() != dataPoints.length) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "time range size %d does not match data points size %d", timeRanges.size(), dataPoints.length)
            );
        }

        int shingleSize = detector.getShingleSize();
        double rcfTimeDecay = detector.getTimeDecay();

        // Train RCF models and collect non-zero scores
        int baseDimension = dataPoints[0].length;
        // speed is important in preview. We don't want cx to wait too long.
        // thus use the default value of boundingBoxCacheFraction = 1
        ThresholdedRandomCutForest.Builder trcfBuilder = ThresholdedRandomCutForest
            .builder()
            .randomSeed(0L)
            .dimensions(baseDimension * shingleSize)
            .sampleSize(rcfNumSamplesInTree)
            .numberOfTrees(rcfNumTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(rcfNumMinSamples)
            .initialAcceptFraction(initialAcceptFraction)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(TimeSeriesSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            .shingleSize(shingleSize)
            .anomalyRate(1 - this.thresholdMinPvalue)
            .transformMethod(TransformMethod.NORMALIZE)
            .alertOnce(true)
            .autoAdjust(true)
            .internalShinglingEnabled(true);

        if (shingleSize > 1) {
            trcfBuilder.forestMode(ForestMode.STREAMING_IMPUTE);
            trcfBuilder = ModelColdStart.applyImputationMethod(detector, trcfBuilder);
        } else {
            // imputation with shingle size 1 is not meaningful
            trcfBuilder.forestMode(ForestMode.STANDARD);
        }

        ADColdStart.applyRule(trcfBuilder, detector);

        ThresholdedRandomCutForest trcf = trcfBuilder.build();

        return IntStream.range(0, dataPoints.length).mapToObj(i -> {
            // we don't have missing values in preview data. We have already filtered them out.
            double[] point = dataPoints[i];
            // Get the data end epoch milliseconds corresponding to this index and convert it to seconds
            long timestampSecs = timeRanges.get(i).getValue() / 1000;
            AnomalyDescriptor descriptor = trcf.process(point, timestampSecs); // Use the timestamp here

            if (descriptor != null) {
                return toResult(trcf.getForest(), descriptor, point, false, detector);
            }

            return null;
        }).collect(Collectors.toList());
    }

    /**
     * Get a RCF model's total updates.
     * @param modelId the RCF model's id
     * @param detectorId detector Id
     * @param listener listener to return the result
     */
    public void getTotalUpdates(String modelId, String detectorId, ActionListener<Long> listener) {
        ModelState<ThresholdedRandomCutForest> model = forests.get(modelId);
        if (model != null) {
            if (model.getModel().isPresent() && model.getModel().get().getForest() != null) {
                listener.onResponse(model.getModel().get().getForest().getTotalUpdates());
            } else {
                listener.onResponse(0L);
            }
        } else {
            checkpointDao
                .getTRCFModel(
                    modelId,
                    ActionListener
                        .wrap(checkpoint -> processRestoredCheckpoint(checkpoint, modelId, detectorId, listener), listener::onFailure)
                );
        }
    }

    @Override
    protected ThresholdingResult createEmptyResult() {
        return new ThresholdingResult(0, 0, 0);
    }

    @Override
    protected ThresholdingResult toResult(
        RandomCutForest rcf,
        AnomalyDescriptor anomalyDescriptor,
        double[] point,
        boolean isImputed,
        Config config
    ) {
        ImputedFeatureResult result = ModelUtil.calculateImputedFeatures(anomalyDescriptor, point, isImputed, config);

        return new ThresholdingResult(
            anomalyDescriptor.getAnomalyGrade(),
            anomalyDescriptor.getDataConfidence(),
            anomalyDescriptor.getRCFScore(),
            anomalyDescriptor.getTotalUpdates(),
            anomalyDescriptor.getRelativeIndex(),
            normalizeAttribution(rcf, anomalyDescriptor.getRelevantAttribution()),
            anomalyDescriptor.getPastValues(),
            anomalyDescriptor.getExpectedValuesList(),
            anomalyDescriptor.getLikelihoodOfValues(),
            anomalyDescriptor.getThreshold(),
            rcfNumTrees,
            result.getActual(),
            result.getIsFeatureImputed()
        );
    }
}
