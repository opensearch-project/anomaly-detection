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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.ml;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.DetectorModelSize;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.ml.ModelManager.ModelType;
import org.opensearch.ad.ml.rcf.CombinedRcfResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.settings.AnomalyDetectorSettings;

import com.amazon.randomcutforest.ERCF.AnomalyDescriptor;
import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.returntypes.DiVector;

/**
 * A facade managing ML operations and models.
 */
public class ModelManager implements DetectorModelSize {
    protected static final String DETECTOR_ID_PATTERN = "(.*)_model_.+";

    protected static final String ENTITY_SAMPLE = "sp";
    protected static final String ENTITY_RCF = "rcf";
    protected static final String ENTITY_THRESHOLD = "th";

    public enum ModelType {
        RCF("rcf"),
        THRESHOLD("threshold"),
        ENTITY("entity");

        private String name;

        ModelType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    private static final double FULL_CONFIDENCE_EXPONENT = 18.43; // exponent over which confidence is 1

    private static final Logger logger = LogManager.getLogger(ModelManager.class);

    // states
    private RCFMemoryAwareConcurrentHashmap<String> forests;
    private Map<String, ModelState<ThresholdingModel>> thresholds;

    // configuration
    private final int rcfNumTrees;
    private final int rcfNumSamplesInTree;
    private final double rcfTimeDecay;
    private final int rcfNumMinSamples;
    private final double thresholdMinPvalue;
    private final double thresholdMaxRankError;
    private final double thresholdMaxScore;
    private final int thresholdNumLogNormalQuantiles;
    private final int thresholdDownsamples;
    private final long thresholdMaxSamples;
    private final int minPreviewSize;
    private final Duration modelTtl;
    private final Duration checkpointInterval;

    // dependencies
    private final CheckpointDao checkpointDao;
    private final Clock clock;
    public FeatureManager featureManager;

    private EntityColdStarter entityColdStarter;
    private ModelPartitioner modelPartitioner;
    private MemoryTracker memoryTracker;

    /**
     * Constructor.
     *
     * @param checkpointDao model checkpoint storage
     * @param clock clock for system time
     * @param rcfNumTrees number of trees used in RCF
     * @param rcfNumSamplesInTree number of samples in a RCF tree
     * @param rcfTimeDecay time decay for RCF
     * @param rcfNumMinSamples minimum samples for RCF to score
     * @param thresholdMinPvalue min P-value for thresholding
     * @param thresholdMaxRankError  max rank error for thresholding
     * @param thresholdMaxScore max RCF score to thresholding
     * @param thresholdNumLogNormalQuantiles num of lognormal quantiles for thresholding
     * @param thresholdDownsamples the number of samples to keep during downsampling
     * @param thresholdMaxSamples the max number of samples before downsampling
     * @param minPreviewSize minimum number of data points for preview
     * @param modelTtl time to live for hosted models
     * @param checkpointInterval interval between checkpoints
     * @param entityColdStarter HCAD cold start utility
     * @param modelPartitioner Used to partition RCF models
     * @param featureManager Used to create features for models
     * @param memoryTracker AD memory usage tracker
     */
    public ModelManager(
        CheckpointDao checkpointDao,
        Clock clock,
        int rcfNumTrees,
        int rcfNumSamplesInTree,
        double rcfTimeDecay,
        int rcfNumMinSamples,
        double thresholdMinPvalue,
        double thresholdMaxRankError,
        double thresholdMaxScore,
        int thresholdNumLogNormalQuantiles,
        int thresholdDownsamples,
        long thresholdMaxSamples,
        int minPreviewSize,
        Duration modelTtl,
        Duration checkpointInterval,
        EntityColdStarter entityColdStarter,
        ModelPartitioner modelPartitioner,
        FeatureManager featureManager,
        MemoryTracker memoryTracker
    ) {
        this.checkpointDao = checkpointDao;
        this.clock = clock;
        this.rcfNumTrees = rcfNumTrees;
        this.rcfNumSamplesInTree = rcfNumSamplesInTree;
        this.rcfTimeDecay = rcfTimeDecay;
        this.rcfNumMinSamples = rcfNumMinSamples;
        this.thresholdMinPvalue = thresholdMinPvalue;
        this.thresholdMaxRankError = thresholdMaxRankError;
        this.thresholdMaxScore = thresholdMaxScore;
        this.thresholdNumLogNormalQuantiles = thresholdNumLogNormalQuantiles;
        this.thresholdDownsamples = thresholdDownsamples;
        this.thresholdMaxSamples = thresholdMaxSamples;
        this.minPreviewSize = minPreviewSize;
        this.modelTtl = modelTtl;
        this.checkpointInterval = checkpointInterval;

        this.forests = new RCFMemoryAwareConcurrentHashmap<>(memoryTracker);
        this.thresholds = new ConcurrentHashMap<>();

        this.entityColdStarter = entityColdStarter;
        this.modelPartitioner = modelPartitioner;
        this.featureManager = featureManager;
        this.memoryTracker = memoryTracker;
    }

    /**
     * Combines RCF results into a single result.
     *
     * Final RCF score is calculated by averaging scores weighted by model size (number of trees).
     * Confidence is the weighted average of confidence with confidence for missing models being 0.
     * Attribution is normalized weighted average for the most recent feature dimensions.
     *
     * @param rcfResults RCF results from partitioned models
     * @param numFeatures number of features for attribution
     * @return combined RCF result
     */
    public CombinedRcfResult combineRcfResults(List<RcfResult> rcfResults, int numFeatures) {
        CombinedRcfResult combinedResult = null;
        if (rcfResults.isEmpty()) {
            combinedResult = new CombinedRcfResult(0, 0, new double[0]);
        } else {
            int totalForestSize = rcfResults.stream().mapToInt(RcfResult::getForestSize).sum();
            if (totalForestSize == 0) {
                combinedResult = new CombinedRcfResult(0, 0, new double[0]);
            } else {
                double score = rcfResults.stream().mapToDouble(r -> r.getScore() * r.getForestSize()).sum() / totalForestSize;
                double confidence = rcfResults.stream().mapToDouble(r -> r.getConfidence() * r.getForestSize()).sum() / Math
                    .max(rcfNumTrees, totalForestSize);
                double[] attribution = combineAttribution(rcfResults, numFeatures, totalForestSize);
                combinedResult = new CombinedRcfResult(score, confidence, attribution);
            }
        }
        return combinedResult;
    }

    private double[] combineAttribution(List<RcfResult> rcfResults, int numFeatures, int totalForestSize) {
        double[] combined = new double[numFeatures];
        double sum = 0;
        for (RcfResult result : rcfResults) {
            double[] attribution = result.getAttribution();
            for (int i = 0; i < numFeatures; i++) {
                double attr = attribution[attribution.length - numFeatures + i] * result.getForestSize() / totalForestSize;
                combined[i] += attr;
                sum += attr;
            }
        }
        for (int i = 0; i < numFeatures; i++) {
            combined[i] /= sum;
        }
        return combined;
    }

    /**
     * Gets the detector id from the model id.
     *
     * @param modelId id of a model
     * @return id of the detector the model is for
     * @throws IllegalArgumentException if model id is invalid
     */
    public String getDetectorIdForModelId(String modelId) {
        Matcher matcher = Pattern.compile(DETECTOR_ID_PATTERN).matcher(modelId);
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            throw new IllegalArgumentException("Invalid model id " + modelId);
        }
    }

    /**
     * Returns to listener the RCF anomaly result using the specified model.
     *
     * @param detectorId ID of the detector
     * @param modelId ID of the model to score the point
     * @param point features of the data point
     * @param listener onResponse is called with RCF result for the input point, including a score
     *                 onFailure is called with ResourceNotFoundException when the model is not found
     *                 onFailure is called with LimitExceededException when a limit is exceeded for the model
     */
    public void getRcfResult(String detectorId, String modelId, double[] point, ActionListener<RcfResult> listener) {
        if (forests.containsKey(modelId)) {
            getRcfResult(forests.get(modelId), point, listener);
        } else {
            checkpointDao
                .getRCFModel(
                    modelId,
                    ActionListener
                        .wrap(checkpoint -> processRcfCheckpoint(checkpoint, modelId, detectorId, point, listener), listener::onFailure)
                );
        }
    }

    private void getRcfResult(ModelState<RandomCutForest> modelState, double[] point, ActionListener<RcfResult> listener) {
        modelState.setLastUsedTime(clock.instant());

        RandomCutForest rcf = modelState.getModel();
        double score = rcf.getAnomalyScore(point);
        double confidence = computeRcfConfidence(rcf);
        int forestSize = rcf.getNumberOfTrees();
        double[] attribution = getAnomalyAttribution(rcf, point);
        rcf.update(point);
        long totalUpdates = rcf.getTotalUpdates();
        listener.onResponse(new RcfResult(score, confidence, forestSize, attribution, rcf.getTotalUpdates()));
    }

    private double[] getAnomalyAttribution(RandomCutForest rcf, double[] point) {
        DiVector vec = rcf.getAnomalyAttribution(point);
        vec.renormalize(1d);
        double[] attribution = new double[vec.getDimensions()];
        for (int i = 0; i < attribution.length; i++) {
            attribution[i] = vec.getHighLowSum(i);
        }
        return attribution;
    }

    private Optional<ModelState<RandomCutForest>> restoreCheckpoint(
        Optional<RandomCutForest> rcfCheckpoint,
        String modelId,
        String detectorId
    ) {
        if (!rcfCheckpoint.isPresent()) {
            return Optional.empty();
        }

        return rcfCheckpoint
            .filter(rcf -> memoryTracker.isHostingAllowed(detectorId, rcf))
            .map(rcf -> ModelState.createSingleEntityModelState(rcf, modelId, detectorId, ModelType.RCF.getName(), clock));
    }

    private void processRcfCheckpoint(
        Optional<RandomCutForest> rcfCheckpoint,
        String modelId,
        String detectorId,
        double[] point,
        ActionListener<RcfResult> listener
    ) {
        Optional<ModelState<RandomCutForest>> model = restoreCheckpoint(rcfCheckpoint, modelId, detectorId);
        if (model.isPresent()) {
            forests.put(modelId, model.get());
            getRcfResult(model.get(), point, listener);
        } else {
            throw new ResourceNotFoundException(detectorId, CommonErrorMessages.NO_CHECKPOINT_ERR_MSG + modelId);
        }
    }

    /**
     * Process rcf checkpoint for total rcf updates polling
     * @param checkpointModel rcf model restored from its checkpoint
     * @param modelId model Id
     * @param detectorId detector Id
     * @param listener listener to return total updates of rcf
     */
    private void processRcfCheckpoint(
        Optional<RandomCutForest> checkpointModel,
        String modelId,
        String detectorId,
        ActionListener<Long> listener
    ) {
        logger.info("Restoring checkpoint for {}", modelId);
        Optional<ModelState<RandomCutForest>> model = restoreCheckpoint(checkpointModel, modelId, detectorId);
        if (model.isPresent()) {
            forests.put(modelId, model.get());
            listener.onResponse(model.get().getModel().getTotalUpdates());
        } else {
            listener.onFailure(new ResourceNotFoundException(detectorId, CommonErrorMessages.NO_CHECKPOINT_ERR_MSG + modelId));
        }
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
        ThresholdingModel threshold = modelState.getModel();
        double grade = threshold.grade(score);
        double confidence = threshold.confidence();
        if (score > 0) {
            threshold.update(score);
        }
        modelState.setLastUsedTime(clock.instant());
        listener.onResponse(new ThresholdingResult(grade, confidence, score));
    }

    private void processThresholdCheckpoint(
        Optional<ThresholdingModel> thresholdModel,
        String modelId,
        String detectorId,
        double score,
        ActionListener<ThresholdingResult> listener
    ) {
        Optional<ModelState<ThresholdingModel>> model = thresholdModel
            .map(
                threshold -> ModelState.createSingleEntityModelState(threshold, modelId, detectorId, ModelType.THRESHOLD.getName(), clock)
            );
        if (model.isPresent()) {
            thresholds.put(modelId, model.get());
            getThresholdingResult(model.get(), score, listener);
        } else {
            throw new ResourceNotFoundException(detectorId, CommonErrorMessages.NO_CHECKPOINT_ERR_MSG + modelId);
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
        if (modelState.isPresent()) {
            T model = modelState.get().getModel();
            if (model instanceof RandomCutForest) {
                checkpointDao
                    .putRCFCheckpoint(
                        modelId,
                        (RandomCutForest) model,
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
     *
     * @param detectorId id the of the detector for which models are to be permanently deleted
     * @param listener onResponse is called with null when this operation is completed
     */
    public void clear(String detectorId, ActionListener<Void> listener) {
        clearModels(detectorId, forests, ActionListener.wrap(r -> clearModels(detectorId, thresholds, listener), listener::onFailure));
    }

    private void clearModels(String detectorId, Map<String, ?> models, ActionListener<Void> listener) {
        Iterator<String> id = models.keySet().iterator();
        clearModelForIterator(detectorId, models, id, listener);
    }

    private void clearModelForIterator(String detectorId, Map<String, ?> models, Iterator<String> idIter, ActionListener<Void> listener) {
        if (idIter.hasNext()) {
            String modelId = idIter.next();
            if (getDetectorIdForModelId(modelId).equals(detectorId)) {
                models.remove(modelId);
                checkpointDao
                    .deleteModelCheckpoint(
                        modelId,
                        ActionListener.wrap(r -> clearModelForIterator(detectorId, models, idIter, listener), listener::onFailure)
                    );
            } else {
                clearModelForIterator(detectorId, models, idIter, listener);
            }
        } else {
            listener.onResponse(null);
        }
    }

    /**
    * Trains and saves cold-start AD models.
    *
    * This implementations splits RCF models and trains them all.
    * As all model partitions have the same size, the scores from RCF models are merged by averaging.
    * Since RCF outputs 0 until it is ready, initial 0 scores are meaningless and therefore filtered out.
    * Filtered (non-zero) RCF scores are the training data for a single thresholding model.
    * All trained models are serialized and persisted to be hosted.
    *
    * @param anomalyDetector the detector for which models are trained
    * @param dataPoints M, N shape, where M is the number of samples for training and N is the number of features
    * @param listener onResponse is called with null when this operation is completed
    *                 onFailure is called IllegalArgumentException when training data is invalid
    *                 onFailure is called LimitExceededException when a limit for training is exceeded
    */
    public void trainModel(AnomalyDetector anomalyDetector, double[][] dataPoints, ActionListener<Void> listener) {
        if (dataPoints.length == 0 || dataPoints[0].length == 0) {
            listener.onFailure(new IllegalArgumentException("Data points must not be empty."));
        } else {
            int rcfNumFeatures = dataPoints[0].length;
            // creates partitioned RCF models
            try {
                Entry<Integer, Integer> partitionResults = modelPartitioner
                    .getPartitionedForestSizes(
                        RandomCutForest
                            .builder()
                            .dimensions(rcfNumFeatures)
                            .sampleSize(rcfNumSamplesInTree)
                            .numberOfTrees(rcfNumTrees)
                            .outputAfter(rcfNumSamplesInTree)
                            .parallelExecutionEnabled(false)
                            .compact(true)
                            .precision(Precision.FLOAT_32)
                            .boundingBoxCacheFraction(AnomalyDetectorSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
                            // same with dimension for opportunistic memory saving
                            .shingleSize(anomalyDetector.getShingleSize())
                            .build(),
                        anomalyDetector.getDetectorId()
                    );
                int numForests = partitionResults.getKey();
                int forestSize = partitionResults.getValue();
                double[] scores = new double[dataPoints.length];
                Arrays.fill(scores, 0.);
                trainModelForStep(anomalyDetector, dataPoints, rcfNumFeatures, numForests, forestSize, scores, 0, listener);
            } catch (LimitExceededException e) {
                listener.onFailure(e);
            }
        }
    }

    private void trainModelForStep(
        AnomalyDetector detector,
        double[][] dataPoints,
        int rcfNumFeatures,
        int numForests,
        int forestSize,
        final double[] scores,
        int step,
        ActionListener<Void> listener
    ) {
        if (step < numForests) {
            RandomCutForest rcf = RandomCutForest
                .builder()
                .dimensions(rcfNumFeatures)
                .sampleSize(rcfNumSamplesInTree)
                .numberOfTrees(forestSize)
                .timeDecay(rcfTimeDecay)
                .outputAfter(rcfNumMinSamples)
                .parallelExecutionEnabled(false)
                .compact(true)
                .precision(Precision.FLOAT_32)
                .boundingBoxCacheFraction(AnomalyDetectorSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
                // same with dimension for opportunistic memory saving
                .shingleSize(detector.getShingleSize())
                .build();
            for (int j = 0; j < dataPoints.length; j++) {
                scores[j] += rcf.getAnomalyScore(dataPoints[j]);
                rcf.update(dataPoints[j]);
            }
            String modelId = modelPartitioner.getRcfModelId(detector.getDetectorId(), step);
            checkpointDao
                .putRCFCheckpoint(
                    modelId,
                    rcf,
                    ActionListener
                        .wrap(
                            r -> trainModelForStep(
                                detector,
                                dataPoints,
                                rcfNumFeatures,
                                numForests,
                                forestSize,
                                scores,
                                step + 1,
                                listener
                            ),
                            listener::onFailure
                        )
                );
        } else {
            double[] rcfScores = DoubleStream.of(scores).filter(score -> score > 0).map(score -> score / numForests).toArray();

            // Train thresholding model
            ThresholdingModel threshold = new HybridThresholdingModel(
                thresholdMinPvalue,
                thresholdMaxRankError,
                thresholdMaxScore,
                thresholdNumLogNormalQuantiles,
                thresholdDownsamples,
                thresholdMaxSamples
            );
            threshold.train(rcfScores);

            // Persist thresholding model
            String modelId = modelPartitioner.getThresholdModelId(detector.getDetectorId());
            checkpointDao
                .putThresholdCheckpoint(modelId, threshold, ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure));
        }
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
                T model = modelState.getModel();
                if (model instanceof RandomCutForest) {
                    checkpointDao.putRCFCheckpoint(modelId, (RandomCutForest) model, checkpointListener);
                } else if (model instanceof ThresholdingModel) {
                    checkpointDao.putThresholdCheckpoint(modelId, (ThresholdingModel) model, checkpointListener);
                } else {
                    checkpointListener.onFailure(new IllegalArgumentException("Unexpected model type"));
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
     * @param dataPoints features of preview data points
     * @param shingleSize model shingle size
     * @return thresholding results of preview data points
     * @throws IllegalArgumentException when preview data points are not valid
     */
    public List<ThresholdingResult> getPreviewResults(double[][] dataPoints, int shingleSize) {
        if (dataPoints.length < minPreviewSize) {
            throw new IllegalArgumentException("Insufficient data for preview results. Minimum required: " + minPreviewSize);
        }
        // Train RCF models and collect non-zero scores
        int rcfNumFeatures = dataPoints[0].length;
        // speed is important in preview. We don't want cx to wait too long.
        // thus use the default value of boundingBoxCacheFraction = 1
        RandomCutForest forest = RandomCutForest
            .builder()
            .randomSeed(0L)
            .dimensions(rcfNumFeatures)
            .sampleSize(rcfNumSamplesInTree)
            .numberOfTrees(rcfNumTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(rcfNumSamplesInTree)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(AnomalyDetectorSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            // same with dimension for opportunistic memory saving
            .shingleSize(shingleSize)
            .build();
        double[] rcfScores = Arrays.stream(dataPoints).mapToDouble(point -> {
            double score = forest.getAnomalyScore(point);
            forest.update(point);
            return score;
        }).filter(score -> score > 0.).toArray();
        // Train thresholding model
        ThresholdingModel threshold = new HybridThresholdingModel(
            thresholdMinPvalue,
            thresholdMaxRankError,
            thresholdMaxScore,
            thresholdNumLogNormalQuantiles,
            thresholdDownsamples,
            thresholdMaxSamples
        );
        threshold.train(rcfScores);

        // Get results from trained models
        return Arrays.stream(dataPoints).map(point -> {
            double rcfScore = forest.getAnomalyScore(point);
            forest.update(point);
            ThresholdingResult result = new ThresholdingResult(threshold.grade(rcfScore), threshold.confidence(), rcfScore);
            threshold.update(rcfScore);
            return result;
        }).collect(Collectors.toList());
    }

    /**
     * Computes the probabilities of non-coldstart points in the current forest.
     */
    private double computeRcfConfidence(RandomCutForest forest) {
        long total = forest.getTotalUpdates();
        double lambda = forest.getTimeDecay();
        double totalExponent = total * lambda;
        if (totalExponent >= FULL_CONFIDENCE_EXPONENT) {
            return 1.;
        } else {
            double eTotal = Math.exp(totalExponent);
            double confidence = (eTotal - Math.exp(lambda * Math.min(total, forest.getSampleSize()))) / (eTotal - 1);
            return Math.max(0, confidence); // Replaces -0 wth 0 for cosmetic purpose.
        }
    }

    /**
     * Get all RCF partition's size corresponding to a detector.  Thresholding models' size is a constant since they are small in size (KB).
     * @param detectorId detector id
     * @return a map of model id to its memory size
     */
    @Override
    public Map<String, Long> getModelSize(String detectorId) {
        Map<String, Long> res = new HashMap<>();
        forests
            .entrySet()
            .stream()
            .filter(entry -> getDetectorIdForModelId(entry.getKey()).equals(detectorId))
            .forEach(entry -> { res.put(entry.getKey(), memoryTracker.estimateRCFModelSize(entry.getValue().getModel())); });
        thresholds
            .entrySet()
            .stream()
            .filter(entry -> getDetectorIdForModelId(entry.getKey()).equals(detectorId))
            .forEach(entry -> { res.put(entry.getKey(), (long) memoryTracker.getThresholdModelBytes()); });
        return res;
    }

    /**
     * Get a RCF model's total updates.
     * @param modelId the RCF model's id
     * @param detectorId detector Id
     * @param listener listener to return the result
     */
    public void getTotalUpdates(String modelId, String detectorId, ActionListener<Long> listener) {
        ModelState<RandomCutForest> model = forests.get(modelId);
        if (model != null) {
            listener.onResponse(model.getModel().getTotalUpdates());
        } else {
            checkpointDao
                .getRCFModel(
                    modelId,
                    ActionListener.wrap(checkpoint -> processRcfCheckpoint(checkpoint, modelId, detectorId, listener), listener::onFailure)
                );
        }
    }

    /**
     * Compute anomaly result for the given data point
     * @param datapoint Data point
     * @param modelState the state associated with the entity
     * @param modelId the model Id
     * @param detector Detector accessor
     * @param entity entity accessor
     *
     * @return anomaly result, confidence, and the corresponding RCF score.
     */
    public ThresholdingResult getAnomalyResultForEntity(
        double[] datapoint,
        ModelState<EntityModel> modelState,
        String modelId,
        AnomalyDetector detector,
        Entity entity
    ) {
        ThresholdingResult result = new ThresholdingResult(0, 0, 0);
        if (modelState != null) {
            EntityModel entityModel = modelState.getModel();

            if (entityModel == null) {
                entityModel = new EntityModel(entity, new ArrayDeque<>(), null, null);
                modelState.setModel(entityModel);
            }

            if (entityModel.getErcf().isPresent()) {
                result = toResult(entityModel.getErcf().get().process(datapoint));
            } else {
                // trainModelFromExistingSamples may be able to make models not null
                if (entityModel.getRcf() == null || entityModel.getThreshold() == null) {
                    entityColdStarter.trainModelFromExistingSamples(modelState);
                }

                if (entityModel.getRcf() != null && entityModel.getThreshold() != null) {
                    return score(datapoint, modelId, modelState);
                } else {
                    entityModel.addSample(datapoint);
                    return new ThresholdingResult(0, 0, 0);
                }
            }
        } else {
            return new ThresholdingResult(0, 0, 0);
        }
        return result;
    }

    public ThresholdingResult score(double[] feature, String modelId, ModelState<EntityModel> modelState) {
        EntityModel model = modelState.getModel();
        if (model == null) {
            return new ThresholdingResult(0, 0, 0);
        }
        RandomCutForest rcf = model.getRcf();
        ThresholdingModel threshold = model.getThreshold();
        if (rcf == null || threshold == null) {
            return new ThresholdingResult(0, 0, 0);
        }

        // clear feature not scored yet
        Queue<double[]> samples = model.getSamples();
        while (samples != null && samples.peek() != null) {
            double[] recordedFeature = samples.poll();
            double rcfScore = rcf.getAnomalyScore(recordedFeature);
            rcf.update(recordedFeature);
            threshold.update(rcfScore);
        }

        double rcfScore = rcf.getAnomalyScore(feature);
        rcf.update(feature);
        threshold.update(rcfScore);

        double anomalyGrade = threshold.grade(rcfScore);
        double anomalyConfidence = computeRcfConfidence(rcf) * threshold.confidence();
        ThresholdingResult result = new ThresholdingResult(anomalyGrade, anomalyConfidence, rcfScore);

        modelState.setLastUsedTime(clock.instant());
        return result;
    }

    /**
     * Instantiate an entity state out of checkpoint. Train models if there are
     * enough samples.
     * @param checkpoint Checkpoint loaded from index
     * @param entity objects to access Entity attributes
     * @param modelId Model Id
     * @param detectorId Detector Id
     *
     * @return updated model state
     *
     */
    public ModelState<EntityModel> processEntityCheckpoint(
        Optional<Entry<EntityModel, Instant>> checkpoint,
        Entity entity,
        String modelId,
        String detectorId
    ) {
        // entity state to instantiate
        ModelState<EntityModel> modelState = new ModelState<>(
            new EntityModel(entity, new ArrayDeque<>(), null, null),
            modelId,
            detectorId,
            ModelType.ENTITY.getName(),
            clock,
            0
        );

        if (checkpoint.isPresent()) {
            Entry<EntityModel, Instant> modelToTime = checkpoint.get();
            EntityModel restoredModel = modelToTime.getKey();
            combineSamples(modelState.getModel(), restoredModel);
            modelState.setModel(restoredModel);
            modelState.setLastCheckpointTime(modelToTime.getValue());
        }
        EntityModel model = modelState.getModel();
        if (model == null) {
            model = new EntityModel(null, new ArrayDeque<>(), null, null);
            modelState.setModel(model);
        }

        if ((model.getRcf() == null || model.getThreshold() == null)
            && model.getSamples() != null
            && model.getSamples().size() >= rcfNumMinSamples) {
            entityColdStarter.trainModelFromExistingSamples(modelState);
        }
        return modelState;
    }

    private void combineSamples(EntityModel fromModel, EntityModel toModel) {
        Queue<double[]> samples = fromModel.getSamples();
        while (samples.peek() != null) {
            toModel.addSample(samples.poll());
        }
    }

    private ThresholdingResult toResult(AnomalyDescriptor anomalyDescriptor) {
        return new ThresholdingResult(anomalyDescriptor.getAnomalyGrade(), /*TODO: pending ercf*/1.0, anomalyDescriptor.getRcfScore());
    }
}
