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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.ad.DetectorModelSize;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.DateUtils;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;
import com.amazon.randomcutforest.parkservices.AnomalyDescriptor;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * A facade managing ML operations and models.
 */
public class ModelManager implements DetectorModelSize {
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

    private static final Logger logger = LogManager.getLogger(ModelManager.class);

    // states
    private TRCFMemoryAwareConcurrentHashmap<String> forests;
    private Map<String, ModelState<ThresholdingModel>> thresholds;

    // configuration
    private final int rcfNumTrees;
    private final int rcfNumSamplesInTree;
    private final double rcfTimeDecay;
    private final int rcfNumMinSamples;
    private final double thresholdMinPvalue;
    private final int minPreviewSize;
    private final Duration modelTtl;
    private Duration checkpointInterval;

    // dependencies
    private final CheckpointDao checkpointDao;
    private final Clock clock;
    public FeatureManager featureManager;

    private EntityColdStarter entityColdStarter;
    private MemoryTracker memoryTracker;

    private final double initialAcceptFraction;

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
     * @param minPreviewSize minimum number of data points for preview
     * @param modelTtl time to live for hosted models
     * @param checkpointIntervalSetting setting of interval between checkpoints
     * @param entityColdStarter HCAD cold start utility
     * @param featureManager Used to create features for models
     * @param memoryTracker AD memory usage tracker
     * @param settings Node settings
     * @param clusterService Cluster service accessor
     */
    public ModelManager(
        CheckpointDao checkpointDao,
        Clock clock,
        int rcfNumTrees,
        int rcfNumSamplesInTree,
        double rcfTimeDecay,
        int rcfNumMinSamples,
        double thresholdMinPvalue,
        int minPreviewSize,
        Duration modelTtl,
        Setting<TimeValue> checkpointIntervalSetting,
        EntityColdStarter entityColdStarter,
        FeatureManager featureManager,
        MemoryTracker memoryTracker,
        Settings settings,
        ClusterService clusterService
    ) {
        this.checkpointDao = checkpointDao;
        this.clock = clock;
        this.rcfNumTrees = rcfNumTrees;
        this.rcfNumSamplesInTree = rcfNumSamplesInTree;
        this.rcfTimeDecay = rcfTimeDecay;
        this.rcfNumMinSamples = rcfNumMinSamples;
        this.thresholdMinPvalue = thresholdMinPvalue;
        this.minPreviewSize = minPreviewSize;
        this.modelTtl = modelTtl;
        this.checkpointInterval = DateUtils.toDuration(checkpointIntervalSetting.get(settings));
        if (clusterService != null) {
            clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(checkpointIntervalSetting, it -> this.checkpointInterval = DateUtils.toDuration(it));
        }

        this.forests = new TRCFMemoryAwareConcurrentHashmap<>(memoryTracker);
        this.thresholds = new ConcurrentHashMap<>();

        this.entityColdStarter = entityColdStarter;
        this.featureManager = featureManager;
        this.memoryTracker = memoryTracker;
        this.initialAcceptFraction = rcfNumMinSamples * 1.0d / rcfNumSamplesInTree;
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

    private void getTRcfResult(
        ModelState<ThresholdedRandomCutForest> modelState,
        double[] point,
        ActionListener<ThresholdingResult> listener
    ) {
        modelState.setLastUsedTime(clock.instant());

        ThresholdedRandomCutForest trcf = modelState.getModel();
        try {
            AnomalyDescriptor result = trcf.process(point, 0);
            double[] attribution = normalizeAttribution(trcf.getForest(), result.getRelevantAttribution());
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
                        result.getNumberOfTrees()
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

    private Optional<ModelState<ThresholdedRandomCutForest>> restoreModelState(
        Optional<ThresholdedRandomCutForest> rcfModel,
        String modelId,
        String detectorId
    ) {
        if (!rcfModel.isPresent()) {
            return Optional.empty();
        }
        return rcfModel
            .filter(rcf -> memoryTracker.isHostingAllowed(detectorId, rcf))
            .map(rcf -> ModelState.createSingleEntityModelState(rcf, modelId, detectorId, ModelType.RCF.getName(), clock));
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
        if (model.isPresent()) {
            forests.put(modelId, model.get());
            if (model.get().getModel() != null && model.get().getModel().getForest() != null)
                listener.onResponse(model.get().getModel().getForest().getTotalUpdates());
        } else {
            listener.onFailure(new ResourceNotFoundException(detectorId, ADCommonMessages.NO_CHECKPOINT_ERR_MSG + modelId));
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
        if (modelState.isPresent()) {
            T model = modelState.get().getModel();
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

    private void clearModels(String detectorId, Map<String, ?> models, ActionListener<Void> listener) {
        Iterator<String> id = models.keySet().iterator();
        clearModelForIterator(detectorId, models, id, listener);
    }

    private void clearModelForIterator(String detectorId, Map<String, ?> models, Iterator<String> idIter, ActionListener<Void> listener) {
        if (idIter.hasNext()) {
            String modelId = idIter.next();
            if (SingleStreamModelIdMapper.getDetectorIdForModelId(modelId).equals(detectorId)) {
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
            try {
                trainModelForStep(anomalyDetector, dataPoints, rcfNumFeatures, 0, listener);
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }
    }

    private void trainModelForStep(
        AnomalyDetector detector,
        double[][] dataPoints,
        int rcfNumFeatures,
        int step,
        ActionListener<Void> listener
    ) {
        ThresholdedRandomCutForest trcf = ThresholdedRandomCutForest
            .builder()
            .dimensions(rcfNumFeatures)
            .sampleSize(rcfNumSamplesInTree)
            .numberOfTrees(rcfNumTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(rcfNumMinSamples)
            .initialAcceptFraction(initialAcceptFraction)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(AnomalyDetectorSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
            .shingleSize(detector.getShingleSize())
            .anomalyRate(1 - thresholdMinPvalue)
            .build();
        Arrays.stream(dataPoints).forEach(s -> trcf.process(s, 0));

        String modelId = SingleStreamModelIdMapper.getRcfModelId(detector.getDetectorId(), step);
        checkpointDao.putTRCFCheckpoint(modelId, trcf, ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure));
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
        ThresholdedRandomCutForest trcf = ThresholdedRandomCutForest
            .builder()
            .randomSeed(0L)
            .dimensions(rcfNumFeatures)
            .sampleSize(rcfNumSamplesInTree)
            .numberOfTrees(rcfNumTrees)
            .timeDecay(rcfTimeDecay)
            .outputAfter(rcfNumMinSamples)
            .initialAcceptFraction(initialAcceptFraction)
            .parallelExecutionEnabled(false)
            .compact(true)
            .precision(Precision.FLOAT_32)
            .boundingBoxCacheFraction(AnomalyDetectorSettings.BATCH_BOUNDING_BOX_CACHE_RATIO)
            .shingleSize(shingleSize)
            .anomalyRate(1 - this.thresholdMinPvalue)
            .build();
        return Arrays.stream(dataPoints).map(point -> {
            AnomalyDescriptor descriptor = trcf.process(point, 0);
            return new ThresholdingResult(
                descriptor.getAnomalyGrade(),
                descriptor.getDataConfidence(),
                descriptor.getRCFScore(),
                descriptor.getTotalUpdates(),
                descriptor.getRelativeIndex(),
                normalizeAttribution(trcf.getForest(), descriptor.getRelevantAttribution()),
                descriptor.getPastValues(),
                descriptor.getExpectedValuesList(),
                descriptor.getLikelihoodOfValues(),
                descriptor.getThreshold(),
                rcfNumTrees
            );
        }).collect(Collectors.toList());
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
            .filter(entry -> SingleStreamModelIdMapper.getDetectorIdForModelId(entry.getKey()).equals(detectorId))
            .forEach(entry -> { res.put(entry.getKey(), memoryTracker.estimateTRCFModelSize(entry.getValue().getModel())); });
        thresholds
            .entrySet()
            .stream()
            .filter(entry -> SingleStreamModelIdMapper.getDetectorIdForModelId(entry.getKey()).equals(detectorId))
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
        ModelState<ThresholdedRandomCutForest> model = forests.get(modelId);
        if (model != null) {
            if (model.getModel() != null && model.getModel().getForest() != null) {
                listener.onResponse(model.getModel().getForest().getTotalUpdates());
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

    /**
     * Compute anomaly result for the given data point
     * @param datapoint Data point
     * @param modelState the state associated with the entity
     * @param modelId the model Id
     * @param entity entity accessor
     * @param shingleSize Shingle size
     *
     * @return anomaly result, confidence, and the corresponding RCF score.
     */
    public ThresholdingResult getAnomalyResultForEntity(
        double[] datapoint,
        ModelState<EntityModel> modelState,
        String modelId,
        Entity entity,
        int shingleSize
    ) {
        ThresholdingResult result = new ThresholdingResult(0, 0, 0);
        if (modelState != null) {
            EntityModel entityModel = modelState.getModel();

            if (entityModel == null) {
                entityModel = new EntityModel(entity, new ArrayDeque<>(), null);
                modelState.setModel(entityModel);
            }

            if (!entityModel.getTrcf().isPresent()) {
                entityColdStarter.trainModelFromExistingSamples(modelState, shingleSize);
            }

            if (entityModel.getTrcf().isPresent()) {
                result = score(datapoint, modelId, modelState);
            } else {
                entityModel.addSample(datapoint);
            }
        }
        return result;
    }

    public ThresholdingResult score(double[] feature, String modelId, ModelState<EntityModel> modelState) {
        ThresholdingResult result = new ThresholdingResult(0, 0, 0);
        EntityModel model = modelState.getModel();
        try {
            if (model != null && model.getTrcf().isPresent()) {
                ThresholdedRandomCutForest trcf = model.getTrcf().get();
                Optional.ofNullable(model.getSamples()).ifPresent(q -> {
                    q.stream().forEach(s -> trcf.process(s, 0));
                    q.clear();
                });
                result = toResult(trcf.getForest(), trcf.process(feature, 0));
            }
        } catch (Exception e) {
            logger
                .error(
                    new ParameterizedMessage(
                        "Fail to score for [{}]: model Id [{}], feature [{}]",
                        modelState.getModel().getEntity(),
                        modelId,
                        Arrays.toString(feature)
                    ),
                    e
                );
            throw e;
        } finally {
            modelState.setLastUsedTime(clock.instant());
        }
        return result;
    }

    /**
     * Instantiate an entity state out of checkpoint. Train models if there are
     * enough samples.
     * @param checkpoint Checkpoint loaded from index
     * @param entity objects to access Entity attributes
     * @param modelId Model Id
     * @param detectorId Detector Id
     * @param shingleSize Shingle size
     *
     * @return updated model state
     *
     */
    public ModelState<EntityModel> processEntityCheckpoint(
        Optional<Entry<EntityModel, Instant>> checkpoint,
        Entity entity,
        String modelId,
        String detectorId,
        int shingleSize
    ) {
        // entity state to instantiate
        ModelState<EntityModel> modelState = new ModelState<>(
            new EntityModel(entity, new ArrayDeque<>(), null),
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
            model = new EntityModel(null, new ArrayDeque<>(), null);
            modelState.setModel(model);
        }

        if (!model.getTrcf().isPresent() && model.getSamples() != null && model.getSamples().size() >= rcfNumMinSamples) {
            entityColdStarter.trainModelFromExistingSamples(modelState, shingleSize);
        }
        return modelState;
    }

    private void combineSamples(EntityModel fromModel, EntityModel toModel) {
        Queue<double[]> samples = fromModel.getSamples();
        while (samples.peek() != null) {
            toModel.addSample(samples.poll());
        }
    }

    private ThresholdingResult toResult(RandomCutForest rcf, AnomalyDescriptor anomalyDescriptor) {
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
            rcfNumTrees
        );
    }
}
