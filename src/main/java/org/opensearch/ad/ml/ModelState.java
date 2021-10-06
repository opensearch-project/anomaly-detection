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
import java.util.HashMap;
import java.util.Map;

import org.opensearch.ad.ExpiringState;
import org.opensearch.ad.constant.CommonName;

/**
 * A ML model and states such as usage.
 */
public class ModelState<T> implements ExpiringState {

    public static String MODEL_TYPE_KEY = "model_type";
    public static String LAST_USED_TIME_KEY = "last_used_time";
    public static String LAST_CHECKPOINT_TIME_KEY = "last_checkpoint_time";
    public static String PRIORITY_KEY = "priority";
    private T model;
    private String modelId;
    private String detectorId;
    private String modelType;
    // time when the ML model was used last time
    private Instant lastUsedTime;
    private Instant lastCheckpointTime;
    private Clock clock;
    private float priority;

    /**
     * Constructor.
     *
     * @param model ML model
     * @param modelId Id of model partition
     * @param detectorId Id of detector this model partition is used for
     * @param modelType type of model
     * @param clock UTC clock
     * @param priority Priority of the model state.  Used in multi-entity detectors' cache.
     */
    public ModelState(T model, String modelId, String detectorId, String modelType, Clock clock, float priority) {
        this.model = model;
        this.modelId = modelId;
        this.detectorId = detectorId;
        this.modelType = modelType;
        this.lastUsedTime = clock.instant();
        // this is inaccurate until we find the last checkpoint time from disk
        this.lastCheckpointTime = Instant.MIN;
        this.clock = clock;
        this.priority = priority;
    }

    /**
     * Create state with zero priority. Used in single-entity detector.
     *
     * @param <T> Model object's type
     * @param model The actual model object
     * @param modelId Model Id
     * @param detectorId Detector Id
     * @param modelType Model type like RCF model
     * @param clock UTC clock
     *
     * @return the created model state
     */
    public static <T> ModelState<T> createSingleEntityModelState(
        T model,
        String modelId,
        String detectorId,
        String modelType,
        Clock clock
    ) {
        return new ModelState<>(model, modelId, detectorId, modelType, clock, 0f);
    }

    /**
     * Returns the ML model.
     *
     * @return the ML model.
     */
    public T getModel() {
        return this.model;
    }

    public void setModel(T model) {
        this.model = model;
    }

    /**
     * Gets the model ID
     *
     * @return modelId of model
     */
    public String getModelId() {
        return modelId;
    }

    /**
     * Gets the detectorID of the model
     *
     * @return detectorId associated with the model
     */
    public String getDetectorId() {
        return detectorId;
    }

    /**
     * Gets the type of the model
     *
     * @return modelType of the model
     */
    public String getModelType() {
        return modelType;
    }

    /**
     * Returns the time when the ML model was last used.
     *
     * @return the time when the ML model was last used
     */
    public Instant getLastUsedTime() {
        return this.lastUsedTime;
    }

    /**
     * Sets the time when ML model was last used.
     *
     * @param lastUsedTime time when the ML model was used last time
     */
    public void setLastUsedTime(Instant lastUsedTime) {
        this.lastUsedTime = lastUsedTime;
    }

    /**
     * Returns the time when a checkpoint for the ML model was made last time.
     *
     * @return the time when a checkpoint for the ML model was made last time.
     */
    public Instant getLastCheckpointTime() {
        return this.lastCheckpointTime;
    }

    /**
     * Sets the time when a checkpoint for the ML model was made last time.
     *
     * @param lastCheckpointTime time when a checkpoint for the ML model was made last time.
     */
    public void setLastCheckpointTime(Instant lastCheckpointTime) {
        this.lastCheckpointTime = lastCheckpointTime;
    }

    /**
     * Returns priority of the ModelState
     * @return the priority
     */
    public float getPriority() {
        return priority;
    }

    public void setPriority(float priority) {
        this.priority = priority;
    }

    /**
     * Gets the Model State as a map
     *
     * @return Map of ModelStates
     */
    public Map<String, Object> getModelStateAsMap() {
        return new HashMap<String, Object>() {
            {
                put(CommonName.MODEL_ID_KEY, modelId);
                put(CommonName.DETECTOR_ID_KEY, detectorId);
                put(MODEL_TYPE_KEY, modelType);
                /* A stats API broadcasts requests to all nodes and renders node responses using toXContent.
                 *
                 * For the local node, the stats API's calls toXContent on the node response directly.
                 * For remote node, the coordinating node gets a serialized content from
                 * ADStatsNodeResponse.writeTo, deserializes the content, and renders the result using toXContent.
                 * Since ADStatsNodeResponse.writeTo uses StreamOutput::writeGenericValue, we can only use
                 *  a long instead of the Instant object itself as
                 *  StreamOutput::writeGenericValue only recognizes built-in types.*/
                put(LAST_USED_TIME_KEY, lastUsedTime.toEpochMilli());
                if (lastCheckpointTime != Instant.MIN) {
                    put(LAST_CHECKPOINT_TIME_KEY, lastCheckpointTime.toEpochMilli());
                }
                if (model != null && model instanceof EntityModel) {
                    EntityModel summary = (EntityModel) model;
                    if (summary.getEntity().isPresent()) {
                        put(CommonName.ENTITY_KEY, summary.getEntity().get().toStat());
                    }
                }
            }
        };
    }

    @Override
    public boolean expired(Duration stateTtl) {
        return expired(lastUsedTime, stateTtl, clock.instant());
    }
}
