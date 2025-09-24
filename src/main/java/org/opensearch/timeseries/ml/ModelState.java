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

package org.opensearch.timeseries.ml;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Entity;

public class ModelState<T> implements org.opensearch.timeseries.ExpiringState {
    public static String MODEL_TYPE_KEY = "model_type";
    public static String LAST_USED_TIME_KEY = "last_used_time";
    public static String LAST_CHECKPOINT_TIME_KEY = "last_checkpoint_time";
    public static String PRIORITY_KEY = "priority";

    protected T model;
    protected String modelId;
    protected String configId;
    protected String modelType;
    // time when the ML model was used last time
    protected Instant lastUsedTime;
    protected Instant lastCheckpointTime;
    protected Clock clock;
    protected float priority;
    protected Deque<Sample> samples;
    protected Optional<Entity> entity;

    /**
     * Constructor.
     *
     * @param model ML model
     * @param modelId Id of model partition
     * @param configId Id of analysis this model partition is used for
     * @param modelType type of model
     * @param clock UTC clock
     * @param priority Priority of the model state.  Used in multi-entity detectors' cache.
     * @param entity Entity info if this is a HC entity state
     * @param samples existing samples that haven't been processed
     */
    public ModelState(
        T model,
        String modelId,
        String configId,
        String modelType,
        Clock clock,
        float priority,
        Optional<Entity> entity,
        Deque<Sample> samples
    ) {
        this.model = model;
        this.modelId = modelId;
        this.configId = configId;
        this.modelType = modelType;
        this.lastUsedTime = clock.instant();
        // this is inaccurate until we find the last checkpoint time from disk
        this.lastCheckpointTime = Instant.MIN;
        this.clock = clock;
        this.priority = priority;
        this.entity = entity;
        this.samples = samples;
    }

    /**
     * Constructor. Used in single-stream analysis.
     *
     * @param model ML model
     * @param modelId Id of model partition
     * @param configId Id of analysis this model partition is used for
     * @param modelType type of model
     * @param clock UTC clock
     */
    public ModelState(T model, String modelId, String configId, String modelType, Clock clock) {
        this(model, modelId, configId, modelType, clock, 0, Optional.empty(), new ArrayDeque<>());
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
        this.lastUsedTime = clock.instant();
    }

    @Override
    public boolean expired(Duration stateTtl) {
        return expired(lastUsedTime, stateTtl, clock.instant());
    }

    /**
     * Gets the Config ID of the model
     *
     * @return the config id associated with the model
     */
    public String getConfigId() {
        return configId;
    }

    /**
     * In old checkpoint mapping, we don't have entity. It's fine we are missing
     * entity as it is mostly used for debugging.
     * @return entity
     */
    public Optional<Entity> getEntity() {
        return entity;
    }

    public Deque<Sample> getSamples() {
        return this.samples;
    }

    public void addSample(Sample sample) {
        if (this.samples == null) {
            this.samples = new ArrayDeque<>();
        }
        if (sample != null && sample.getValueList() != null && sample.getValueList().length != 0) {
            this.samples.add(sample);
        }
        this.lastUsedTime = clock.instant();
    }

    /**
     * Sets a model.
     *
     * @param model model instance
     */
    public void setModel(T model) {
        this.model = model;
        this.lastUsedTime = clock.instant();
    }

    /**
     *
     * @return optional model.
     */
    public Optional<T> getModel() {
        this.lastUsedTime = clock.instant();
        return Optional.ofNullable(this.model);
    }

    public void clearSamples() {
        if (samples != null) {
            samples.clear();
        }
        this.lastUsedTime = clock.instant();
    }

    public void clear() {
        clearSamples();
        model = null;
    }

    /**
     * Gets the Model State as a map
     *
     * @return Map of ModelStates
     */
    public Map<String, Object> getModelStateAsMap() {
        return new HashMap<String, Object>() {
            {
                put(CommonName.MODEL_ID_FIELD, modelId);
                put(CommonName.CONFIG_ID_KEY, configId);
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
                if (entity.isPresent()) {
                    put(CommonName.ENTITY_KEY, entity.get().toStat());
                }
            }
        };
    }
}
