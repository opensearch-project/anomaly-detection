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

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utilities to map between single-stream models and ids.  We will have circular
 * dependency between ModelManager and CheckpointDao if we put these functions inside
 * ModelManager.
 *
 */
public class SingleStreamModelIdMapper {
    protected static final String CONFIG_ID_PATTERN = "(.*)_model_.+";
    protected static final String RCF_MODEL_ID_PATTERN = "%s_model_rcf_%d";
    protected static final String THRESHOLD_MODEL_ID_PATTERN = "%s_model_threshold";
    protected static final String CASTER_MODEL_ID_PATTERN = "%s_model_caster";

    /**
     * Returns the model ID for the RCF model partition.
     *
     * @param detectorId ID of the detector for which the RCF model is trained
     * @param partitionNumber number of the partition
     * @return ID for the RCF model partition
     */
    public static String getRcfModelId(String detectorId, int partitionNumber) {
        return String.format(Locale.ROOT, RCF_MODEL_ID_PATTERN, detectorId, partitionNumber);
    }

    /**
     * Returns the model ID for the thresholding model.
     *
     * @param detectorId ID of the detector for which the thresholding model is trained
     * @return ID for the thresholding model
     */
    public static String getThresholdModelId(String detectorId) {
        return String.format(Locale.ROOT, THRESHOLD_MODEL_ID_PATTERN, detectorId);
    }

    /**
     * Returns the model ID for the rcf caster model.
     *
     * @param forecasterId ID of the forecaster for which the model is trained
     * @return ID for the forecaster model
     */
    public static String getCasterModelId(String forecasterId) {
        return String.format(Locale.ROOT, CASTER_MODEL_ID_PATTERN, forecasterId);
    }

    /**
     * Gets the config id from the model id.
     *
     * @param modelId id of a model
     * @return id of the detector the model is for
     * @throws IllegalArgumentException if model id is invalid
     */
    public static String getConfigIdForModelId(String modelId) {
        Matcher matcher = Pattern.compile(CONFIG_ID_PATTERN).matcher(modelId);
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            throw new IllegalArgumentException("Invalid model id " + modelId);
        }
    }

    /**
     * Returns the model ID for the thresholding model according to the input
     * rcf model id.
     * @param rcfModelId RCF model id
     * @return thresholding model Id
     */
    public static String getThresholdModelIdFromRCFModelId(String rcfModelId) {
        String detectorId = getConfigIdForModelId(rcfModelId);
        return getThresholdModelId(detectorId);
    }
}
