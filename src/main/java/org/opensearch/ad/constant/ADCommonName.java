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

package org.opensearch.ad.constant;

public class ADCommonName {
    // ======================================
    // Index name
    // ======================================
    // index name for anomaly checkpoint of each model. One model one document.
    public static final String CHECKPOINT_INDEX_NAME = ".opendistro-anomaly-checkpoints";
    // index name for anomaly detection state. Will store AD task in this index as well.
    public static final String DETECTION_STATE_INDEX = ".opendistro-anomaly-detection-state";
    // config index. We are reusing ad detector index.
    public static final String CONFIG_INDEX = ".opendistro-anomaly-detectors";

    // The alias of the index in which to write AD result history
    public static final String ANOMALY_RESULT_INDEX_ALIAS = ".opendistro-anomaly-results";

    // The insights result index alias
    public static final String INSIGHTS_RESULT_INDEX_ALIAS = "opensearch-ad-plugin-insights";

    // ======================================
    // Anomaly Detector name for X-Opaque-Id header
    // ======================================
    public static final String ANOMALY_DETECTOR = "[Anomaly Detector]";
    public static final String AD_PLUGIN_NAME = "opensearch-anomaly-detection";
    public static final String AD_PLUGIN_NAME_FOR_TEST = "org.opensearch.ad.AnomalyDetectorPlugin";
    public static final String AD_PLUGIN_VERSION_FOR_TEST = "NA";

    // ======================================
    // Resource name used in resource-access-control
    // ======================================
    public static final String AD_RESOURCE_TYPE = "anomaly-detector";

    // ======================================
    // Historical detectors
    // ======================================
    public static final String AD_TASK = "ad_task";
    public static final String HISTORICAL_ANALYSIS = "historical_analysis";
    public static final String AD_TASK_REMOTE = "ad_task_remote";
    public static final String CANCEL_TASK = "cancel_task";

    // ======================================
    // Used in stats API
    // ======================================
    public static final String DETECTOR_ID_KEY = "detector_id";

    // ======================================
    // Used in toXContent
    // ======================================
    public static final String RCF_SCORE_JSON_KEY = "rCFScore";
    public static final String ID_JSON_KEY = "adID";
    public static final String FEATURE_JSON_KEY = "features";
    public static final String CONFIDENCE_JSON_KEY = "confidence";
    public static final String ANOMALY_GRADE_JSON_KEY = "anomalyGrade";
    public static final String QUEUE_JSON_KEY = "queue";

    // ======================================
    // Validation
    // ======================================
    // detector validation aspect
    public static final String DETECTOR_ASPECT = "detector";
    // ======================================
    // Used for custom AD result index
    // ======================================
    public static final String DUMMY_AD_RESULT_ID = "dummy_ad_result_id";
    public static final String DUMMY_DETECTOR_ID = "dummy_detector_id";
    public static final String CUSTOM_RESULT_INDEX_PREFIX = "opensearch-ad-plugin-result-";

    // ======================================
    // Insights job
    // ======================================
    // The Insights job name
    public static final String INSIGHTS_JOB_NAME = "insights_job";
}
