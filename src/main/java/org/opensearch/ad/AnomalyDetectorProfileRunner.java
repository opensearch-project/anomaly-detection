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

package org.opensearch.ad;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.settings.ADNumericSetting;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ProfileRunner;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Since version 2.15, we have merged the single-stream and HC detector workflows. Consequently, separate logic for profiling is no longer necessary.
 *
 * During a Blue/Green (B/G) deployment, if an old node communicates with a new node regarding an old model, we will not execute RCFPollingAction to
 * determine model updates. However, we have fallback logic that checks for anomaly results. If any results are found, the initialization progress is
 * set to 100%.
 *
 */
public class AnomalyDetectorProfileRunner extends
    ProfileRunner<ADTaskCacheManager, ADTaskType, ADTask, ADIndex, ADIndexManagement, ADTaskProfile, ADTaskManager, DetectorProfile, ADProfileAction, ADTaskProfileRunner> {

    private final Logger logger = LogManager.getLogger(AnomalyDetectorProfileRunner.class);

    public AnomalyDetectorProfileRunner(
        Client client,
        SecurityClientUtil clientUtil,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        long requiredSamples,
        TransportService transportService,
        ADTaskManager adTaskManager,
        ADTaskProfileRunner taskProfileRunner
    ) {
        super(
            client,
            clientUtil,
            xContentRegistry,
            nodeFilter,
            requiredSamples,
            transportService,
            adTaskManager,
            AnalysisType.AD,
            ADTaskType.REALTIME_TASK_TYPES,
            ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES,
            ADNumericSetting.maxCategoricalFields(),
            ProfileName.AD_TASK,
            ADProfileAction.INSTANCE,
            AnomalyDetector::parse,
            taskProfileRunner,
            ADCommonName.CONFIG_INDEX
        );
    }

    @Override
    protected DetectorProfile.Builder createProfileBuilder() {
        return new DetectorProfile.Builder();
    }
}
