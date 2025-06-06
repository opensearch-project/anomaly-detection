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

package org.opensearch.ad.transport;

import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.ADEntityProfileRunner;
import org.opensearch.ad.ADTaskProfileRunner;
import org.opensearch.ad.AnomalyDetectorProfileRunner;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.model.EntityProfile;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.transport.BaseGetConfigTransportAction;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public class GetAnomalyDetectorTransportAction extends
    BaseGetConfigTransportAction<GetAnomalyDetectorResponse, ADTaskCacheManager, ADTaskType, ADTask, ADIndex, ADIndexManagement, ADTaskManager, AnomalyDetector, ADEntityProfileAction, ADEntityProfileRunner, ADTaskProfile, DetectorProfile, ADProfileAction, ADTaskProfileRunner, AnomalyDetectorProfileRunner> {

    public static final Logger LOG = LogManager.getLogger(GetAnomalyDetectorTransportAction.class);

    @Inject
    public GetAnomalyDetectorTransportAction(
        TransportService transportService,
        DiscoveryNodeFilterer nodeFilter,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager,
        ADTaskProfileRunner adTaskProfileRunner
    ) {
        super(
            transportService,
            nodeFilter,
            actionFilters,
            clusterService,
            client,
            clientUtil,
            settings,
            xContentRegistry,
            adTaskManager,
            GetAnomalyDetectorAction.NAME,
            AnomalyDetector.class,
            AnomalyDetector.PARSE_FIELD_NAME,
            ADTaskType.ALL_DETECTOR_TASK_TYPES,
            ADTaskType.REALTIME_HC_DETECTOR.name(),
            ADTaskType.REALTIME_SINGLE_ENTITY.name(),
            ADTaskType.HISTORICAL_HC_DETECTOR.name(),
            ADTaskType.HISTORICAL_SINGLE_ENTITY.name(),
            AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES,
            adTaskProfileRunner
        );
    }

    @Override
    protected Optional<ADTask> fillInHistoricalTaskforBwc(Map<String, ADTask> tasks) {
        if (tasks.containsKey(ADTaskType.HISTORICAL.name())) {
            return Optional.ofNullable(tasks.get(ADTaskType.HISTORICAL.name()));
        }
        return Optional.empty();
    }

    @Override
    protected GetAnomalyDetectorResponse createResponse(
        long version,
        String id,
        long primaryTerm,
        long seqNo,
        AnomalyDetector config,
        Job job,
        boolean returnJob,
        Optional<ADTask> realtimeTask,
        Optional<ADTask> historicalTask,
        boolean returnTask,
        RestStatus restStatus,
        DetectorProfile detectorProfile,
        EntityProfile entityProfile,
        boolean profileResponse
    ) {
        return new GetAnomalyDetectorResponse(
            version,
            id,
            primaryTerm,
            seqNo,
            config,
            job,
            returnJob,
            realtimeTask.orElse(null),
            historicalTask.orElse(null),
            returnTask,
            RestStatus.OK,
            detectorProfile,
            entityProfile,
            profileResponse
        );
    }

    @Override
    protected ADEntityProfileRunner createEntityProfileRunner(
        Client client,
        SecurityClientUtil clientUtil,
        NamedXContentRegistry xContentRegistry,
        long requiredSamples
    ) {
        return new ADEntityProfileRunner(client, clientUtil, xContentRegistry, TimeSeriesSettings.NUM_MIN_SAMPLES);
    }

    @Override
    protected AnomalyDetectorProfileRunner createProfileRunner(
        Client client,
        SecurityClientUtil clientUtil,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        long requiredSamples,
        TransportService transportService,
        ADTaskManager taskManager,
        ADTaskProfileRunner taskProfileRunner
    ) {
        return new AnomalyDetectorProfileRunner(
            client,
            clientUtil,
            xContentRegistry,
            nodeFilter,
            TimeSeriesSettings.NUM_MIN_SAMPLES,
            transportService,
            taskManager,
            taskProfileRunner
        );
    }

    // no need to adjust for AD
    @Override
    protected void adjustState(Optional<ADTask> taskOptional, Job job) {
        // TODO Auto-generated method stub

    }

}
