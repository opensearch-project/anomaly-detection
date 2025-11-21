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

import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_START_DETECTOR;
import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_STOP_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_REQUEST_TIMEOUT;

import java.time.Clock;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.rest.handler.ADIndexJobActionHandler;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.timeseries.transport.BaseJobTransportAction;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class AnomalyDetectorJobTransportAction extends
    BaseJobTransportAction<ADIndex, ADIndexManagement, ADTaskCacheManager, ADTaskType, ADTask, ADTaskManager, AnomalyResult, ADProfileAction, ExecuteADResultResponseRecorder, ADIndexJobActionHandler, AnomalyDetector> {
    @Inject
    public AnomalyDetectorJobTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        ADIndexJobActionHandler adIndexJobActionHandler
    ) {
        super(
            transportService,
            actionFilters,
            client,
            clusterService,
            settings,
            xContentRegistry,
            AD_FILTER_BY_BACKEND_ROLES,
            AnomalyDetectorJobAction.NAME,
            AD_REQUEST_TIMEOUT,
            FAIL_TO_START_DETECTOR,
            FAIL_TO_STOP_DETECTOR,
            AnomalyDetector.class,
            adIndexJobActionHandler,
            Clock.systemUTC(), // inject cannot find clock due to OS limitation
            AnomalyDetector.class
        );
    }
}
