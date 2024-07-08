/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.ad.caching.ADCacheProvider;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.transport.ProfileNodeRequest;
import org.opensearch.timeseries.transport.ProfileNodeResponse;
import org.opensearch.timeseries.transport.ProfileRequest;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.transport.TransportService;

/**
 * This utility class serves as a delegate for testing ProfileTransportAction functionalities.
 * It facilitates the invocation of protected methods within the org.opensearch.ad.transport.ADProfileTransportAction
 * and org.opensearch.timeseries.transport.BaseProfileTransportAction classes, which are otherwise inaccessible
 * due to Java's access control restrictions. This is achieved by extending the target classes or using reflection
 * where inheritance is not possible, enabling the testing framework to perform comprehensive tests on protected
 * class members across different packages.
 */
public class DelegateADProfileTransportAction extends ADProfileTransportAction {

    public DelegateADProfileTransportAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        ADTaskCacheManager taskCacheManager,
        ADCacheProvider cacheProvider,
        Settings settings
    ) {
        super(threadPool, clusterService, transportService, actionFilters, taskCacheManager, cacheProvider, settings);
    }

    @Override
    public ProfileResponse newResponse(ProfileRequest request, List<ProfileNodeResponse> responses, List<FailedNodeException> failures) {
        return super.newResponse(request, responses, failures);
    }

    @Override
    public ProfileNodeRequest newNodeRequest(ProfileRequest request) {
        return super.newNodeRequest(request);
    }
}
