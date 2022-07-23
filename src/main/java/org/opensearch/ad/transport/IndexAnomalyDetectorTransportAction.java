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

import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_CREATE_DETECTOR;
import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_UPDATE_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES;
import static org.opensearch.ad.util.ParseUtils.checkFilterByBackendRoles;
import static org.opensearch.ad.util.ParseUtils.getDetector;
import static org.opensearch.ad.util.ParseUtils.getNullUser;
import static org.opensearch.ad.util.RestHandlerUtils.wrapRestActionListener;

import java.util.List;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.auth.UserIdentity;
import org.opensearch.ad.feature.SearchFeatureDao;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorActionHandler;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class IndexAnomalyDetectorTransportAction extends HandledTransportAction<IndexAnomalyDetectorRequest, IndexAnomalyDetectorResponse> {
    private static final Logger LOG = LogManager.getLogger(IndexAnomalyDetectorTransportAction.class);
    private final Client client;
    private final TransportService transportService;
    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final ADTaskManager adTaskManager;
    private volatile Boolean filterByEnabled;
    private final SearchFeatureDao searchFeatureDao;

    @Inject
    public IndexAnomalyDetectorTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        AnomalyDetectionIndices anomalyDetectionIndices,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager,
        SearchFeatureDao searchFeatureDao
    ) {
        super(IndexAnomalyDetectorAction.NAME, transportService, actionFilters, IndexAnomalyDetectorRequest::new);
        this.client = client;
        this.transportService = transportService;
        this.clusterService = clusterService;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.xContentRegistry = xContentRegistry;
        this.adTaskManager = adTaskManager;
        this.searchFeatureDao = searchFeatureDao;
        filterByEnabled = AnomalyDetectorSettings.FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
    }

    @Override
    protected void doExecute(Task task, IndexAnomalyDetectorRequest request, ActionListener<IndexAnomalyDetectorResponse> actionListener) {
        // Temporary null user for AD extension without security. Will always execute detector.
        UserIdentity user = getNullUser();
        String detectorId = request.getDetectorID();
        RestRequest.Method method = request.getMethod();
        String errorMessage = method == RestRequest.Method.PUT ? FAIL_TO_UPDATE_DETECTOR : FAIL_TO_CREATE_DETECTOR;
        ActionListener<IndexAnomalyDetectorResponse> listener = wrapRestActionListener(actionListener, errorMessage);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            resolveUserAndExecute(user, detectorId, method, listener, (detector) -> adExecute(request, user, detector, context, listener));
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    private void resolveUserAndExecute(
        UserIdentity requestedUser,
        String detectorId,
        RestRequest.Method method,
        ActionListener<IndexAnomalyDetectorResponse> listener,
        Consumer<AnomalyDetector> function
    ) {
        try {
            // Check if user has backend roles
            // When filter by is enabled, block users creating/updating detectors who do not have backend roles.
            if (filterByEnabled && !checkFilterByBackendRoles(requestedUser, listener)) {
                return;
            }
            if (method == RestRequest.Method.PUT) {
                // requestedUser == null means security is disabled or user is superadmin. In this case we don't need to
                // check if request user have access to the detector or not. But we still need to get current detector for
                // this case, so we can keep current detector's user data.
                boolean filterByBackendRole = requestedUser == null ? false : filterByEnabled;
                // Update detector request, check if user has permissions to update the detector
                // Get detector and verify backend roles
                getDetector(requestedUser, detectorId, listener, function, client, clusterService, xContentRegistry, filterByBackendRole);
            } else {
                // Create Detector. No need to get current detector.
                function.accept(null);
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected void adExecute(
        IndexAnomalyDetectorRequest request,
        UserIdentity user,
        AnomalyDetector currentDetector,
        ThreadContext.StoredContext storedContext,
        ActionListener<IndexAnomalyDetectorResponse> listener
    ) {
        anomalyDetectionIndices.update();
        String detectorId = request.getDetectorID();
        long seqNo = request.getSeqNo();
        long primaryTerm = request.getPrimaryTerm();
        WriteRequest.RefreshPolicy refreshPolicy = request.getRefreshPolicy();
        AnomalyDetector detector = request.getDetector();
        RestRequest.Method method = request.getMethod();
        TimeValue requestTimeout = request.getRequestTimeout();
        Integer maxSingleEntityAnomalyDetectors = request.getMaxSingleEntityAnomalyDetectors();
        Integer maxMultiEntityAnomalyDetectors = request.getMaxMultiEntityAnomalyDetectors();
        Integer maxAnomalyFeatures = request.getMaxAnomalyFeatures();

        storedContext.restore();
        checkIndicesAndExecute(detector.getIndices(), () -> {
            // Don't replace detector's user when update detector
            // Github issue: https://github.com/opensearch-project/anomaly-detection/issues/124
            UserIdentity detectorUser = currentDetector == null ? user : currentDetector.getUser();
            IndexAnomalyDetectorActionHandler indexAnomalyDetectorActionHandler = new IndexAnomalyDetectorActionHandler(
                clusterService,
                client,
                transportService,
                listener,
                anomalyDetectionIndices,
                detectorId,
                seqNo,
                primaryTerm,
                refreshPolicy,
                detector,
                requestTimeout,
                maxSingleEntityAnomalyDetectors,
                maxMultiEntityAnomalyDetectors,
                maxAnomalyFeatures,
                method,
                xContentRegistry,
                detectorUser,
                adTaskManager,
                searchFeatureDao
            );
            indexAnomalyDetectorActionHandler.start();
        }, listener);
    }

    private void checkIndicesAndExecute(
        List<String> indices,
        AnomalyDetectorFunction function,
        ActionListener<IndexAnomalyDetectorResponse> listener
    ) {
        SearchRequest searchRequest = new SearchRequest()
            .indices(indices.toArray(new String[0]))
            .source(new SearchSourceBuilder().size(1).query(QueryBuilders.matchAllQuery()));
        client.search(searchRequest, ActionListener.wrap(r -> { function.execute(); }, e -> {
            // Due to below issue with security plugin, we get security_exception when invalid index name is mentioned.
            // https://github.com/opendistro-for-elasticsearch/security/issues/718
            LOG.error(e);
            listener.onFailure(e);
        }));
    }
}
