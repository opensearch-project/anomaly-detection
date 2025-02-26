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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.settings.ADEnabledSetting;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.cluster.HashRing;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.LimitExceededException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.FeatureManager;
import org.opensearch.timeseries.stats.StatNames;
import org.opensearch.timeseries.transport.ResultProcessor;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public class AnomalyResultTransportAction extends HandledTransportAction<ActionRequest, AnomalyResultResponse> {

    private static final Logger LOG = LogManager.getLogger(AnomalyResultTransportAction.class);
    private ADResultProcessor resultProcessor;
    private final Client client;
    private CircuitBreakerService adCircuitBreakerService;
    // Cache HC detector id. This is used to count HC failure stats. We can tell a detector
    // is HC or not by checking if detector id exists in this field or not. Will add
    // detector id to this field when start to run realtime detection and remove detector
    // id once realtime detection done.
    private final Set<String> hcDetectors;
    private final ADStats adStats;
    private final NodeStateManager nodeStateManager;

    @Inject
    public AnomalyResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        Client client,
        SecurityClientUtil clientUtil,
        NodeStateManager nodeStateManager,
        FeatureManager featureManager,
        HashRing hashRing,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        CircuitBreakerService adCircuitBreakerService,
        ADStats adStats,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager realTimeTaskManager
    ) {
        super(AnomalyResultAction.NAME, transportService, actionFilters, AnomalyResultRequest::new);
        this.resultProcessor = new ADResultProcessor(
            AnomalyDetectorSettings.AD_REQUEST_TIMEOUT,
            EntityADResultAction.NAME,
            StatNames.AD_HC_EXECUTE_REQUEST_COUNT,
            settings,
            clusterService,
            threadPool,
            hashRing,
            nodeStateManager,
            transportService,
            adStats,
            realTimeTaskManager,
            xContentRegistry,
            client,
            clientUtil,
            indexNameExpressionResolver,
            AnomalyResultResponse.class,
            featureManager
        );
        this.client = client;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.hcDetectors = new HashSet<>();
        this.adStats = adStats;
        this.nodeStateManager = nodeStateManager;
    }

    /**
     * All the exceptions thrown by AD is a subclass of AnomalyDetectionException.
     *  ClientException is a subclass of AnomalyDetectionException. All exception visible to
     *   Client is under ClientVisible. Two classes directly extends ClientException:
     *   - InternalFailure for "root cause unknown failure. Maybe transient." We can continue the
     *    detector running.
     *   - EndRunException for "failures that might impact the customer." The method endNow() is
     *    added to indicate whether the client should immediately terminate running a detector.
     *      + endNow() returns true for "unrecoverable issue". We want to terminate the detector run
     *       immediately.
     *      + endNow() returns false for "maybe unrecoverable issue but worth retrying a few more
     *       times." We want to wait for a few more times on different requests before terminating
     *        the detector run.
     *
     *  AD may not be able to get an anomaly grade but can find a feature vector.  Consider the
     *   case when the shingle is not ready.  In that case, AD just put NaN as anomaly grade and
     *    return the feature vector. If AD cannot even find a feature vector, AD throws
     *     EndRunException if there is an issue or returns empty response (all the numeric fields
     *      are Double.NaN and feature array is empty.  Do so so that customer can write painless
     *       script.) otherwise.
     *
     *
     *  Known causes of EndRunException with endNow returning false:
     *   + training data for cold start not available
     *   + cold start cannot succeed
     *   + unknown prediction error
     *   + memory circuit breaker tripped
     *   + invalid search query
     *
     *  Known causes of EndRunException with endNow returning true:
     *   + a model partition's memory size reached limit
     *   + models' total memory size reached limit
     *   + Having trouble querying feature data due to
     *    * index does not exist
     *    * all features have been disabled
     *
     *   + anomaly detector is not available
     *   + AD plugin is disabled
     *   + training data is invalid due to serious internal bug(s)
     *
     *  Known causes of InternalFailure:
     *   + threshold model node is not available
     *   + cluster read/write is blocked
     *   + cold start hasn't been finished
     *   + fail to get all of rcf model nodes' responses
     *   + fail to get threshold model node's response
     *   + RCF/Threshold model node failing to get checkpoint to restore model before timeout
     *   + Detection is throttle because previous detection query is running
     *
     */
    @Override
    protected void doExecute(Task task, ActionRequest actionRequest, ActionListener<AnomalyResultResponse> listener) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            AnomalyResultRequest request = AnomalyResultRequest.fromActionRequest(actionRequest);
            String adID = request.getConfigId();
            ActionListener<AnomalyResultResponse> original = listener;
            listener = ActionListener.wrap(r -> {
                hcDetectors.remove(adID);
                original.onResponse(r);
            }, e -> {
                // If exception is AnomalyDetectionException and it should not be counted in stats,
                // we will not count it in failure stats.
                if (!(e instanceof TimeSeriesException) || ((TimeSeriesException) e).isCountedInStats()) {
                    adStats.getStat(StatNames.AD_EXECUTE_FAIL_COUNT.getName()).increment();
                    if (hcDetectors.contains(adID)) {
                        adStats.getStat(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName()).increment();
                    }
                }
                hcDetectors.remove(adID);
                original.onFailure(e);
            });

            if (!ADEnabledSetting.isADEnabled()) {
                throw new EndRunException(adID, ADCommonMessages.DISABLED_ERR_MSG, true).countedInStats(false);
            }

            adStats.getStat(StatNames.AD_EXECUTE_REQUEST_COUNT.getName()).increment();

            if (adCircuitBreakerService.isOpen()) {
                listener.onFailure(new LimitExceededException(adID, CommonMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
                return;
            }
            try {
                nodeStateManager
                    .getConfig(adID, AnalysisType.AD, resultProcessor.onGetConfig(listener, adID, request, Optional.of(hcDetectors)));
            } catch (Exception ex) {
                ResultProcessor.handleExecuteException(ex, listener, adID);
            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }
}
