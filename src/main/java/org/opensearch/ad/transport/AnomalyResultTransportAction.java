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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.transport;

import static org.opensearch.ad.constant.CommonErrorMessages.INVALID_SEARCH_QUERY_MSG;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.PAGE_SIZE;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.ClientException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.InternalFailure;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.feature.CompositeRetriever;
import org.opensearch.ad.feature.CompositeRetriever.PageIterator;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.SinglePointFeatures;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.ModelPartitioner;
import org.opensearch.ad.ml.RcfResult;
import org.opensearch.ad.ml.rcf.CombinedRcfResult;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.StatNames;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.node.NodeClosedException;
import org.opensearch.rest.RestStatus;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ActionNotFoundTransportException;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

public class AnomalyResultTransportAction extends HandledTransportAction<ActionRequest, AnomalyResultResponse> {

    private static final Logger LOG = LogManager.getLogger(AnomalyResultTransportAction.class);
    static final String NO_MODEL_ERR_MSG = "No RCF models are available either because RCF"
        + " models are not ready or all nodes are unresponsive or the system might have bugs.";
    static final String WAIT_FOR_THRESHOLD_ERR_MSG = "Exception in waiting for threshold result";
    static final String NODE_UNRESPONSIVE_ERR_MSG = "Model node is unresponsive.  Mute model";
    static final String READ_WRITE_BLOCKED = "Cannot read/write due to global block.";
    static final String INDEX_READ_BLOCKED = "Cannot read user index due to read block.";
    static final String LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE = OpenSearchException.getExceptionName(new LimitExceededException("", ""));
    static final String NULL_RESPONSE = "Received null response from";

    static final String TROUBLE_QUERYING_ERR_MSG = "Having trouble querying data: ";
    static final String NO_ACK_ERR = "no acknowledgements from model hosting nodes.";

    private final TransportService transportService;
    private final NodeStateManager stateManager;
    private final FeatureManager featureManager;
    private final ModelPartitioner modelPartitioner;
    private final ModelManager modelManager;
    private final HashRing hashRing;
    private final TransportRequestOptions option;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ADStats adStats;
    private final ADCircuitBreakerService adCircuitBreakerService;
    private final ThreadPool threadPool;
    private final Client client;

    // cache HC detector id
    private final Set<String> hcDetectors;
    private NamedXContentRegistry xContentRegistry;
    private Settings settings;
    // within an interval, how many percents are used to process requests.
    // 1.0 means we use all of the detection interval to process requests.
    // to ensure we don't block next interval, it is better to set it less than 1.0.
    private final float intervalRatioForRequest;
    private int maxEntitiesPerInterval;
    private int pageSize;

    @Inject
    public AnomalyResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        Settings settings,
        Client client,
        NodeStateManager manager,
        FeatureManager featureManager,
        ModelManager modelManager,
        ModelPartitioner modelPartitioner,
        HashRing hashRing,
        ClusterService clusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ADCircuitBreakerService adCircuitBreakerService,
        ADStats adStats,
        ThreadPool threadPool,
        NamedXContentRegistry xContentRegistry
    ) {
        super(AnomalyResultAction.NAME, transportService, actionFilters, AnomalyResultRequest::new);
        this.transportService = transportService;
        this.settings = settings;
        this.client = client;
        this.stateManager = manager;
        this.featureManager = featureManager;
        this.modelPartitioner = modelPartitioner;
        this.modelManager = modelManager;
        this.hashRing = hashRing;
        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings))
            .build();
        this.clusterService = clusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.adStats = adStats;
        this.threadPool = threadPool;
        this.hcDetectors = new HashSet<>();
        this.xContentRegistry = xContentRegistry;
        this.intervalRatioForRequest = AnomalyDetectorSettings.INTERVAL_RATIO_FOR_REQUESTS;

        this.maxEntitiesPerInterval = MAX_ENTITIES_PER_QUERY.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ENTITIES_PER_QUERY, it -> maxEntitiesPerInterval = it);

        this.pageSize = PAGE_SIZE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(PAGE_SIZE, it -> pageSize = it);
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
     *  Also, AD is responsible for logging the stack trace.  To avoid bloating our logs, alerting
     *   should always just log the message of an AnomalyDetectionException exception by default.
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
            String adID = request.getAdID();
            ActionListener<AnomalyResultResponse> original = listener;
            listener = ActionListener.wrap(r -> {
                hcDetectors.remove(adID);
                original.onResponse(r);
            }, e -> {
                // If exception is AnomalyDetectionException and it should not be counted in stats,
                // we will not count it in failure stats.
                if (!(e instanceof AnomalyDetectionException) || ((AnomalyDetectionException) e).isCountedInStats()) {
                    adStats.getStat(StatNames.AD_EXECUTE_FAIL_COUNT.getName()).increment();
                    if (hcDetectors.contains(adID)) {
                        adStats.getStat(StatNames.AD_HC_EXECUTE_FAIL_COUNT.getName()).increment();
                    }
                }
                hcDetectors.remove(adID);
                original.onFailure(e);
            });

            if (!EnabledSetting.isADPluginEnabled()) {
                throw new EndRunException(adID, CommonErrorMessages.DISABLED_ERR_MSG, true).countedInStats(false);
            }

            adStats.getStat(StatNames.AD_EXECUTE_REQUEST_COUNT.getName()).increment();

            if (adCircuitBreakerService.isOpen()) {
                listener.onFailure(new LimitExceededException(adID, CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG, false));
                return;
            }
            try {
                stateManager.getAnomalyDetector(adID, onGetDetector(listener, adID, request));
            } catch (Exception ex) {
                handleExecuteException(ex, listener, adID);
            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    //

    /**
     * didn't use ActionListener.wrap so that I can
     * 1) use this to refer to the listener inside the listener
     * 2) pass parameters using constructors
     *
     */
    class PageListener implements ActionListener<CompositeRetriever.Page> {
        private PageIterator pageIterator;
        private String detectorId;
        private long dataStartTime;
        private long dataEndTime;

        PageListener(PageIterator pageIterator, String detectorId, long dataStartTime, long dataEndTime) {
            this.pageIterator = pageIterator;
            this.detectorId = detectorId;
            this.dataStartTime = dataStartTime;
            this.dataEndTime = dataEndTime;
        }

        @Override
        public void onResponse(CompositeRetriever.Page entityFeatures) {
            if (entityFeatures != null && false == entityFeatures.isEmpty()) {
                // wrap expensive operation inside ad threadpool
                threadPool.executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME).execute(() -> {
                    try {
                        Set<Entry<DiscoveryNode, Map<Entity, double[]>>> node2Entities = entityFeatures
                            .getResults()
                            .entrySet()
                            .stream()
                            .collect(
                                Collectors
                                    .groupingBy(
                                        // from entity name to its node
                                        e -> hashRing.getOwningNode(e.getKey().toString()).get(),
                                        Collectors.toMap(Entry::getKey, Entry::getValue)
                                    )
                            )
                            .entrySet();

                        Iterator<Entry<DiscoveryNode, Map<Entity, double[]>>> iterator = node2Entities.iterator();

                        while (iterator.hasNext()) {
                            Entry<DiscoveryNode, Map<Entity, double[]>> entry = iterator.next();
                            DiscoveryNode modelNode = entry.getKey();
                            if (modelNode == null) {
                                iterator.remove();
                                continue;
                            }
                            String modelNodeId = modelNode.getId();
                            if (stateManager.isMuted(modelNodeId)) {
                                LOG.info(String.format(Locale.ROOT, NODE_UNRESPONSIVE_ERR_MSG + " %s", modelNodeId));
                                iterator.remove();
                            }
                        }

                        final AtomicReference<AnomalyDetectionException> failure = new AtomicReference<>();
                        int nodeCount = node2Entities.size();
                        AtomicInteger responseCount = new AtomicInteger();
                        node2Entities.stream().forEach(nodeEntity -> {
                            DiscoveryNode node = nodeEntity.getKey();
                            transportService
                                .sendRequest(
                                    node,
                                    EntityResultAction.NAME,
                                    new EntityResultRequest(detectorId, nodeEntity.getValue(), dataStartTime, dataEndTime),
                                    option,
                                    new ActionListenerResponseHandler<>(
                                        new EntityResultListener(
                                            node.getId(),
                                            detectorId,
                                            failure,
                                            nodeCount,
                                            pageIterator,
                                            this,
                                            responseCount
                                        ),
                                        AcknowledgedResponse::new,
                                        ThreadPool.Names.SAME
                                    )
                                );
                        });
                    } catch (Exception e) {
                        LOG.error("Unexpetected exception", e);
                        handleException(e);
                    }
                });
            }
        }

        @Override
        public void onFailure(Exception e) {
            LOG.error("Unexpetected exception", e);
            handleException(e);
        }

        private void handleException(Exception e) {
            Exception convertedException = convertedQueryFailureException(e, detectorId);
            if (false == (convertedException instanceof AnomalyDetectionException)) {
                Throwable cause = ExceptionsHelper.unwrapCause(convertedException);
                convertedException = new InternalFailure(detectorId, cause);
            }
            stateManager.setException(detectorId, convertedException);
        }
    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        ActionListener<AnomalyResultResponse> listener,
        String adID,
        AnomalyResultRequest request
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (!detectorOptional.isPresent()) {
                listener.onFailure(new EndRunException(adID, "AnomalyDetector is not available.", true));
                return;
            }

            AnomalyDetector anomalyDetector = detectorOptional.get();
            if (anomalyDetector.isMultientityDetector()) {
                hcDetectors.add(adID);
                adStats.getStat(StatNames.AD_HC_EXECUTE_REQUEST_COUNT.getName()).increment();
            }

            long delayMillis = Optional
                .ofNullable((IntervalTimeConfiguration) anomalyDetector.getWindowDelay())
                .map(t -> t.toDuration().toMillis())
                .orElse(0L);
            long dataStartTime = request.getStart() - delayMillis;
            long dataEndTime = request.getEnd() - delayMillis;

            List<String> categoryField = anomalyDetector.getCategoryField();
            if (categoryField != null) {
                Optional<AnomalyDetectionException> previousException = stateManager.fetchExceptionAndClear(adID);

                if (previousException.isPresent()) {
                    Exception exception = previousException.get();
                    LOG.error("Previous exception of {}: {}", adID, exception);
                    if (exception instanceof EndRunException) {
                        EndRunException endRunException = (EndRunException) exception;
                        if (endRunException.isEndNow()) {
                            listener.onFailure(exception);
                            return;
                        }
                    }
                }

                // assume request are in epoch milliseconds
                long nextDetectionStartTime = request.getEnd() + (long) (anomalyDetector.getDetectorIntervalInMilliseconds()
                    * intervalRatioForRequest);

                CompositeRetriever compositeRetriever = new CompositeRetriever(
                    dataStartTime,
                    dataEndTime,
                    anomalyDetector,
                    xContentRegistry,
                    client,
                    nextDetectionStartTime,
                    settings,
                    maxEntitiesPerInterval,
                    pageSize
                );

                PageIterator pageIterator = null;

                try {
                    pageIterator = compositeRetriever.iterator();
                } catch (Exception e) {
                    listener
                        .onFailure(
                            new EndRunException(anomalyDetector.getDetectorId(), CommonErrorMessages.INVALID_SEARCH_QUERY_MSG, e, false)
                        );
                    return;
                }

                PageListener getEntityFeatureslistener = new PageListener(pageIterator, adID, dataStartTime, dataEndTime);

                if (pageIterator.hasNext()) {
                    pageIterator.next(getEntityFeatureslistener);
                }

                // We don't know when the pagination will not finish. To not
                // block the following interval request to start, we return immediately.
                // Pagination will stop itself when the time is up.
                if (previousException.isPresent()) {
                    listener.onFailure(previousException.get());
                } else {
                    listener.onResponse(new AnomalyResultResponse(Double.NaN, Double.NaN, Double.NaN, new ArrayList<FeatureData>()));
                }
                return;
            }

            String thresholdModelID = modelPartitioner.getThresholdModelId(adID);
            Optional<DiscoveryNode> asThresholdNode = hashRing.getOwningNode(thresholdModelID);
            if (!asThresholdNode.isPresent()) {
                listener.onFailure(new InternalFailure(adID, "Threshold model node is not available."));
                return;
            }

            DiscoveryNode thresholdNode = asThresholdNode.get();

            if (!shouldStart(listener, adID, anomalyDetector, thresholdNode.getId(), thresholdModelID)) {
                return;
            }

            featureManager
                .getCurrentFeatures(
                    anomalyDetector,
                    dataStartTime,
                    dataEndTime,
                    onFeatureResponse(adID, anomalyDetector, listener, thresholdModelID, thresholdNode, dataStartTime, dataEndTime)
                );
        }, exception -> handleExecuteException(exception, listener, adID));

    }

    private ActionListener<SinglePointFeatures> onFeatureResponse(
        String adID,
        AnomalyDetector detector,
        ActionListener<AnomalyResultResponse> listener,
        String thresholdModelID,
        DiscoveryNode thresholdNode,
        long dataStartTime,
        long dataEndTime
    ) {
        return ActionListener.wrap(featureOptional -> {
            List<FeatureData> featureInResponse = null;

            if (featureOptional.getUnprocessedFeatures().isPresent()) {
                featureInResponse = ParseUtils.getFeatureData(featureOptional.getUnprocessedFeatures().get(), detector);
            }

            if (!featureOptional.getProcessedFeatures().isPresent()) {
                Optional<AnomalyDetectionException> exception = coldStartIfNoCheckPoint(detector);
                if (exception.isPresent()) {
                    listener.onFailure(exception.get());
                    return;
                }

                if (!featureOptional.getUnprocessedFeatures().isPresent()) {
                    // Feature not available is common when we have data holes. Respond empty response
                    // so that alerting will not print stack trace to avoid bloating our logs.
                    LOG.info("No data in current detection window between {} and {} for {}", dataStartTime, dataEndTime, adID);
                    listener
                        .onResponse(
                            new AnomalyResultResponse(
                                Double.NaN,
                                Double.NaN,
                                Double.NaN,
                                new ArrayList<FeatureData>(),
                                "No data in current detection window"
                            )
                        );
                } else {
                    LOG.info("Return at least current feature value between {} and {} for {}", dataStartTime, dataEndTime, adID);
                    listener
                        .onResponse(
                            new AnomalyResultResponse(
                                Double.NaN,
                                Double.NaN,
                                Double.NaN,
                                featureInResponse,
                                "No full shingle in current detection window"
                            )
                        );
                }
                return;
            }

            // Can throw LimitExceededException when a single partition is more than X% of heap memory.
            // Compute this number once and the value won't change unless the coordinating AD node for an
            // detector changes or the cluster size changes.
            int rcfPartitionNum = stateManager.getPartitionNumber(adID, detector);

            List<RCFResultResponse> rcfResults = new ArrayList<>();

            final AtomicReference<AnomalyDetectionException> failure = new AtomicReference<AnomalyDetectionException>();

            final AtomicInteger responseCount = new AtomicInteger();

            for (int i = 0; i < rcfPartitionNum; i++) {
                String rcfModelID = modelPartitioner.getRcfModelId(adID, i);

                Optional<DiscoveryNode> rcfNode = hashRing.getOwningNode(rcfModelID.toString());
                if (!rcfNode.isPresent()) {
                    continue;
                }
                String rcfNodeId = rcfNode.get().getId();
                if (stateManager.isMuted(rcfNodeId)) {
                    LOG.info(String.format(Locale.ROOT, NODE_UNRESPONSIVE_ERR_MSG + " %s", rcfNodeId));
                    continue;
                }

                LOG.info("Sending RCF request to {} for model {}", rcfNodeId, rcfModelID);

                RCFActionListener rcfListener = new RCFActionListener(
                    rcfResults,
                    rcfModelID.toString(),
                    failure,
                    rcfNodeId,
                    detector,
                    listener,
                    thresholdModelID,
                    thresholdNode,
                    featureInResponse,
                    rcfPartitionNum,
                    responseCount,
                    adID,
                    detector.getEnabledFeatureIds().size()
                );

                transportService
                    .sendRequest(
                        rcfNode.get(),
                        RCFResultAction.NAME,
                        new RCFResultRequest(adID, rcfModelID, featureOptional.getProcessedFeatures().get()),
                        option,
                        new ActionListenerResponseHandler<>(rcfListener, RCFResultResponse::new)
                    );
            }
        }, exception -> { handleQueryFailure(exception, listener, adID); });
    }

    private void handleQueryFailure(Exception exception, ActionListener<AnomalyResultResponse> listener, String adID) {
        Exception convertedQueryFailureException = convertedQueryFailureException(exception, adID);

        if (convertedQueryFailureException instanceof EndRunException) {
            // invalid feature query
            listener.onFailure(convertedQueryFailureException);
        } else {
            handleExecuteException(convertedQueryFailureException, listener, adID);
        }
    }

    /**
     * Convert a query related exception to EndRunException
     *
     * These query exception can happen during the starting phase of the OpenSearch
     * process.  Thus, set the stopNow parameter of these EndRunException to false
     * and confirm the EndRunException is not a false positive.
     *
     * @param exception Exception
     * @param adID detector Id
     * @return the converted exception if the exception is query related
     */
    private Exception convertedQueryFailureException(Exception exception, String adID) {
        if (ExceptionUtil.isIndexNotAvailable(exception)) {
            return new EndRunException(adID, TROUBLE_QUERYING_ERR_MSG + exception.getMessage(), false).countedInStats(false);
        } else if (exception instanceof SearchPhaseExecutionException && invalidQuery((SearchPhaseExecutionException) exception)) {
            // This is to catch invalid aggregation on wrong field type. For example,
            // sum aggregation on text field. We should end detector run for such case.
            return new EndRunException(
                adID,
                INVALID_SEARCH_QUERY_MSG + " " + ((SearchPhaseExecutionException) exception).getDetailedMessage(),
                exception,
                false
            ).countedInStats(false);
        }

        return exception;
    }

    /**
     * Verify failure of rcf or threshold models. If there is no model, trigger cold
     * start. If there is an exception for the previous cold start of this detector,
     * throw exception to the caller.
     *
     * @param failure  object that may contain exceptions thrown
     * @param detector detector object
     * @return exception if AD job execution gets resource not found exception
     * @throws AnomalyDetectionException List of exceptions we can throw
     *     1. Exception from cold start:
     *       1). InternalFailure due to
     *         a. OpenSearchTimeoutException thrown by putModelCheckpoint during cold start
     *       2). EndRunException with endNow equal to false
     *         a. training data not available
     *         b. cold start cannot succeed
     *         c. invalid training data
     *       3) EndRunException with endNow equal to true
     *         a. invalid search query
     *     2. LimitExceededException from one of RCF model node when the total size of the models
     *      is more than X% of heap memory.
     *     3. InternalFailure wrapping OpenSearchTimeoutException inside caused by
     *      RCF/Threshold model node failing to get checkpoint to restore model before timeout.
     */
    private AnomalyDetectionException coldStartIfNoModel(AtomicReference<AnomalyDetectionException> failure, AnomalyDetector detector)
        throws AnomalyDetectionException {
        AnomalyDetectionException exp = failure.get();
        if (exp == null) {
            return null;
        }

        // rethrow exceptions like LimitExceededException to caller
        if (!(exp instanceof ResourceNotFoundException)) {
            throw exp;
        }

        // fetch previous cold start exception
        String adID = detector.getDetectorId();
        final Optional<AnomalyDetectionException> previousException = stateManager.fetchExceptionAndClear(adID);
        if (previousException.isPresent()) {
            Exception exception = previousException.get();
            LOG.error("Previous exception of {}: {}", () -> adID, () -> exception);
            if (exception instanceof EndRunException && ((EndRunException) exception).isEndNow()) {
                return (EndRunException) exception;
            }
        }
        LOG.info("Trigger cold start for {}", detector.getDetectorId());
        coldStart(detector);
        return previousException.orElse(new InternalFailure(adID, NO_MODEL_ERR_MSG));
    }

    private void findException(Throwable cause, String adID, AtomicReference<AnomalyDetectionException> failure) {
        if (cause instanceof Error) {
            // we cannot do anything with Error.
            LOG.error(new ParameterizedMessage("Error during prediction for {}: ", adID), cause);
            return;
        }

        Exception causeException = (Exception) cause;
        if (ExceptionUtil
            .isException(causeException, ResourceNotFoundException.class, ExceptionUtil.RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE)
            || (causeException instanceof IndexNotFoundException
                && causeException.getMessage().contains(CommonName.CHECKPOINT_INDEX_NAME))) {
            failure.set(new ResourceNotFoundException(adID, causeException.getMessage()));
        } else if (ExceptionUtil.isException(causeException, LimitExceededException.class, LIMIT_EXCEEDED_EXCEPTION_NAME_UNDERSCORE)) {
            failure.set(new LimitExceededException(adID, causeException.getMessage(), false));
        } else if (causeException instanceof OpenSearchTimeoutException) {
            // we can have OpenSearchTimeoutException when a node tries to load RCF or
            // threshold model
            failure.set(new InternalFailure(adID, causeException));
        } else {
            // some unexpected bugs occur while predicting anomaly
            failure.set(new EndRunException(adID, CommonErrorMessages.BUG_RESPONSE, causeException, false));
        }
    }

    private CombinedRcfResult getCombinedResult(List<RCFResultResponse> rcfResults, int numFeatures) {
        List<RcfResult> rcfResultLib = new ArrayList<>();
        for (RCFResultResponse result : rcfResults) {
            rcfResultLib.add(new RcfResult(result.getRCFScore(), result.getConfidence(), result.getForestSize(), result.getAttribution()));
        }
        return modelManager.combineRcfResults(rcfResultLib, numFeatures);
    }

    void handleExecuteException(Exception ex, ActionListener<AnomalyResultResponse> listener, String adID) {
        if (ex instanceof ClientException) {
            listener.onFailure(ex);
        } else if (ex instanceof AnomalyDetectionException) {
            listener.onFailure(new InternalFailure((AnomalyDetectionException) ex));
        } else {
            Throwable cause = ExceptionsHelper.unwrapCause(ex);
            listener.onFailure(new InternalFailure(adID, cause));
        }
    }

    private boolean invalidQuery(SearchPhaseExecutionException ex) {
        // If all shards return bad request and failure cause is IllegalArgumentException, we
        // consider the feature query is invalid and will not count the error in failure stats.
        for (ShardSearchFailure failure : ex.shardFailures()) {
            if (RestStatus.BAD_REQUEST != failure.status() || !(failure.getCause() instanceof IllegalArgumentException)) {
                return false;
            }
        }
        return true;
    }

    class RCFActionListener implements ActionListener<RCFResultResponse> {
        private List<RCFResultResponse> rcfResults;
        private String modelID;
        private AtomicReference<AnomalyDetectionException> failure;
        private String rcfNodeID;
        private AnomalyDetector detector;
        private ActionListener<AnomalyResultResponse> listener;
        private String thresholdModelID;
        private DiscoveryNode thresholdNode;
        private List<FeatureData> featureInResponse;
        private int nodeCount;
        private final AtomicInteger responseCount;
        private final String adID;
        private int numEnabledFeatures;

        RCFActionListener(
            List<RCFResultResponse> rcfResults,
            String modelID,
            AtomicReference<AnomalyDetectionException> failure,
            String rcfNodeID,
            AnomalyDetector detector,
            ActionListener<AnomalyResultResponse> listener,
            String thresholdModelID,
            DiscoveryNode thresholdNode,
            List<FeatureData> features,
            int nodeCount,
            AtomicInteger responseCount,
            String adID,
            int numEnabledFeatures
        ) {
            this.rcfResults = rcfResults;
            this.modelID = modelID;
            this.rcfNodeID = rcfNodeID;
            this.detector = detector;
            this.listener = listener;
            this.thresholdNode = thresholdNode;
            this.thresholdModelID = thresholdModelID;
            this.featureInResponse = features;
            this.failure = failure;
            this.nodeCount = nodeCount;
            this.responseCount = responseCount;
            this.adID = adID;
            this.numEnabledFeatures = numEnabledFeatures;
        }

        @Override
        public void onResponse(RCFResultResponse response) {
            try {
                stateManager.resetBackpressureCounter(rcfNodeID);
                if (response != null) {
                    rcfResults.add(response);
                } else {
                    LOG.warn(NULL_RESPONSE + " {} for {}", modelID, rcfNodeID);
                }
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
            } finally {
                if (nodeCount == responseCount.incrementAndGet()) {
                    handleRCFResults(numEnabledFeatures);
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                handlePredictionFailure(e, adID, rcfNodeID, failure);
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
            } finally {
                if (nodeCount == responseCount.incrementAndGet()) {
                    handleRCFResults(numEnabledFeatures);
                }
            }
        }

        private void handleRCFResults(int numFeatures) {
            try {
                AnomalyDetectionException exception = coldStartIfNoModel(failure, detector);
                if (exception != null) {
                    listener.onFailure(exception);
                    return;
                }

                if (rcfResults.isEmpty()) {
                    listener.onFailure(new InternalFailure(adID, NO_MODEL_ERR_MSG));
                    return;
                }

                CombinedRcfResult combinedResult = getCombinedResult(rcfResults, numFeatures);
                double combinedScore = combinedResult.getScore();

                final AtomicReference<AnomalyResultResponse> anomalyResultResponse = new AtomicReference<>();

                String thresholdNodeId = thresholdNode.getId();
                LOG.info("Sending threshold request to {} for model {}", thresholdNodeId, thresholdModelID);
                ThresholdActionListener thresholdListener = new ThresholdActionListener(
                    anomalyResultResponse,
                    featureInResponse,
                    thresholdNodeId,
                    detector,
                    combinedResult,
                    listener,
                    adID
                );
                transportService
                    .sendRequest(
                        thresholdNode,
                        ThresholdResultAction.NAME,
                        new ThresholdResultRequest(adID, thresholdModelID, combinedScore),
                        option,
                        new ActionListenerResponseHandler<>(thresholdListener, ThresholdResultResponse::new)
                    );
            } catch (Exception ex) {
                handleExecuteException(ex, listener, adID);
            }
        }
    }

    class ThresholdActionListener implements ActionListener<ThresholdResultResponse> {
        private AtomicReference<AnomalyResultResponse> anomalyResultResponse;
        private List<FeatureData> features;
        private AtomicReference<AnomalyDetectionException> failure;
        private String thresholdNodeID;
        private ActionListener<AnomalyResultResponse> listener;
        private AnomalyDetector detector;
        private CombinedRcfResult combinedResult;
        private String adID;

        ThresholdActionListener(
            AtomicReference<AnomalyResultResponse> anomalyResultResponse,
            List<FeatureData> features,
            String thresholdNodeID,
            AnomalyDetector detector,
            CombinedRcfResult combinedResult,
            ActionListener<AnomalyResultResponse> listener,
            String adID
        ) {
            this.anomalyResultResponse = anomalyResultResponse;
            this.features = features;
            this.thresholdNodeID = thresholdNodeID;
            this.detector = detector;
            this.combinedResult = combinedResult;
            this.failure = new AtomicReference<AnomalyDetectionException>();
            this.listener = listener;
            this.adID = adID;
        }

        @Override
        public void onResponse(ThresholdResultResponse response) {
            try {
                anomalyResultResponse
                    .set(new AnomalyResultResponse(response.getAnomalyGrade(), response.getConfidence(), Double.NaN, features));
                stateManager.resetBackpressureCounter(thresholdNodeID);
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
            } finally {
                handleThresholdResult();
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                handlePredictionFailure(e, adID, thresholdNodeID, failure);
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
            } finally {
                handleThresholdResult();
            }
        }

        private void handleThresholdResult() {
            try {
                AnomalyDetectionException exception = coldStartIfNoModel(failure, detector);
                if (exception != null) {
                    listener.onFailure(exception);
                    return;
                }

                if (anomalyResultResponse.get() != null) {
                    AnomalyResultResponse response = anomalyResultResponse.get();
                    double confidence = response.getConfidence() * combinedResult.getConfidence();
                    response = new AnomalyResultResponse(
                        response.getAnomalyGrade(),
                        confidence,
                        combinedResult.getScore(),
                        response.getFeatures()
                    );
                    listener.onResponse(response);
                } else if (failure.get() != null) {
                    listener.onFailure(failure.get());
                } else {
                    listener.onFailure(new InternalFailure(adID, "Node connection problem or unexpected exception"));
                }
            } catch (Exception ex) {
                handleExecuteException(ex, listener, adID);
            }
        }
    }

    private void handlePredictionFailure(Exception e, String adID, String nodeID, AtomicReference<AnomalyDetectionException> failure) {
        LOG.error(new ParameterizedMessage("Received an error from node {} while doing model inference for {}", nodeID, adID), e);
        if (e == null) {
            return;
        }
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (hasConnectionIssue(cause)) {
            handleConnectionException(nodeID);
        } else {
            findException(cause, adID, failure);
        }
    }

    /**
     * Check if the input exception indicates connection issues.
     * During blue-green deployment, we may see ActionNotFoundTransportException.
     * Count that as connection issue and isolate that node if it continues to happen.
     *
     * @param e exception
     * @return true if we get disconnected from the node or the node is not in the
     *         right state (being closed) or transport request times out (sent from TimeoutHandler.run)
     */
    private boolean hasConnectionIssue(Throwable e) {
        return e instanceof ConnectTransportException
            || e instanceof NodeClosedException
            || e instanceof ReceiveTimeoutTransportException
            || e instanceof NodeNotConnectedException
            || e instanceof ConnectException
            || e instanceof ActionNotFoundTransportException;
    }

    private void handleConnectionException(String node) {
        final DiscoveryNodes nodes = clusterService.state().nodes();
        if (!nodes.nodeExists(node) && hashRing.build()) {
            return;
        }
        // rebuilding is not done or node is unresponsive
        stateManager.addPressure(node);
    }

    /**
     * Since we need to read from customer index and write to anomaly result index,
     * we need to make sure we can read and write.
     *
     * @param state Cluster state
     * @return whether we have global block or not
     */
    private boolean checkGlobalBlock(ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ) != null
            || state.blocks().globalBlockedException(ClusterBlockLevel.WRITE) != null;
    }

    /**
     * Similar to checkGlobalBlock, we check block on the indices level.
     *
     * @param state   Cluster state
     * @param level   block level
     * @param indices the indices on which to check block
     * @return whether any of the index has block on the level.
     */
    private boolean checkIndicesBlocked(ClusterState state, ClusterBlockLevel level, String... indices) {
        // the original index might be an index expression with wildcards like "log*",
        // so we need to expand the expression to concrete index name
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), indices);

        return state.blocks().indicesBlockedException(level, concreteIndices) != null;
    }

    /**
     * Check if we should start anomaly prediction.
     *
     * @param listener listener to respond back to AnomalyResultRequest.
     * @param adID     detector ID
     * @param detector detector instance corresponds to adID
     * @param thresholdNodeId the threshold model hosting node ID for adID
     * @param thresholdModelID the threshold model ID for adID
     * @return if we can start anomaly prediction.
     */
    private boolean shouldStart(
        ActionListener<AnomalyResultResponse> listener,
        String adID,
        AnomalyDetector detector,
        String thresholdNodeId,
        String thresholdModelID
    ) {
        ClusterState state = clusterService.state();
        if (checkGlobalBlock(state)) {
            listener.onFailure(new InternalFailure(adID, READ_WRITE_BLOCKED));
            return false;
        }

        if (stateManager.isMuted(thresholdNodeId)) {
            listener.onFailure(new InternalFailure(adID, String.format(Locale.ROOT, NODE_UNRESPONSIVE_ERR_MSG + " %s", thresholdModelID)));
            return false;
        }

        if (checkIndicesBlocked(state, ClusterBlockLevel.READ, detector.getIndices().toArray(new String[0]))) {
            listener.onFailure(new InternalFailure(adID, INDEX_READ_BLOCKED));
            return false;
        }

        return true;
    }

    private void coldStart(AnomalyDetector detector) {
        String detectorId = detector.getDetectorId();

        // If last cold start is not finished, we don't trigger another one
        if (stateManager.isColdStartRunning(detectorId)) {
            return;
        }

        final Releasable coldStartFinishingCallback = stateManager.markColdStartRunning(detectorId);

        ActionListener<Optional<double[][]>> listener = ActionListener.wrap(trainingData -> {
            if (trainingData.isPresent()) {
                double[][] dataPoints = trainingData.get();

                ActionListener<Void> trainModelListener = ActionListener
                    .wrap(res -> { LOG.info("Succeeded in training {}", detectorId); }, exception -> {
                        if (exception instanceof AnomalyDetectionException) {
                            // e.g., partitioned model exceeds memory limit
                            stateManager.setException(detectorId, exception);
                        } else if (exception instanceof IllegalArgumentException) {
                            // IllegalArgumentException due to invalid training data
                            stateManager
                                .setException(detectorId, new EndRunException(detectorId, "Invalid training data", exception, false));
                        } else if (exception instanceof OpenSearchTimeoutException) {
                            stateManager
                                .setException(
                                    detectorId,
                                    new InternalFailure(detectorId, "Time out while indexing cold start checkpoint", exception)
                                );
                        } else {
                            stateManager
                                .setException(detectorId, new EndRunException(detectorId, "Error while training model", exception, false));
                        }
                    });

                modelManager
                    .trainModel(
                        detector,
                        dataPoints,
                        new ThreadedActionListener<>(LOG, threadPool, AnomalyDetectorPlugin.AD_THREAD_POOL_NAME, trainModelListener, false)
                    );
            } else {
                stateManager.setException(detectorId, new EndRunException(detectorId, "Cannot get training data", false));
            }
        }, exception -> {
            if (exception instanceof OpenSearchTimeoutException) {
                stateManager.setException(detectorId, new InternalFailure(detectorId, "Time out while getting training data", exception));
            } else if (exception instanceof AnomalyDetectionException) {
                // e.g., Invalid search query
                stateManager.setException(detectorId, exception);
            } else {
                stateManager.setException(detectorId, new EndRunException(detectorId, "Error while cold start", exception, false));
            }
        });

        final ActionListener<Optional<double[][]>> listenerWithReleaseCallback = ActionListener
            .runAfter(listener, coldStartFinishingCallback::close);

        threadPool
            .executor(AnomalyDetectorPlugin.AD_THREAD_POOL_NAME)
            .execute(
                () -> featureManager
                    .getColdStartData(
                        detector,
                        new ThreadedActionListener<>(
                            LOG,
                            threadPool,
                            AnomalyDetectorPlugin.AD_THREAD_POOL_NAME,
                            listenerWithReleaseCallback,
                            false
                        )
                    )
            );
    }

    /**
     * Check if checkpoint for an detector exists or not.  If not and previous
     *  run is not EndRunException whose endNow is true, trigger cold start.
     * @param detector detector object
     * @return previous cold start exception
     */
    private Optional<AnomalyDetectionException> coldStartIfNoCheckPoint(AnomalyDetector detector) {
        String detectorId = detector.getDetectorId();

        Optional<AnomalyDetectionException> previousException = stateManager.fetchExceptionAndClear(detectorId);

        if (previousException.isPresent()) {
            Exception exception = previousException.get();
            LOG.error("Previous exception of {}: {}", detectorId, exception);
            if (exception instanceof EndRunException && ((EndRunException) exception).isEndNow()) {
                return previousException;
            }
        }

        stateManager.getDetectorCheckpoint(detectorId, ActionListener.wrap(checkpointExists -> {
            if (!checkpointExists) {
                LOG.info("Trigger cold start for {}", detectorId);
                coldStart(detector);
            }
        }, exception -> {
            Throwable cause = ExceptionsHelper.unwrapCause(exception);
            if (cause instanceof IndexNotFoundException) {
                LOG.info("Trigger cold start for {}", detectorId);
                coldStart(detector);
            } else {
                String errorMsg = String.format(Locale.ROOT, "Fail to get checkpoint state for %s", detectorId);
                LOG.error(errorMsg, exception);
                stateManager.setException(detectorId, new AnomalyDetectionException(errorMsg, exception));
            }
        }));

        return previousException;
    }

    class EntityResultListener implements ActionListener<AcknowledgedResponse> {
        private String nodeId;
        private final String adID;
        private AtomicReference<AnomalyDetectionException> failure;
        private int nodeCount;
        private AtomicInteger responseCount;
        private PageIterator pageIterator;
        private PageListener pageListener;

        EntityResultListener(
            String nodeId,
            String adID,
            AtomicReference<AnomalyDetectionException> failure,
            int nodeCount,
            PageIterator pageIterator,
            PageListener pageListener,
            AtomicInteger responseCount
        ) {
            this.nodeId = nodeId;
            this.adID = adID;
            this.failure = failure;
            this.nodeCount = nodeCount;
            this.pageIterator = pageIterator;
            this.responseCount = responseCount;
            this.pageListener = pageListener;
        }

        @Override
        public void onResponse(AcknowledgedResponse response) {
            try {
                if (response.isAcknowledged() == false) {
                    LOG.error("Cannot send entities' features to {} for {}", nodeId, adID);
                    stateManager.addPressure(nodeId);
                } else {
                    stateManager.resetBackpressureCounter(nodeId);
                }
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
                handleException(ex);
            } finally {
                if (nodeCount == responseCount.incrementAndGet() && pageIterator.hasNext()) {
                    pageIterator.next(pageListener);
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                // e.g., we have connection issues with all of the nodes while restarting clusters
                LOG.error(new ParameterizedMessage("Cannot send entities' features to {} for {}", nodeId, adID), e);

                handleException(e);

            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
                handleException(ex);
            } finally {
                if (nodeCount == responseCount.incrementAndGet() && pageIterator.hasNext()) {
                    pageIterator.next(pageListener);
                }
            }
        }

        private void handleException(Exception e) {
            handlePredictionFailure(e, adID, nodeId, failure);

            if (failure.get() != null) {
                stateManager.setException(adID, failure.get());
            }
        }
    }
}
