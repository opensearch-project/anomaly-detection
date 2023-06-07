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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.action.support.TransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.ClientException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.InternalFailure;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.common.exception.NotSerializedADExceptionName;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.feature.CompositeRetriever;
import org.opensearch.ad.feature.CompositeRetriever.PageIterator;
import org.opensearch.ad.feature.FeatureManager;
import org.opensearch.ad.feature.SinglePointFeatures;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.ad.ml.SingleStreamModelIdMapper;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.settings.EnabledSetting;
import org.opensearch.ad.stats.ADStats;
import org.opensearch.ad.stats.StatNames;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.io.stream.NotSerializableExceptionWrapper;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.NetworkExceptionHelper;
import org.opensearch.core.common.lease.Releasable;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.node.NodeClosedException;
import org.opensearch.rest.RestStatus;
import org.opensearch.sdk.ExtensionsRunner;
import org.opensearch.sdk.SDKClient.SDKRestClient;
import org.opensearch.sdk.SDKClusterService;
import org.opensearch.sdk.SDKNamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskManager;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ActionNotFoundTransportException;
import org.opensearch.transport.ConnectTransportException;
import org.opensearch.transport.NodeNotConnectedException;
import org.opensearch.transport.ReceiveTimeoutTransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import com.google.inject.Inject;

public class AnomalyResultTransportAction extends TransportAction<ActionRequest, AnomalyResultResponse> {

    private static final Logger LOG = LogManager.getLogger(AnomalyResultTransportAction.class);
    static final String NO_MODEL_ERR_MSG = "No RCF models are available either because RCF"
        + " models are not ready or all nodes are unresponsive or the system might have bugs.";
    static final String WAIT_FOR_THRESHOLD_ERR_MSG = "Exception in waiting for threshold result";
    static final String NODE_UNRESPONSIVE_ERR_MSG = "Model node is unresponsive.  Mute node";
    static final String READ_WRITE_BLOCKED = "Cannot read/write due to global block.";
    static final String INDEX_READ_BLOCKED = "Cannot read user index due to read block.";
    static final String NULL_RESPONSE = "Received null response from";

    static final String TROUBLE_QUERYING_ERR_MSG = "Having trouble querying data: ";
    static final String NO_ACK_ERR = "no acknowledgements from model hosting nodes.";

    private final ExtensionsRunner extensionsRunner;
    private final TransportService transportService;
    private final NodeStateManager stateManager;
    private final FeatureManager featureManager;
    private final ModelManager modelManager;
    private final TransportRequestOptions option;
    private final SDKClusterService sdkClusterService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ADStats adStats;
    private final ADCircuitBreakerService adCircuitBreakerService;
    private final ThreadPool threadPool;
    private final SDKRestClient sdkRestClient;
    private final ADTaskManager adTaskManager;

    // Cache HC detector id. This is used to count HC failure stats. We can tell a detector
    // is HC or not by checking if detector id exists in this field or not. Will add
    // detector id to this field when start to run realtime detection and remove detector
    // id once realtime detection done.
    private final Set<String> hcDetectors;
    private SDKNamedXContentRegistry xContentRegistry;
    private Settings settings;
    // within an interval, how many percents are used to process requests.
    // 1.0 means we use all of the detection interval to process requests.
    // to ensure we don't block next interval, it is better to set it less than 1.0.
    private final float intervalRatioForRequest;
    private int maxEntitiesPerInterval;
    private int pageSize;

    @Inject
    public AnomalyResultTransportAction(
        ExtensionsRunner extensionsRunner,
        ActionFilters actionFilters,
        TaskManager taskManager,
        SDKRestClient sdkRestClient,
        NodeStateManager manager,
        FeatureManager featureManager,
        ModelManager modelManager,
        SDKClusterService sdkClusterService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ADCircuitBreakerService adCircuitBreakerService,
        ADStats adStats,
        ThreadPool threadPool,
        SDKNamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager
    ) {
        super(AnomalyResultAction.NAME, actionFilters, taskManager);
        this.extensionsRunner = extensionsRunner;
        this.transportService = extensionsRunner.getExtensionTransportService();
        this.settings = extensionsRunner.getEnvironmentSettings();
        this.sdkRestClient = sdkRestClient;
        this.stateManager = manager;
        this.featureManager = featureManager;
        this.modelManager = modelManager;
        this.option = TransportRequestOptions
            .builder()
            .withType(TransportRequestOptions.Type.REG)
            .withTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings))
            .build();
        this.sdkClusterService = sdkClusterService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.adCircuitBreakerService = adCircuitBreakerService;
        this.adStats = adStats;
        this.threadPool = threadPool;
        this.hcDetectors = new HashSet<>();
        this.xContentRegistry = xContentRegistry;
        this.intervalRatioForRequest = AnomalyDetectorSettings.INTERVAL_RATIO_FOR_REQUESTS;

        this.maxEntitiesPerInterval = MAX_ENTITIES_PER_QUERY.get(settings);
        sdkClusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ENTITIES_PER_QUERY, it -> maxEntitiesPerInterval = it);

        this.pageSize = PAGE_SIZE.get(settings);
        sdkClusterService.getClusterSettings().addSettingsUpdateConsumer(PAGE_SIZE, it -> pageSize = it);
        this.adTaskManager = adTaskManager;
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
        try {
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

                        DiscoveryNode extensionNode = extensionsRunner.getExtensionNode();

                        Set<Entry<DiscoveryNode, Map<Entity, double[]>>> node2Entities = entityFeatures
                            .getResults()
                            .entrySet()
                            .stream()
                            .collect(
                                Collectors
                                    .groupingBy(
                                        // from entity name to its node
                                        e -> extensionNode,
                                        Collectors.toMap(Entry::getKey, Entry::getValue)
                                    )
                            )
                            .entrySet();

                        /* @anomaly.detection Commented until we have extension support for hashring : https://github.com/opensearch-project/opensearch-sdk-java/issues/200 
                        
                        Set<Entry<DiscoveryNode, Map<Entity, double[]>>> node2Entities = entityFeatures
                            .getResults()
                            .entrySet()
                            .stream()
                            .filter(e -> hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(e.getKey().toString()).isPresent())
                            .collect(
                                Collectors
                                    .groupingBy(
                                        // from entity name to its node
                                        e -> hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(e.getKey().toString()).get(),
                                        Collectors.toMap(Entry::getKey, Entry::getValue)
                                    )
                            )
                            .entrySet();
                        */

                        Iterator<Entry<DiscoveryNode, Map<Entity, double[]>>> iterator = node2Entities.iterator();

                        while (iterator.hasNext()) {
                            Entry<DiscoveryNode, Map<Entity, double[]>> entry = iterator.next();
                            DiscoveryNode modelNode = entry.getKey();
                            if (modelNode == null) {
                                iterator.remove();
                                continue;
                            }
                            String modelNodeId = modelNode.getId();
                            if (stateManager.isMuted(modelNodeId, detectorId)) {
                                LOG
                                    .info(
                                        String
                                            .format(Locale.ROOT, NODE_UNRESPONSIVE_ERR_MSG + " %s for detector %s", modelNodeId, detectorId)
                                    );
                                iterator.remove();
                            }
                        }

                        final AtomicReference<Exception> failure = new AtomicReference<>();
                        int nodeCount = node2Entities.size();
                        AtomicInteger responseCount = new AtomicInteger();
                        node2Entities.stream().forEach(nodeEntity -> {
                            DiscoveryNode node = nodeEntity.getKey();
                            EntityResultListener entityResultListener = new EntityResultListener(
                                node.getId(),
                                detectorId,
                                failure,
                                nodeCount,
                                pageIterator,
                                this,
                                responseCount
                            );
                            sdkRestClient
                                .execute(
                                    EntityResultAction.INSTANCE,
                                    new EntityResultRequest(detectorId, nodeEntity.getValue(), dataStartTime, dataEndTime),
                                    ActionListener
                                        .wrap(
                                            response -> entityResultListener.onResponse(response),
                                            ex -> entityResultListener.onFailure(ex)
                                        )
                                );
                        });

                    } catch (Exception e) {
                        LOG.error("Unexpected exception", e);
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

            adTaskManager
                .initRealtimeTaskCacheAndCleanupStaleCache(
                    adID,
                    anomalyDetector,
                    transportService,
                    ActionListener
                        .runAfter(
                            initRealtimeTaskCacheListener(adID),
                            () -> executeAnomalyDetection(listener, adID, request, anomalyDetector, dataStartTime, dataEndTime)
                        )
                );
        }, exception -> handleExecuteException(exception, listener, adID));
    }

    private ActionListener<Boolean> initRealtimeTaskCacheListener(String detectorId) {
        return ActionListener.wrap(r -> {
            if (r) {
                LOG.debug("Realtime task cache initied for detector {}", detectorId);
            }
        }, e -> LOG.error("Failed to init realtime task cache for " + detectorId, e));
    }

    private void executeAnomalyDetection(
        ActionListener<AnomalyResultResponse> listener,
        String adID,
        AnomalyResultRequest request,
        AnomalyDetector anomalyDetector,
        long dataStartTime,
        long dataEndTime
    ) {
        // HC logic starts here
        if (anomalyDetector.isMultientityDetector()) {
            Optional<Exception> previousException = stateManager.fetchExceptionAndClear(adID);

            if (previousException.isPresent()) {
                Exception exception = previousException.get();
                LOG.error(new ParameterizedMessage("Previous exception of [{}]", adID), exception);
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
                sdkRestClient,
                nextDetectionStartTime,
                settings,
                maxEntitiesPerInterval,
                pageSize,
                indexNameExpressionResolver,
                sdkClusterService
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
                listener
                    .onResponse(
                        new AnomalyResultResponse(
                            new ArrayList<FeatureData>(),
                            null,
                            null,
                            anomalyDetector.getDetectorIntervalInMinutes(),
                            true
                        )
                    );
            }
            return;
        }

        // HC logic ends and single entity logic starts here
        // We are going to use only 1 model partition for a single stream detector.
        // That's why we use 0 here.
        String rcfModelID = SingleStreamModelIdMapper.getRcfModelId(adID, 0);
        /* @anomaly.detection Commented until we have extension support for hashring : https://github.com/opensearch-project/opensearch-sdk-java/issues/200
        Optional<DiscoveryNode> asRCFNode = hashRing.getOwningNodeWithSameLocalAdVersionForRealtimeAD(rcfModelID);
        if (!asRCFNode.isPresent()) {
            listener.onFailure(new InternalFailure(adID, "RCF model node is not available."));
            return;
        }
        
        DiscoveryNode rcfNode = asRCFNode.get();
        */
        DiscoveryNode rcfNode = extensionsRunner.getExtensionNode();

        if (!shouldStart(listener, adID, anomalyDetector, rcfNode.getId(), rcfModelID)) {
            return;
        }

        featureManager
            .getCurrentFeatures(
                anomalyDetector,
                dataStartTime,
                dataEndTime,
                onFeatureResponseForSingleEntityDetector(adID, anomalyDetector, listener, rcfModelID, rcfNode, dataStartTime, dataEndTime)
            );
    }

    // For single entity detector
    private ActionListener<SinglePointFeatures> onFeatureResponseForSingleEntityDetector(
        String adID,
        AnomalyDetector detector,
        ActionListener<AnomalyResultResponse> listener,
        String rcfModelId,
        DiscoveryNode rcfNode,
        long dataStartTime,
        long dataEndTime
    ) {
        return ActionListener.wrap(featureOptional -> {
            List<FeatureData> featureInResponse = null;

            if (featureOptional.getUnprocessedFeatures().isPresent()) {
                featureInResponse = ParseUtils.getFeatureData(featureOptional.getUnprocessedFeatures().get(), detector);
            }

            if (!featureOptional.getProcessedFeatures().isPresent()) {
                Optional<Exception> exception = coldStartIfNoCheckPoint(detector);
                if (exception.isPresent()) {
                    listener.onFailure(exception.get());
                    return;
                }

                if (!featureOptional.getUnprocessedFeatures().isPresent()) {
                    // Feature not available is common when we have data holes. Respond empty response
                    // and don't log to avoid bloating our logs.
                    LOG.debug("No data in current detection window between {} and {} for {}", dataStartTime, dataEndTime, adID);
                    listener
                        .onResponse(
                            new AnomalyResultResponse(
                                new ArrayList<FeatureData>(),
                                "No data in current detection window",
                                null,
                                null,
                                false
                            )
                        );
                } else {
                    LOG.debug("Return at least current feature value between {} and {} for {}", dataStartTime, dataEndTime, adID);
                    listener
                        .onResponse(
                            new AnomalyResultResponse(featureInResponse, "No full shingle in current detection window", null, null, false)
                        );
                }
                return;
            }

            final AtomicReference<Exception> failure = new AtomicReference<Exception>();

            LOG.info("Sending RCF request to {} for model {}", rcfNode.getId(), rcfModelId);

            RCFActionListener rcfListener = new RCFActionListener(
                rcfModelId,
                failure,
                rcfNode.getId(),
                detector,
                listener,
                featureInResponse,
                adID
            );

            try {
                CompletableFuture<RCFResultResponse> rcfResultFuture = new CompletableFuture<>();
                sdkRestClient
                    .execute(
                        RCFResultAction.INSTANCE,
                        new RCFResultRequest(adID, rcfModelId, featureOptional.getProcessedFeatures().get()),
                        ActionListener.wrap(response -> rcfResultFuture.complete(response), ex -> rcfResultFuture.completeExceptionally(ex))
                    );
                RCFResultResponse rcfResultResponse = rcfResultFuture
                    .orTimeout(AnomalyDetectorSettings.REQUEST_TIMEOUT.get(settings).getMillis(), TimeUnit.MILLISECONDS)
                    .join();

                rcfListener.onResponse(rcfResultResponse);
            } catch (Exception ex) {
                rcfListener.onFailure(ex);
            }
            /*
            transportService
                .sendRequest(
                    rcfNode,
                    RCFResultAction.NAME,
                    new RCFResultRequest(adID, rcfModelId, featureOptional.getProcessedFeatures().get()),
                    option,
                    new ActionListenerResponseHandler<>(rcfListener, RCFResultResponse::new)
                );
            */
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
     * @throws Exception when the input failure is not a ResourceNotFoundException.
     *   List of exceptions we can throw
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
    private Exception coldStartIfNoModel(AtomicReference<Exception> failure, AnomalyDetector detector) throws Exception {
        Exception exp = failure.get();
        if (exp == null) {
            return null;
        }

        // return exceptions like LimitExceededException to caller
        if (!(exp instanceof ResourceNotFoundException)) {
            return exp;
        }

        // fetch previous cold start exception
        String adID = detector.getDetectorId();
        final Optional<Exception> previousException = stateManager.fetchExceptionAndClear(adID);
        if (previousException.isPresent()) {
            Exception exception = previousException.get();
            LOG.error("Previous exception of {}: {}", () -> adID, () -> exception);
            if (exception instanceof EndRunException && ((EndRunException) exception).isEndNow()) {
                return exception;
            }
        }
        LOG.info("Trigger cold start for {}", detector.getDetectorId());
        coldStart(detector);
        return previousException.orElse(new InternalFailure(adID, NO_MODEL_ERR_MSG));
    }

    private void findException(Throwable cause, String adID, AtomicReference<Exception> failure, String nodeId) {
        if (cause == null) {
            LOG.error(new ParameterizedMessage("Null input exception"));
            return;
        }
        if (cause instanceof Error) {
            // we cannot do anything with Error.
            LOG.error(new ParameterizedMessage("Error during prediction for {}: ", adID), cause);
            return;
        }

        Exception causeException = (Exception) cause;

        if (causeException instanceof AnomalyDetectionException) {
            failure.set(causeException);
        } else if (causeException instanceof NotSerializableExceptionWrapper) {
            // we only expect this happens on AD exceptions
            Optional<AnomalyDetectionException> actualException = NotSerializedADExceptionName
                .convertWrappedAnomalyDetectionException((NotSerializableExceptionWrapper) causeException, adID);
            if (actualException.isPresent()) {
                AnomalyDetectionException adException = actualException.get();
                failure.set(adException);
                if (adException instanceof ResourceNotFoundException) {
                    // During a rolling upgrade or blue/green deployment, ResourceNotFoundException might be caused by old node using RCF
                    // 1.0
                    // cannot recognize new checkpoint produced by the coordinating node using compact RCF. Add pressure to mute the node
                    // after consecutive failures.
                    stateManager.addPressure(nodeId, adID);
                }
            } else {
                // some unexpected bugs occur while predicting anomaly
                failure.set(new EndRunException(adID, CommonErrorMessages.BUG_RESPONSE, causeException, false));
            }
        } else if (causeException instanceof IndexNotFoundException
            && causeException.getMessage().contains(CommonName.CHECKPOINT_INDEX_NAME)) {
            // checkpoint index does not exist
            // ResourceNotFoundException will trigger cold start later
            failure.set(new ResourceNotFoundException(adID, causeException.getMessage()));
        } else if (causeException instanceof OpenSearchTimeoutException) {
            // we can have OpenSearchTimeoutException when a node tries to load RCF or
            // threshold model
            failure.set(new InternalFailure(adID, causeException));
        } else {
            // some unexpected bug occurred or cluster is unstable (e.g., ClusterBlockException) or index is red (e.g.
            // NoShardAvailableActionException) while predicting anomaly
            failure.set(new EndRunException(adID, CommonErrorMessages.BUG_RESPONSE, causeException, false));
        }
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

    // For single entity detector
    class RCFActionListener implements ActionListener<RCFResultResponse> {
        private String modelID;
        private AtomicReference<Exception> failure;
        private String rcfNodeID;
        private AnomalyDetector detector;
        private ActionListener<AnomalyResultResponse> listener;
        private List<FeatureData> featureInResponse;
        private final String adID;

        RCFActionListener(
            String modelID,
            AtomicReference<Exception> failure,
            String rcfNodeID,
            AnomalyDetector detector,
            ActionListener<AnomalyResultResponse> listener,
            List<FeatureData> features,
            String adID
        ) {
            this.modelID = modelID;
            this.failure = failure;
            this.rcfNodeID = rcfNodeID;
            this.detector = detector;
            this.listener = listener;
            this.featureInResponse = features;
            this.adID = adID;
        }

        @Override
        public void onResponse(RCFResultResponse response) {
            try {
                stateManager.resetBackpressureCounter(rcfNodeID, adID);
                if (response != null) {
                    listener
                        .onResponse(
                            new AnomalyResultResponse(
                                response.getAnomalyGrade(),
                                response.getConfidence(),
                                response.getRCFScore(),
                                featureInResponse,
                                null,
                                response.getTotalUpdates(),
                                detector.getDetectorIntervalInMinutes(),
                                false,
                                response.getRelativeIndex(),
                                response.getAttribution(),
                                response.getPastValues(),
                                response.getExpectedValuesList(),
                                response.getLikelihoodOfValues(),
                                response.getThreshold()
                            )
                        );
                } else {
                    LOG.warn(NULL_RESPONSE + " {} for {}", modelID, rcfNodeID);
                    listener.onFailure(new InternalFailure(adID, NO_MODEL_ERR_MSG));
                }
            } catch (Exception ex) {
                LOG.error(new ParameterizedMessage("Unexpected exception for [{}]", adID), ex);
                handleExecuteException(ex, listener, adID);
            }
        }

        @Override
        public void onFailure(Exception e) {
            try {
                if (e instanceof CompletionException) {
                    e = new ResourceNotFoundException(e.getMessage());
                }
                handlePredictionFailure(e, adID, rcfNodeID, failure);
                Exception exception = coldStartIfNoModel(failure, detector);
                if (exception != null) {
                    listener.onFailure(exception);
                } else {
                    listener.onFailure(new InternalFailure(adID, "Node connection problem or unexpected exception"));
                }
            } catch (Exception ex) {
                LOG.error(new ParameterizedMessage("Unexpected exception for [{}]", adID), ex);
                handleExecuteException(ex, listener, adID);
            }
        }
    }

    /**
     * Handle a prediction failure.  Possibly (i.e., we don't always need to do that)
     * convert the exception to a form that AD can recognize and handle and sets the
     * input failure reference to the converted exception.
     *
     * @param e prediction exception
     * @param adID Detector Id
     * @param nodeID Node Id
     * @param failure Parameter to receive the possibly converted function for the
     *  caller to deal with
     */
    private void handlePredictionFailure(Exception e, String adID, String nodeID, AtomicReference<Exception> failure) {
        LOG.error(new ParameterizedMessage("Received an error from node {} while doing model inference for {}", nodeID, adID), e);
        if (e == null) {
            return;
        }
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        if (hasConnectionIssue(cause)) {
            handleConnectionException(nodeID, adID);
        } else {
            findException(cause, adID, failure, nodeID);
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
            || NetworkExceptionHelper.isCloseConnectionException(e)
            || e instanceof ActionNotFoundTransportException;
    }

    private void handleConnectionException(String node, String detectorId) {
        final DiscoveryNodes nodes = sdkClusterService.state().nodes();
        if (!nodes.nodeExists(node)) {
            /* @anomaly.detection Commented until we have extension support for hashring : https://github.com/opensearch-project/opensearch-sdk-java/issues/200
            hashRing.buildCirclesForRealtimeAD();
            */
            return;
        }
        // rebuilding is not done or node is unresponsive
        stateManager.addPressure(node, detectorId);
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
     * @param rcfNodeId the rcf model hosting node ID for adID
     * @param rcfModelID the rcf model ID for adID
     * @return if we can start anomaly prediction.
     */
    private boolean shouldStart(
        ActionListener<AnomalyResultResponse> listener,
        String adID,
        AnomalyDetector detector,
        String rcfNodeId,
        String rcfModelID
    ) {
        ClusterState state = sdkClusterService.state();
        if (checkGlobalBlock(state)) {
            listener.onFailure(new InternalFailure(adID, READ_WRITE_BLOCKED));
            return false;
        }

        if (stateManager.isMuted(rcfNodeId, adID)) {
            listener
                .onFailure(
                    new InternalFailure(
                        adID,
                        String.format(Locale.ROOT, NODE_UNRESPONSIVE_ERR_MSG + " %s for rcf model %s", rcfNodeId, rcfModelID)
                    )
                );
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
    private Optional<Exception> coldStartIfNoCheckPoint(AnomalyDetector detector) {
        String detectorId = detector.getDetectorId();

        Optional<Exception> previousException = stateManager.fetchExceptionAndClear(detectorId);

        if (previousException.isPresent()) {
            Exception exception = previousException.get();
            LOG.error(new ParameterizedMessage("Previous exception of {}:", detectorId), exception);
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
            if (cause.getMessage().contains("index_not_found_exception")) {
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
        private AtomicReference<Exception> failure;
        private int nodeCount;
        private AtomicInteger responseCount;
        private PageIterator pageIterator;
        private PageListener pageListener;

        EntityResultListener(
            String nodeId,
            String adID,
            AtomicReference<Exception> failure,
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
                    stateManager.addPressure(nodeId, adID);
                } else {
                    stateManager.resetBackpressureCounter(nodeId, adID);
                }
            } catch (Exception ex) {
                LOG.error("Unexpected exception: {} for {}", ex, adID);
                handleException(ex);
            } finally {
                if (nodeCount == responseCount.incrementAndGet() && pageIterator != null && pageIterator.hasNext()) {
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
                if (nodeCount == responseCount.incrementAndGet() && pageIterator != null && pageIterator.hasNext()) {
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
