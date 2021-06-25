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
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package org.opensearch.ad.task;

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.ad.constant.CommonErrorMessages.DETECTOR_IS_RUNNING;
import static org.opensearch.ad.constant.CommonErrorMessages.EXCEED_HISTORICAL_ANALYSIS_LIMIT;
import static org.opensearch.ad.constant.CommonErrorMessages.NO_ELIGIBLE_NODE_TO_RUN_DETECTOR;
import static org.opensearch.ad.model.ADTask.DETECTOR_ID_FIELD;
import static org.opensearch.ad.model.ADTask.ERROR_FIELD;
import static org.opensearch.ad.model.ADTask.EXECUTION_END_TIME_FIELD;
import static org.opensearch.ad.model.ADTask.EXECUTION_START_TIME_FIELD;
import static org.opensearch.ad.model.ADTask.IS_LATEST_FIELD;
import static org.opensearch.ad.model.ADTask.LAST_UPDATE_TIME_FIELD;
import static org.opensearch.ad.model.ADTask.PARENT_TASK_ID_FIELD;
import static org.opensearch.ad.model.ADTask.STATE_FIELD;
import static org.opensearch.ad.model.ADTask.STOPPED_BY_FIELD;
import static org.opensearch.ad.model.ADTask.TASK_PROGRESS_FIELD;
import static org.opensearch.ad.model.ADTask.TASK_TYPE_FIELD;
import static org.opensearch.ad.model.ADTaskType.ALL_HISTORICAL_TASK_TYPES;
import static org.opensearch.ad.model.ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES;
import static org.opensearch.ad.model.ADTaskType.REALTIME_TASK_TYPES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.BATCH_TASK_PIECE_INTERVAL_SECONDS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_OLD_AD_TASK_DOCS_PER_DETECTOR;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;
import static org.opensearch.ad.util.ExceptionUtil.getErrorMessage;
import static org.opensearch.ad.util.ExceptionUtil.getShardsFailure;
import static org.opensearch.ad.util.RestHandlerUtils.createXContentParserFromRegistry;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.ad.cluster.HashRing;
import org.opensearch.ad.common.exception.ADTaskCancelledException;
import org.opensearch.ad.common.exception.DuplicateTaskException;
import org.opensearch.ad.common.exception.InternalFailure;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.common.exception.ResourceNotFoundException;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskAction;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectionDateRange;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.ad.rest.handler.IndexAnomalyDetectorJobActionHandler;
import org.opensearch.ad.transport.ADBatchAnomalyResultAction;
import org.opensearch.ad.transport.ADBatchAnomalyResultRequest;
import org.opensearch.ad.transport.ADCancelTaskAction;
import org.opensearch.ad.transport.ADCancelTaskRequest;
import org.opensearch.ad.transport.ADTaskProfileAction;
import org.opensearch.ad.transport.ADTaskProfileNodeResponse;
import org.opensearch.ad.transport.ADTaskProfileRequest;
import org.opensearch.ad.transport.AnomalyDetectorJobResponse;
import org.opensearch.ad.transport.ForwardADTaskAction;
import org.opensearch.ad.transport.ForwardADTaskRequest;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.commons.authuser.User;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.index.reindex.UpdateByQueryAction;
import org.opensearch.index.reindex.UpdateByQueryRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.script.Script;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

/**
 * Manage AD task.
 */
public class ADTaskManager {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final Set<String> retryableErrors = ImmutableSet.of(EXCEED_HISTORICAL_ANALYSIS_LIMIT, NO_ELIGIBLE_NODE_TO_RUN_DETECTOR);
    private final Client client;
    private final ClusterService clusterService;
    private final NamedXContentRegistry xContentRegistry;
    private final AnomalyDetectionIndices detectionIndices;
    private final DiscoveryNodeFilterer nodeFilter;
    private final ADTaskCacheManager adTaskCacheManager;

    private final HashRing hashRing;
    private volatile Integer maxOldAdTaskDocsPerDetector;
    private volatile Integer pieceIntervalSeconds;
    private volatile TimeValue requestTimeout;

    public ADTaskManager(
        Settings settings,
        ClusterService clusterService,
        Client client,
        NamedXContentRegistry xContentRegistry,
        AnomalyDetectionIndices detectionIndices,
        DiscoveryNodeFilterer nodeFilter,
        HashRing hashRing,
        ADTaskCacheManager adTaskCacheManager
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.detectionIndices = detectionIndices;
        this.nodeFilter = nodeFilter;
        this.clusterService = clusterService;
        this.adTaskCacheManager = adTaskCacheManager;
        this.hashRing = hashRing;

        this.maxOldAdTaskDocsPerDetector = MAX_OLD_AD_TASK_DOCS_PER_DETECTOR.get(settings);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MAX_OLD_AD_TASK_DOCS_PER_DETECTOR, it -> maxOldAdTaskDocsPerDetector = it);

        this.pieceIntervalSeconds = BATCH_TASK_PIECE_INTERVAL_SECONDS.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(BATCH_TASK_PIECE_INTERVAL_SECONDS, it -> pieceIntervalSeconds = it);

        this.requestTimeout = REQUEST_TIMEOUT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(REQUEST_TIMEOUT, it -> requestTimeout = it);
    }

    /**
     * Start detector. Will create schedule job for realtime detector,
     * and start AD task for historical detector.
     *
     * @param detectorId detector id
     * @param detectionDateRange historical analysis date range
     * @param handler anomaly detector job action handler
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void startDetector(
        String detectorId,
        DetectionDateRange detectionDateRange,
        IndexAnomalyDetectorJobActionHandler handler,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getDetector(detectorId, (detector) -> {
            if (validateDetector(detector, listener)) { // validate if detector is ready to start
                if (detectionDateRange == null) {
                    // start realtime job
                    handler.startAnomalyDetectorJob(detector);
                } else {
                    // start historical analysis task
                    startHistoricalAnalysis(detector, detectionDateRange, user, transportService, listener);
                }
            }
        }, listener);
    }

    /**
     * Forward historical analysis task to coordinating node.
     *
     * @param detector anomaly detector
     * @param detectionDateRange historical analysis date range
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void startHistoricalAnalysis(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        String detectorId = detector.getDetectorId();
        Optional<DiscoveryNode> owningNode = hashRing.getOwningNode(detectorId);
        if (!owningNode.isPresent()) {
            logger.debug("Can't find eligible node to run as AD task's coordinating node");
            listener.onFailure(new OpenSearchStatusException("No eligible node to run detector", RestStatus.INTERNAL_SERVER_ERROR));
            return;
        }
        logger.debug("coordinating node is : {} for detector: {}", owningNode.get().getId(), detectorId);
        forwardDetectRequestToCoordinatingNode(
            detector,
            detectionDateRange,
            user,
            ADTaskAction.START,
            transportService,
            owningNode.get(),
            listener
        );
    }

    /**
     * We have three types of nodes in AD task process.
     *
     * 1.Forwarding node which receives external request. The request will \
     *   be sent to coordinating node first.
     * 2.Coordinating node which maintains running historical detector set.\
     *   We use hash ring to find coordinating node with detector id. \
     *   Coordinating node will find a worker node with least load and \
     *   dispatch AD task to that worker node.
     * 3.Worker node which will run AD task.
     *
     * This function is to forward the request to coordinating node.
     *
     * @param detector anomaly detector
     * @param detectionDateRange historical analysis date range
     * @param user user
     * @param adTaskAction AD task action
     * @param transportService transport service
     * @param node ES node
     * @param listener action listener
     */
    protected void forwardDetectRequestToCoordinatingNode(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        ADTaskAction adTaskAction,
        TransportService transportService,
        DiscoveryNode node,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        transportService
            .sendRequest(
                node,
                ForwardADTaskAction.NAME,
                new ForwardADTaskRequest(detector, detectionDateRange, user, adTaskAction),
                getTransportRequestOptions(),
                new ActionListenerResponseHandler<>(listener, AnomalyDetectorJobResponse::new)
            );
    }

    /**
     * Forward AD task to coordinating node
     *
     * @param adTask AD task
     * @param adTaskAction AD task action
     * @param transportService transport service
     * @param listener action listener
     */
    protected void forwardADTaskToCoordinatingNode(
        ADTask adTask,
        ADTaskAction adTaskAction,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        transportService
            .sendRequest(
                getCoordinatingNode(adTask),
                ForwardADTaskAction.NAME,
                new ForwardADTaskRequest(adTask, adTaskAction),
                getTransportRequestOptions(),
                new ActionListenerResponseHandler<>(listener, AnomalyDetectorJobResponse::new)
            );
    }

    private TransportRequestOptions getTransportRequestOptions() {
        return TransportRequestOptions.builder().withType(TransportRequestOptions.Type.REG).withTimeout(requestTimeout).build();
    }

    private DiscoveryNode getCoordinatingNode(ADTask adTask) {
        String coordinatingNode = adTask.getCoordinatingNode();
        DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
        DiscoveryNode targetNode = null;
        for (DiscoveryNode node : eligibleDataNodes) {
            if (node.getId().equals(coordinatingNode)) {
                targetNode = node;
                break;
            }
        }
        if (targetNode == null) {
            throw new ResourceNotFoundException(adTask.getDetectorId(), "AD task coordinating node not found");
        }
        return targetNode;
    }

    /**
     * Start anomaly detector on coordinating node.
     * Will init task index if not exist and write new AD task to index. If task index
     * exists, will check if there is task running. If no running task, reset old task
     * as not latest and clean old tasks which exceeds max old task doc limitation.
     * Then find out node with least load and dispatch task to that node(worker node).
     *
     * @param detector anomaly detector
     * @param detectionDateRange detection date range
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void startDetector(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        try {
            if (detectionIndices.doesDetectorStateIndexExist()) {
                // If detection index exist, check if latest AD task is running
                getLatestADTask(detector.getDetectorId(), getADTaskTypes(detectionDateRange), (adTask) -> {
                    if (!adTask.isPresent() || isADTaskEnded(adTask.get())) {
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        listener.onFailure(new OpenSearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
                    }
                }, transportService, listener);
            } else {
                // If detection index doesn't exist, create index and execute detector.
                detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("Created {} with mappings.", CommonName.DETECTION_STATE_INDEX);
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        String error = "Create index " + CommonName.DETECTION_STATE_INDEX + " with mappings not acknowledged";
                        logger.warn(error);
                        listener.onFailure(new OpenSearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        logger.error("Failed to init anomaly detection state index", e);
                        listener.onFailure(e);
                    }
                }));
            }
        } catch (Exception e) {
            logger.error("Failed to start detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private ADTaskType getADTaskType(AnomalyDetector detector, DetectionDateRange detectionDateRange) {
        if (detectionDateRange == null) {
            return detector.isMultientityDetector() ? ADTaskType.REALTIME_HC_DETECTOR : ADTaskType.REALTIME_SINGLE_ENTITY;
        } else {
            return detector.isMultientityDetector() ? ADTaskType.HISTORICAL_HC_DETECTOR : ADTaskType.HISTORICAL_SINGLE_ENTITY;
        }
    }

    private List<ADTaskType> getADTaskTypes(DetectionDateRange detectionDateRange) {
        return getADTaskTypes(detectionDateRange, false);
    }

    /**
     * Get list of task types.
     * 1. If detection date range is null, will return all realtime task types
     * 2. If detection date range is not null, will return all historical detector level tasks types
     *    if resetLatestTaskStateFlag is true; otherwise return all historical tasks types include
     *    HC entity level task type.
     * @param detectionDateRange detection date range
     * @param resetLatestTaskStateFlag reset latest task state or not
     * @return list of AD task types
     */
    private List<ADTaskType> getADTaskTypes(DetectionDateRange detectionDateRange, boolean resetLatestTaskStateFlag) {
        if (detectionDateRange == null) {
            return REALTIME_TASK_TYPES;
        } else {
            if (resetLatestTaskStateFlag) {
                // return all task types include HC entity task to make sure we can reset all tasks latest flag
                return ALL_HISTORICAL_TASK_TYPES;
            } else {
                return HISTORICAL_DETECTOR_TASK_TYPES;
            }
        }
    }

    /**
     * Stop detector.
     * For realtime detector, will set detector job as disabled.
     * For historical detector, will set its AD task as cancelled.
     *
     * @param detectorId detector id
     * @param historical stop historical analysis or not
     * @param handler AD job action handler
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void stopDetector(
        String detectorId,
        boolean historical,
        IndexAnomalyDetectorJobActionHandler handler,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getDetector(detectorId, (detector) -> {
            if (historical) {
                // stop historical analysis
                getLatestADTask(
                    detectorId,
                    HISTORICAL_DETECTOR_TASK_TYPES,
                    (task) -> stopHistoricalAnalysis(detectorId, task, user, listener),
                    transportService,
                    listener
                );
            } else {
                // stop realtime detector job
                handler.stopAnomalyDetectorJob(detectorId);
            }
        }, listener);
    }

    /**
     * Get anomaly detector and execute consumer function.
     *
     * @param detectorId detector id
     * @param consumer consumer function
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getDetector(String detectorId, Consumer<AnomalyDetector> consumer, ActionListener<T> listener) {
        GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener.onFailure(new OpenSearchStatusException("AnomalyDetector is not found", RestStatus.NOT_FOUND));
                return;
            }
            try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

                consumer.accept(detector);
            } catch (Exception e) {
                String message = "Failed to start anomaly detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
    }

    // TODO: delete this one later
    public <T> void getDetector(
        String detectorId,
        Consumer<AnomalyDetector> realTimeDetectorConsumer,
        Consumer<AnomalyDetector> historicalDetectorConsumer,
        ActionListener<T> listener
    ) {
        GetRequest getRequest = new GetRequest(AnomalyDetector.ANOMALY_DETECTORS_INDEX).id(detectorId);
        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener.onFailure(new OpenSearchStatusException("AnomalyDetector is not found", RestStatus.NOT_FOUND));
                return;
            }
            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetector detector = AnomalyDetector.parse(parser, response.getId(), response.getVersion());

                if (detector.isRealTimeDetector()) {
                    // run realtime detector
                    realTimeDetectorConsumer.accept(detector);
                } else {
                    // run historical detector
                    historicalDetectorConsumer.accept(detector);
                }
            } catch (Exception e) {
                String message = "Failed to start anomaly detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> listener.onFailure(exception)));
    }

    private List<String> taskTypeToString(List<ADTaskType> adTaskTypes) {
        return adTaskTypes.stream().map(type -> type.name()).collect(Collectors.toList());
    }

    /**
     * Get latest AD task and execute consumer function.
     *
     * @param detectorId detector id
     * @param adTaskTypes AD task types
     * @param function consumer function
     * @param transportService transport service
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getLatestADTask(
        String detectorId,
        List<ADTaskType> adTaskTypes,
        Consumer<Optional<ADTask>> function,
        TransportService transportService,
        ActionListener<T> listener
    ) {
        getLatestADTask(detectorId, null, adTaskTypes, function, transportService, true, listener);
    }

    /**
     * Get latest AD task and execute consumer function.
     *
     * @param detectorId detector id
     * @param entityValue entity value
     * @param adTaskTypes AD task types
     * @param function consumer function
     * @param transportService transport service
     * @param resetTaskState reset task state or not
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void getLatestADTask(
        String detectorId,
        String entityValue,
        List<ADTaskType> adTaskTypes,
        Consumer<Optional<ADTask>> function,
        TransportService transportService,
        boolean resetTaskState,
        ActionListener<T> listener
    ) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        if (adTaskTypes != null && adTaskTypes.size() > 0) {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(adTaskTypes)));
        }
        if (entityValue != null) {
            String path = "entity";
            String entityValueFieldName = path + ".value";
            TermQueryBuilder entityValueFilterQuery = QueryBuilders.termQuery(entityValueFieldName, entityValue);
            NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(path, entityValueFilterQuery, ScoreMode.None);
            query.filter(nestedQueryBuilder);
        }
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(query);
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(sourceBuilder);
        searchRequest.indices(CommonName.DETECTION_STATE_INDEX);

        client.search(searchRequest, ActionListener.wrap(r -> {
            // https://github.com/opendistro-for-elasticsearch/anomaly-detection/pull/359#discussion_r558653132
            // getTotalHits will be null when we track_total_hits is false in the query request.
            // Add more checking here to cover some unknown cases.
            if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value == 0) {
                // don't throw exception here as consumer functions need to handle missing task
                // in different way.
                function.accept(Optional.empty());
                return;
            }
            SearchHit searchHit = r.getHits().getAt(0);
            try (XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                ADTask adTask = ADTask.parse(parser, searchHit.getId());
                logger.debug("latest task id is {}, for detector {}", adTask.getTaskId(), adTask.getDetectorId());

                // TODO: check realtime detector job and reset realtime task as stopped.
                if (resetTaskState && adTask.isHistoricalTask() && !isADTaskEnded(adTask) && lastUpdateTimeExpired(adTask)) {
                    // TODO: fix HC task profile when profile change ready
                    // If AD task is still running, but its last updated time not refreshed
                    // for 2 pieces intervals, we will get task profile to check if it's
                    // really running and reset state as STOPPED if not running.
                    // For example, ES process crashes, then all tasks running on it will stay
                    // as running. We can reset the task state when next read happen.
                    getADTaskProfile(adTask, ActionListener.wrap(taskProfile -> {
                        if (taskProfile.getNodeId() == null) {
                            logger.debug("Reset task as STOPPED, taskId {}", adTask.getTaskId());
                            // If no node is running this task, reset it as STOPPED.
                            resetTaskStateAsStopped(adTask, transportService);
                            adTask.setState(ADTaskState.STOPPED.name());
                        }
                        function.accept(Optional.of(adTask));
                    }, e -> {
                        logger.error("Failed to get AD task profile for task " + adTask.getTaskId(), e);
                        listener.onFailure(e);
                    }));
                } else {
                    function.accept(Optional.of(adTask));
                }
            } catch (Exception e) {
                String message = "Failed to parse AD task for detector " + detectorId;
                logger.error(message, e);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.accept(Optional.empty());
            } else {
                logger.error("Failed to search AD task for detector " + detectorId, e);
                listener.onFailure(e);
            }
        }));
    }

    private void stopHistoricalAnalysis(
        String detectorId,
        Optional<ADTask> adTask,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        if (!adTask.isPresent()) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "Detector not started"));
            return;
        }

        if (isADTaskEnded(adTask.get())) {
            listener.onFailure(new ResourceNotFoundException(detectorId, "No running task found"));
            return;
        }

        String taskId = adTask.get().getTaskId();
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        String userName = user == null ? null : user.getName();

        ADCancelTaskRequest cancelTaskRequest = new ADCancelTaskRequest(detectorId, userName, dataNodes);
        client
            .execute(
                ADCancelTaskAction.INSTANCE,
                cancelTaskRequest,
                ActionListener
                    .wrap(response -> { listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK)); }, e -> {
                        logger.error("Failed to cancel AD task " + taskId + ", detector id: " + detectorId, e);
                        listener.onFailure(e);
                    })
            );
    }

    private boolean lastUpdateTimeExpired(ADTask adTask) {
        return adTask.getLastUpdateTime().plus(2 * pieceIntervalSeconds, ChronoUnit.SECONDS).isBefore(Instant.now());
    }

    /**
     * Check if AD task ended.
     *
     * @param adTask AD task
     * @return true if task state is one of STOPPED, FINISHED or FAILED.
     */
    public boolean isADTaskEnded(ADTask adTask) {
        return ADTaskState.STOPPED.name().equals(adTask.getState())
            || ADTaskState.FINISHED.name().equals(adTask.getState())
            || ADTaskState.FAILED.name().equals(adTask.getState());
    }

    private void resetTaskStateAsStopped(ADTask adTask, TransportService transportService) {
        if (!isADTaskEnded(adTask)) {
            cleanDetectorCache(adTask, transportService, () -> {
                Map<String, Object> updatedFields = new HashMap<>();
                updatedFields.put(STATE_FIELD, ADTaskState.STOPPED.name());
                updateADTask(adTask.getTaskId(), updatedFields);
                logger.debug("reset task as stopped, task id " + adTask.getTaskId());
            });
        }
    }

    /**
     * Clean detector cache on coordinating node.
     * If task's coordinating node is still in cluster, will forward stop
     * task request to coordinating node, then coordinating node will
     * remove detector from cache.
     * If task's coordinating node is not in cluster, we don't need to
     * forward stop task request to coordinating node.
     *
     * @param adTask AD task
     * @param transportService transport service
     * @param function will execute it when detector cache cleaned successfully or coordinating node left cluster
     */
    protected void cleanDetectorCache(ADTask adTask, TransportService transportService, AnomalyDetectorFunction function) {
        String coordinatingNode = adTask.getCoordinatingNode();
        DiscoveryNode[] eligibleDataNodes = nodeFilter.getEligibleDataNodes();
        logger.debug("coordinatingNode is: " + coordinatingNode + " for task " + adTask.getTaskId());
        DiscoveryNode targetNode = null;
        for (DiscoveryNode node : eligibleDataNodes) {
            if (node.getId().equals(coordinatingNode)) {
                targetNode = node;
                break;
            }
        }
        if (targetNode != null) {
            logger.debug("coordinatingNode found, will clean detector cache on it, detectorId: " + adTask.getDetectorId());
            forwardDetectRequestToCoordinatingNode(
                adTask.getDetector(),
                adTask.getDetectionDateRange(),
                null,
                ADTaskAction.FINISHED,
                transportService,
                targetNode,
                ActionListener
                    .wrap(
                        r -> { function.execute(); },
                        e -> { logger.error("Failed to clear detector cache on coordinating node " + coordinatingNode, e); }
                    )
            );
        } else {
            logger
                .warn(
                    "coordinating node"
                        + coordinatingNode
                        + " left cluster for detector "
                        + adTask.getDetectorId()
                        + ", task id "
                        + adTask.getTaskId()
                );
            function.execute();
        }
    }

    /**
     * Get AD task profile data.
     *
     * @param detectorId detector id
     * @param transportService transport service
     * @param listener action listener
     */
    public void getLatestADTaskProfile(String detectorId, TransportService transportService, ActionListener<DetectorProfile> listener) {
        getLatestADTask(detectorId, HISTORICAL_DETECTOR_TASK_TYPES, adTask -> {
            if (adTask.isPresent()) {
                getADTaskProfile(adTask.get(), ActionListener.wrap(adTaskProfile -> {
                    DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
                    profileBuilder.adTaskProfile(adTaskProfile);
                    listener.onResponse(profileBuilder.build());
                }, e -> {
                    logger.error("Failed to get AD task profile for task " + adTask.get().getTaskId(), e);
                    listener.onFailure(e);
                }));
            } else {
                listener.onFailure(new ResourceNotFoundException(detectorId, "Can't find latest task for detector"));
            }
        }, transportService, listener);
    }

    private void getADTaskProfile(ADTask adTask, ActionListener<ADTaskProfile> listener) {
        String detectorId = adTask.getDetectorId();

        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        ADTaskProfileRequest adTaskProfileRequest = new ADTaskProfileRequest(detectorId, dataNodes);
        client.execute(ADTaskProfileAction.INSTANCE, adTaskProfileRequest, ActionListener.wrap(response -> {
            if (response.hasFailures()) {
                listener.onFailure(response.failures().get(0));
                return;
            }

            List<ADTaskProfile> nodeResponses = response
                .getNodes()
                .stream()
                .filter(r -> r.getAdTaskProfile() != null)
                .map(ADTaskProfileNodeResponse::getAdTaskProfile)
                .collect(Collectors.toList());

            if (nodeResponses.size() > 1) {
                String error = nodeResponses.size()
                    + " tasks running for detector "
                    + adTask.getDetectorId()
                    + ". Please stop detector to kill all running tasks.";
                logger.error(error);
                listener.onFailure(new InternalFailure(adTask.getDetectorId(), error));
                return;
            }
            if (nodeResponses.size() == 0) {
                ADTaskProfile adTaskProfile = new ADTaskProfile(adTask, null, null, null, null, null, null);
                listener.onResponse(adTaskProfile);
            } else {
                ADTaskProfile nodeResponse = nodeResponses.get(0);
                ADTaskProfile adTaskProfile = new ADTaskProfile(
                    adTask,
                    nodeResponse.getShingleSize(),
                    nodeResponse.getRcfTotalUpdates(),
                    nodeResponse.getThresholdModelTrained(),
                    nodeResponse.getThresholdModelTrainingDataSize(),
                    nodeResponse.getModelSizeInBytes(),
                    nodeResponse.getNodeId()
                );
                listener.onResponse(adTaskProfile);
            }
        }, e -> {
            logger.error("Failed to get task profile for task " + adTask.getTaskId(), e);
            listener.onFailure(e);
        }));
    }

    /**
     * Get task profile for detector.
     * TODO: support return multiple task profiles for HC detector.
     * @param detectorId detector id
     * @return AD task profile
     * @throws LimitExceededException if there are multiple tasks for the detector
     */
    public ADTaskProfile getLocalADTaskProfileByDetectorId(String detectorId) {
        ADTaskProfile adTaskProfile = null;
        List<String> tasksOfDetector = adTaskCacheManager.getTasksOfDetector(detectorId);
        if (tasksOfDetector.size() > 1) {
            String error = "Multiple tasks are running for detector: " + detectorId + ". You can stop detector to kill all running tasks.";
            logger.warn(error);
            throw new LimitExceededException(error);
        }
        if (tasksOfDetector.size() == 1) {
            String taskId = tasksOfDetector.get(0);
            adTaskProfile = new ADTaskProfile(
                adTaskCacheManager.getShingle(taskId).size(),
                adTaskCacheManager.getRcfModel(taskId).getTotalUpdates(),
                adTaskCacheManager.isThresholdModelTrained(taskId),
                adTaskCacheManager.getThresholdModelTrainingDataSize(taskId),
                adTaskCacheManager.getModelSize(taskId),
                clusterService.localNode().getId()
            );
        }
        return adTaskProfile;
    }

    private boolean validateDetector(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) {
        String error = null;
        if (detector.getFeatureAttributes().size() == 0) {
            error = "Can't start detector job as no features configured";
        } else if (detector.getEnabledFeatureIds().size() == 0) {
            error = "Can't start detector job as no enabled features configured";
        }
        if (error != null) {
            listener.onFailure(new OpenSearchStatusException(error, RestStatus.BAD_REQUEST));
            return false;
        }
        return true;
    }

    /**
     * Start historical detector on coordinating node.
     * Will init task index if not exist and write new AD task to index. If task index
     * exists, will check if there is task running. If no running task, reset old task
     * as not latest and clean old tasks which exceeds limitation. Then find out node
     * with least load and dispatch task to that node(worker node).
     *
     * @param detector anomaly detector
     * @param detectionDateRange detection date range
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void startHistoricalAnalysisTask(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        try {
            if (detectionIndices.doesDetectorStateIndexExist()) {
                // If detection index exist, check if latest AD task is running
                getLatestADTask(detector.getDetectorId(), HISTORICAL_DETECTOR_TASK_TYPES, (adTask) -> {
                    if (!adTask.isPresent() || isADTaskEnded(adTask.get())) {
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        listener.onFailure(new OpenSearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
                    }
                }, transportService, listener);
            } else {
                // If detection index doesn't exist, create index and execute historical detector.
                detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("Created {} with mappings.", CommonName.DETECTION_STATE_INDEX);
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        String error = "Create index " + CommonName.DETECTION_STATE_INDEX + " with mappings not acknowledged";
                        logger.warn(error);
                        listener.onFailure(new OpenSearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        executeAnomalyDetector(detector, detectionDateRange, user, listener);
                    } else {
                        logger.error("Failed to init anomaly detection state index", e);
                        listener.onFailure(e);
                    }
                }));
            }
        } catch (Exception e) {
            logger.error("Failed to start historical detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private void executeAnomalyDetector(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest();
        updateByQueryRequest.indices(CommonName.DETECTION_STATE_INDEX);
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detector.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
        // make sure we reset all latest task as false when user switch from single entity to HC, vice versa.
        query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(getADTaskTypes(detectionDateRange, true))));
        updateByQueryRequest.setQuery(query);
        updateByQueryRequest.setRefresh(true);
        updateByQueryRequest.setScript(new Script("ctx._source.is_latest = false;"));

        client.execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(r -> {
            List<BulkItemResponse.Failure> bulkFailures = r.getBulkFailures();
            if (bulkFailures.isEmpty()) {
                createNewADTask(detector, detectionDateRange, user, listener);
            } else {
                logger.error("Failed to update old task's state for detector: {}, response: {} ", detector.getDetectorId(), r.toString());
                listener.onFailure(bulkFailures.get(0).getCause());
            }
        }, e -> {
            logger.error("Failed to reset old tasks as not latest for detector " + detector.getDetectorId(), e);
            listener.onFailure(e);
        }));
    }

    private void createNewADTask(
        AnomalyDetector detector,
        DetectionDateRange detectionDateRange,
        User user,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        String userName = user == null ? null : user.getName();
        Instant now = Instant.now();
        String taskType = getADTaskType(detector, detectionDateRange).name();
        ADTask adTask = new ADTask.Builder()
            .detectorId(detector.getDetectorId())
            .detector(detector)
            .isLatest(true)
            .taskType(taskType)
            .executionStartTime(now)
            .taskProgress(0.0f)
            .initProgress(0.0f)
            .state(ADTaskState.CREATED.name())
            .lastUpdateTime(now)
            .startedBy(userName)
            .coordinatingNode(clusterService.localNode().getId())
            .detectionDateRange(detectionDateRange)
            .user(user)
            .build();

        createADTaskDirectly(
            adTask,
            r -> onIndexADTaskResponse(
                r,
                adTask,
                (response, delegatedListener) -> cleanOldAdTaskDocs(response, adTask, delegatedListener),
                listener
            ),
            listener
        );
    }

    /**
     * Create AD task directly without checking index exists of not.
     *
     * @param adTask AD task
     * @param function consumer function
     * @param listener action listener
     * @param <T> action listener response type
     */
    public <T> void createADTaskDirectly(ADTask adTask, Consumer<IndexResponse> function, ActionListener<T> listener) {
        IndexRequest request = new IndexRequest(CommonName.DETECTION_STATE_INDEX);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            request
                .source(adTask.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE))
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            client.index(request, ActionListener.wrap(r -> function.accept(r), e -> {
                logger.error("Failed to create AD task for detector " + adTask.getDetectorId(), e);
                listener.onFailure(e);
            }));
        } catch (Exception e) {
            logger.error("Failed to create AD task for detector " + adTask.getDetectorId(), e);
            listener.onFailure(e);
        }
    }

    private void onIndexADTaskResponse(
        IndexResponse response,
        ADTask adTask,
        BiConsumer<IndexResponse, ActionListener<AnomalyDetectorJobResponse>> function,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        if (response == null || response.getResult() != CREATED) {
            String errorMsg = getShardsFailure(response);
            listener.onFailure(new OpenSearchStatusException(errorMsg, response.status()));
            return;
        }
        adTask.setTaskId(response.getId());
        ActionListener<AnomalyDetectorJobResponse> delegatedListener = ActionListener.wrap(r -> { listener.onResponse(r); }, e -> {
            handleADTaskException(adTask, e);
            if (e instanceof DuplicateTaskException) {
                listener.onFailure(new OpenSearchStatusException(DETECTOR_IS_RUNNING, RestStatus.BAD_REQUEST));
            } else {
                listener.onFailure(e);
                adTaskCacheManager.removeDetector(adTask.getDetectorId());
            }
        });
        try {
            // Put detector id in cache. If detector id already in cache, will throw
            // DuplicateTaskException. This is to solve race condition when user send
            // multiple start request for one historical detector.
            if (adTask.isHistoricalTask()) {
                adTaskCacheManager.add(adTask.getDetectorId(), adTask.getTaskType());
            }
        } catch (Exception e) {
            delegatedListener.onFailure(e);
            return;
        }
        if (function != null) {
            function.accept(response, delegatedListener);
        }
    }

    private void cleanOldAdTaskDocs(IndexResponse response, ADTask adTask, ActionListener<AnomalyDetectorJobResponse> delegatedListener) {
        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, adTask.getDetectorId()));
        query.filter(new TermQueryBuilder(IS_LATEST_FIELD, false));

        if (adTask.isHistoricalTask()) {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(HISTORICAL_DETECTOR_TASK_TYPES)));
        } else {
            query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(REALTIME_TASK_TYPES)));
        }

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder
            .query(query)
            .sort(EXECUTION_START_TIME_FIELD, SortOrder.DESC)
            // Search query "from" starts from 0.
            .from(maxOldAdTaskDocsPerDetector)
            .trackTotalHits(true)
            .size(MAX_OLD_AD_TASK_DOCS);
        searchRequest.source(sourceBuilder).indices(CommonName.DETECTION_STATE_INDEX);
        String detectorId = adTask.getDetectorId();

        deleteTaskDocs(detectorId, searchRequest, () -> {
            if (adTask.isHistoricalTask()) {
                // run batch result action for historical detection
                runBatchResultAction(response, adTask, delegatedListener);
            } else {
                // return response directly for realtime detection
                AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                    response.getId(),
                    response.getVersion(),
                    response.getSeqNo(),
                    response.getPrimaryTerm(),
                    RestStatus.OK
                );
                delegatedListener.onResponse(anomalyDetectorJobResponse);
            }
        }, delegatedListener);
    }

    private <T> void deleteTaskDocs(
        String detectorId,
        SearchRequest searchRequest,
        AnomalyDetectorFunction function,
        ActionListener<T> listener
    ) {
        ActionListener<SearchResponse> searchListener = ActionListener.wrap(r -> {
            Iterator<SearchHit> iterator = r.getHits().iterator();
            if (iterator.hasNext()) {
                BulkRequest bulkRequest = new BulkRequest();
                while (iterator.hasNext()) {
                    SearchHit searchHit = iterator.next();
                    try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        ADTask adTask = ADTask.parse(parser, searchHit.getId());
                        logger.debug("Delete old task: {} of detector: {}", adTask.getTaskId(), adTask.getDetectorId());
                        // TODO: add deleted task in cache to delete their AD results in hourly cron job
                        bulkRequest.add(new DeleteRequest(CommonName.DETECTION_STATE_INDEX).id(adTask.getTaskId()));
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                }
                client.execute(BulkAction.INSTANCE, bulkRequest, ActionListener.wrap(res -> {
                    logger.info("AD tasks deleted for detector {}", detectorId);
                    // TODO: delete child tasks of HC detector task
                    function.execute();
                }, e -> {
                    logger.warn("Failed to clean AD tasks for detector " + detectorId, e);
                    listener.onFailure(e);
                }));
            } else {
                function.execute();
            }
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.execute();
            } else {
                listener.onFailure(e);
            }
        });

        client.search(searchRequest, searchListener);
    }

    private void runBatchResultAction(IndexResponse response, ADTask adTask, ActionListener<AnomalyDetectorJobResponse> listener) {
        client.execute(ADBatchAnomalyResultAction.INSTANCE, new ADBatchAnomalyResultRequest(adTask), ActionListener.wrap(r -> {
            String remoteOrLocal = r.isRunTaskRemotely() ? "remote" : "local";
            logger
                .info(
                    "AD task {} of detector {} dispatched to {} node {}",
                    adTask.getTaskId(),
                    adTask.getDetectorId(),
                    remoteOrLocal,
                    r.getNodeId()
                );
            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                response.getId(),
                response.getVersion(),
                response.getSeqNo(),
                response.getPrimaryTerm(),
                RestStatus.OK
            );
            listener.onResponse(anomalyDetectorJobResponse);
        }, e -> listener.onFailure(e)));
    }

    /**
     * Handle exceptions for AD task. Update task state and record error message.
     *
     * @param adTask AD task
     * @param e exception
     */
    public void handleADTaskException(ADTask adTask, Exception e) {
        // TODO: handle timeout exception
        String state = ADTaskState.FAILED.name();
        Map<String, Object> updatedFields = new HashMap<>();
        if (e instanceof DuplicateTaskException) {
            // If user send multiple start detector request, we will meet race condition.
            // Cache manager will put first request in cache and throw DuplicateTaskException
            // for the second request. We will delete the second task.
            logger
                .warn(
                    "There is already one running task for detector, detectorId:"
                        + adTask.getDetectorId()
                        + ". Will delete task "
                        + adTask.getTaskId()
                );
            deleteADTask(adTask.getTaskId());
            return;
        }
        if (e instanceof ADTaskCancelledException) {
            logger.info("AD task cancelled, taskId: {}, detectorId: {}", adTask.getTaskId(), adTask.getDetectorId());
            state = ADTaskState.STOPPED.name();
            String stoppedBy = ((ADTaskCancelledException) e).getCancelledBy();
            if (stoppedBy != null) {
                updatedFields.put(STOPPED_BY_FIELD, stoppedBy);
            }
        } else {
            logger.error("Failed to execute AD batch task, task id: " + adTask.getTaskId() + ", detector id: " + adTask.getDetectorId(), e);
        }
        updatedFields.put(ERROR_FIELD, getErrorMessage(e));
        updatedFields.put(STATE_FIELD, state);
        updatedFields.put(EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli());
        updateADTask(adTask.getTaskId(), updatedFields);
    }

    /**
     * Update AD task with specific fields.
     *
     * @param taskId AD task id
     * @param updatedFields updated fields, key: filed name, value: new value
     */
    public void updateADTask(String taskId, Map<String, Object> updatedFields) {
        updateADTask(taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated AD task successfully: {}", response.status());
            } else {
                logger.error("Failed to update AD task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task: " + taskId, e); }));
    }

    /**
     * Update AD task for specific fields.
     *
     * @param taskId task id
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateADTask(String taskId, Map<String, Object> updatedFields, ActionListener<UpdateResponse> listener) {
        UpdateRequest updateRequest = new UpdateRequest(CommonName.DETECTION_STATE_INDEX, taskId);
        Map<String, Object> updatedContent = new HashMap<>();
        updatedContent.putAll(updatedFields);
        updatedContent.put(LAST_UPDATE_TIME_FIELD, Instant.now().toEpochMilli());
        updateRequest.doc(updatedContent);
        updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.update(updateRequest, listener);
    }

    /**
     * Delete AD task with task id.
     *
     * @param taskId AD task id
     */
    public void deleteADTask(String taskId) {
        deleteADTask(
            taskId,
            ActionListener
                .wrap(
                    r -> { logger.info("Deleted AD task {} with status: {}", taskId, r.status()); },
                    e -> { logger.error("Failed to delete AD task " + taskId, e); }
                )
        );
    }

    /**
     * Delete AD task with task id.
     *
     * @param taskId AD task id
     * @param listener action listener
     */
    public void deleteADTask(String taskId, ActionListener<DeleteResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(CommonName.DETECTION_STATE_INDEX, taskId);
        client.delete(deleteRequest, listener);
    }

    /**
     * Cancel running task by detector id.
     *
     * @param detectorId detector id
     * @param reason reason to cancel AD task
     * @param userName which user cancel the AD task
     * @return AD task cancellation state
     */
    public ADTaskCancellationState cancelLocalTaskByDetectorId(String detectorId, String reason, String userName) {
        ADTaskCancellationState cancellationState = adTaskCacheManager.cancelByDetectorId(detectorId, reason, userName);
        logger
            .debug(
                "Cancelled AD task for detector: {}, state: {}, cancelled by: {}, reason: {}",
                detectorId,
                cancellationState,
                userName,
                reason
            );
        return cancellationState;
    }

    /**
     * Delete AD tasks docs.
     *
     * @param detectorId detector id
     * @param function AD function
     * @param listener action listener
     */
    public void deleteADTasks(String detectorId, AnomalyDetectorFunction function, ActionListener<DeleteResponse> listener) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(CommonName.DETECTION_STATE_INDEX);

        BoolQueryBuilder query = new BoolQueryBuilder();
        query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, detectorId));

        request.setQuery(query);
        client.execute(DeleteByQueryAction.INSTANCE, request, ActionListener.wrap(r -> {
            logger.info("AD tasks deleted for detector {}", detectorId);
            function.execute();
        }, e -> {
            if (e instanceof IndexNotFoundException) {
                function.execute();
            } else {
                listener.onFailure(e);
            }
        }));
    }

    /**
     * Remove detector from cache on coordinating node.
     *
     * @param detectorId detector id
     */
    public void removeDetectorFromCache(String detectorId) {
        adTaskCacheManager.removeDetector(detectorId);
    }

    /**
     * Update latest AD task of detector.
     *
     * @param detectorId detector id
     * @param taskTypes task types
     * @param updatedFields updated fields, key: filed name, value: new value
     */
    public void updateLatestADTask(String detectorId, List<ADTaskType> taskTypes, Map<String, Object> updatedFields) {
        updateLatestADTask(
            detectorId,
            taskTypes,
            updatedFields,
            ActionListener
                .wrap(
                    r -> logger.debug("updated latest task of {}", detectorId),
                    e -> logger.warn("failed to update latest task for detector {}", detectorId)
                )
        );
    }

    /**
     * Update latest AD task of detector.
     *
     * @param detectorId detector id
     * @param taskTypes task types
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateLatestADTask(
        String detectorId,
        List<ADTaskType> taskTypes,
        Map<String, Object> updatedFields,
        ActionListener listener
    ) {
        getLatestADTask(detectorId, taskTypes, (adTask) -> {
            if (adTask.isPresent()) {
                updateADTask(adTask.get().getTaskId(), updatedFields);
            }
        }, null, listener);
    }

    /**
     * Update latest realtime task.
     *
     * @param detectorId detector id
     * @param state task state
     * @param error error
     * @param listener action listener
     */
    public void updateLatestRealtimeTask(
        String detectorId,
        ADTaskState state,
        Exception error,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        getLatestADTask(detectorId, REALTIME_TASK_TYPES, (adTask) -> {
            if (adTask.isPresent() && !isADTaskEnded(adTask.get())) {
                Map<String, Object> updatedFields = new HashMap<>();
                updatedFields.put(ADTask.STATE_FIELD, state.name());
                if (error != null) {
                    updatedFields.put(ADTask.ERROR_FIELD, error.getMessage());
                }
                updateADTask(adTask.get().getTaskId(), updatedFields, ActionListener.wrap(r -> {
                    if (error == null) {
                        listener.onResponse(new AnomalyDetectorJobResponse(detectorId, 0, 0, 0, RestStatus.OK));
                    } else {
                        listener.onFailure(error);
                    }
                }, e -> { listener.onFailure(e); }));
            } else {
                listener.onFailure(new OpenSearchStatusException("Anomaly detector job is already stopped: " + detectorId, RestStatus.OK));
            }
        }, null, listener);
    }

    /**
     * Send entity task done message to coordinating node.
     *
     * @param adTask AD task
     * @param exception exception of entity task
     * @param transportService transport service
     */
    protected void entityTaskDone(ADTask adTask, Exception exception, TransportService transportService) {
        entityTaskDone(
            adTask,
            exception,
            transportService,
            ActionListener
                .wrap(
                    r -> logger.debug("AD task forwarded to coordinating node, task id {}", adTask.getTaskId()),
                    e -> logger.debug("AD task failed to forward to coordinating node, task id {}", adTask.getTaskId())
                )
        );
    }

    private void entityTaskDone(
        ADTask adTask,
        Exception exception,
        TransportService transportService,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        try {
            ADTaskAction action = getAdEntityTaskAction(adTask, exception);
            forwardADTaskToCoordinatingNode(adTask, action, transportService, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Get AD entity task action based on exception.
     * 1. If exception is null, return NEXT_ENTITY action which will poll next
     *    entity to run.
     * 2. If exception is retryable, return PUSH_BACK_ENTITY action which will
     *    push entity back to pendig queue.
     * 3. If exception is task cancelled exception, return CANCEL action which
     *    will stop HC detector run.
     *
     * @param adTask AD task
     * @param exception exception
     * @return AD task action
     */
    private ADTaskAction getAdEntityTaskAction(ADTask adTask, Exception exception) {
        ADTaskAction action = ADTaskAction.NEXT_ENTITY;
        if (exception != null) {
            adTask.setError(getErrorMessage(exception));
            if (exception instanceof LimitExceededException && isRetryableError(exception.getMessage())) {
                action = ADTaskAction.PUSH_BACK_ENTITY;
            } else if (exception instanceof ADTaskCancelledException) {
                action = ADTaskAction.CANCEL;
            }
        }
        return action;
    }

    /**
     * Check if error is retryable.
     *
     * @param error error
     * @return retryable or not
     */
    public boolean isRetryableError(String error) {
        if (error == null) {
            return false;
        }
        return retryableErrors.stream().filter(e -> error.contains(e)).findFirst().isPresent();
    }

    /**
     * Set state for HC detector level task when all entities done.
     *
     * The state could be FINISHED,FAILED or STOPPED.
     * 1. If input task state is FINISHED, will check FINISHED entity task count. If
     * there is no FINISHED entity task, will set HC detector level task as FAILED; otherwise
     * set as FINISHED.
     * 2. If input task state is not FINISHED, will set HC detector level task's state as the same.
     *
     * @param adTask AD task
     * @param state AD task state
     * @param listener action listener
     */
    public void setHCDetectorTaskDone(ADTask adTask, ADTaskState state, ActionListener<AnomalyDetectorJobResponse> listener) {
        String detectorId = adTask.getDetectorId();
        String taskId = adTask.isEntityTask() ? adTask.getParentTaskId() : adTask.getTaskId();

        String detectorTaskId = adTask.isEntityTask() ? adTask.getParentTaskId() : adTask.getTaskId();

        logger.info("Historical HC detector done with state: {}. Remove from cache, detector id:{}", state.name(), detectorId);
        this.removeDetectorFromCache(detectorId);

        ActionListener<UpdateResponse> wrappedListener = ActionListener
            .wrap(
                response -> {
                    logger.info("Historical HC detector done with state: {}. Remove from cache, detector id:{}", state.name(), detectorId);
                },
                e -> { logger.error("Failed to update task: " + taskId, e); }
            );

        if (state == ADTaskState.FINISHED) {
            this.countEntityTasksByState(detectorTaskId, ImmutableList.of(ADTaskState.FINISHED), ActionListener.wrap(r -> {
                logger.info("number of finished entity tasks: {}, for detector {}", r, adTask.getDetectorId());
                // Set task as FAILED if no finished entity task; otherwise set as FINISHED
                ADTaskState hcDetectorTaskState = r == 0 ? ADTaskState.FAILED : ADTaskState.FINISHED;
                updateADHCDetectorTask(
                    detectorId,
                    taskId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            hcDetectorTaskState.name(),
                            TASK_PROGRESS_FIELD,
                            1.0,
                            EXECUTION_END_TIME_FIELD,
                            Instant.now().toEpochMilli()
                        ),
                    wrappedListener
                );
            }, e -> {
                logger.error("Failed to get finished entity tasks", e);
                updateADHCDetectorTask(
                    detectorId,
                    taskId,
                    ImmutableMap
                        .of(
                            STATE_FIELD,
                            ADTaskState.FAILED.name(),// set as FAILED if fail to get finished entity tasks.
                            TASK_PROGRESS_FIELD,
                            1.0,
                            ERROR_FIELD,
                            getErrorMessage(e),
                            EXECUTION_END_TIME_FIELD,
                            Instant.now().toEpochMilli()
                        ),
                    wrappedListener
                );
            }));
        } else {
            updateADHCDetectorTask(
                detectorId,
                taskId,
                ImmutableMap
                    .of(STATE_FIELD, state.name(), ERROR_FIELD, adTask.getError(), EXECUTION_END_TIME_FIELD, Instant.now().toEpochMilli()),
                wrappedListener
            );
        }

        listener.onResponse(new AnomalyDetectorJobResponse(taskId, 0, 0, 0, RestStatus.OK));
    }

    /**
     * Count entity tasks by state with detector level task id(parent task id).
     *
     * @param detectorTaskId detector level task id
     * @param taskStates task states
     * @param listener action listener
     */
    public void countEntityTasksByState(String detectorTaskId, List<ADTaskState> taskStates, ActionListener<Long> listener) {
        BoolQueryBuilder queryBuilder = new BoolQueryBuilder();
        queryBuilder.filter(new TermQueryBuilder(PARENT_TASK_ID_FIELD, detectorTaskId));
        if (taskStates != null && taskStates.size() > 0) {
            queryBuilder.filter(new TermsQueryBuilder(STATE_FIELD, taskStates.stream().map(s -> s.name()).collect(Collectors.toList())));
        }
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.size(0);
        sourceBuilder.trackTotalHits(true);
        SearchRequest request = new SearchRequest();
        request.source(sourceBuilder);
        request.indices(CommonName.DETECTION_STATE_INDEX);
        client.search(request, ActionListener.wrap(r -> {
            TotalHits totalHits = r.getHits().getTotalHits();
            listener.onResponse(totalHits.value);
        }, e -> listener.onFailure(e)));
    }

    /**
     * Update HC detector level task with default action listener. There might be
     * multiple entity tasks update detector task concurrently. So we will check
     * if detector task is updating or not to avoid and can only update if it's
     * not updating now, otherwise it may cause version conflict exception.
     *
     * @param detectorId detector id
     * @param taskId AD task id
     * @param updatedFields updated fields, key: filed name, value: new value
     */
    public void updateADHCDetectorTask(String detectorId, String taskId, Map<String, Object> updatedFields) {
        updateADHCDetectorTask(detectorId, taskId, updatedFields, ActionListener.wrap(response -> {
            if (response.status() == RestStatus.OK) {
                logger.debug("Updated AD task successfully: {}, taskId: {}", response.status(), taskId);
            } else {
                logger.error("Failed to update AD task {}, status: {}", taskId, response.status());
            }
        }, e -> { logger.error("Failed to update task1: " + taskId, e); }));
    }

    /**
     * Update HC detector level task. There might be multiple entity tasks update
     * detector task concurrently. So we will check if detector task is updating
     * or not to avoid and can only update if it's not updating now, otherwise it
     * may cause version conflict exception.
     *
     * @param detectorId detector id
     * @param taskId AD task id
     * @param updatedFields updated fields, key: filed name, value: new value
     * @param listener action listener
     */
    public void updateADHCDetectorTask(
        String detectorId,
        String taskId,
        Map<String, Object> updatedFields,
        ActionListener<UpdateResponse> listener
    ) {
        Boolean updating = adTaskCacheManager.isDetectorTaskUpdating(detectorId);
        if (updating == null) {
            logger.info("HC detector task updating flag removed", detectorId, taskId);
            return;
        }
        if (!updating) {
            if (updatedFields.containsKey(STATE_FIELD) && updatedFields.get(STATE_FIELD).equals(ADTaskState.FINISHED)) {
                logger.info("Update HC detector task to state to finished. detectorId:{}, taskId:{}", detectorId, taskId);
            }
            adTaskCacheManager.setDetectorTaskUpdating(detectorId, true);
            updateADTask(taskId, updatedFields, ActionListener.wrap(r -> {
                adTaskCacheManager.setDetectorTaskUpdating(detectorId, false);
                listener.onResponse(r);
            }, e -> {
                adTaskCacheManager.setDetectorTaskUpdating(detectorId, false);
                listener.onFailure(e);
            }));
        } else {
            logger.info("HC detector task is updating, detectorId:{}, taskId:{}", detectorId, taskId);
        }
    }

    /**
     * Run batch result action for entity task.
     * This method will be called by forwarding action.
     * @param adTask ad entity task
     * @param listener action listener
     */
    public void runBatchResultActionForEntity(ADTask adTask, ActionListener<AnomalyDetectorJobResponse> listener) {
        client.execute(ADBatchAnomalyResultAction.INSTANCE, new ADBatchAnomalyResultRequest(adTask), ActionListener.wrap(r -> {
            String remoteOrLocal = r.isRunTaskRemotely() ? "remote" : "local";
            logger
                .info(
                    "AD entity task {} of detector {} dispatched to {} node {}",
                    adTask.getTaskId(),
                    adTask.getDetectorId(),
                    remoteOrLocal,
                    r.getNodeId()
                );
            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                adTask.getDetectorId(),
                0,
                0,
                0,
                RestStatus.OK
            );
            listener.onResponse(anomalyDetectorJobResponse);
        }, e -> { listener.onFailure(e); }));
    }

    /**
     * Calculate historical analysis task progress of HC detector.
     * task_progress = finished_entity_count / total_entity_count
     * @param detectorId detector id
     * @return task progress
     */
    public float hcDetectorProgress(String detectorId) {
        int entityCount = adTaskCacheManager.getTopEntityCount(detectorId);
        int leftEntities = adTaskCacheManager.getPendingEntityCount(detectorId) + adTaskCacheManager.getRunningEntityCount(detectorId);
        return 1 - (float) leftEntities / entityCount;
    }
}
