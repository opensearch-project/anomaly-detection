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

package org.opensearch.ad.util;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.REQUEST_TIMEOUT;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.TaskOperationFailure;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksAction;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.ad.common.exception.InternalFailure;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskId;
import org.opensearch.tasks.TaskInfo;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.constant.CommonMessages;

public class ClientUtil {
    private volatile TimeValue requestTimeout;
    private Client client;
    private final Throttler throttler;
    private ThreadPool threadPool;

    @Inject
    public ClientUtil(Settings setting, Client client, Throttler throttler, ThreadPool threadPool) {
        this.requestTimeout = REQUEST_TIMEOUT.get(setting);
        this.client = client;
        this.throttler = throttler;
        this.threadPool = threadPool;
    }

    /**
     * Send a nonblocking request with a timeout and return response. Blocking is not allowed in a
     * transport call context. See BaseFuture.blockingAllowed
     * @param request request like index/search/get
     * @param LOG log
     * @param consumer functional interface to operate as a client request like client::get
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @return the response
     * @throws OpenSearchTimeoutException when we cannot get response within time.
     * @throws IllegalStateException when the waiting thread is interrupted
     */
    public <Request extends ActionRequest, Response extends ActionResponse> Optional<Response> timedRequest(
        Request request,
        Logger LOG,
        BiConsumer<Request, ActionListener<Response>> consumer
    ) {
        try {
            AtomicReference<Response> respReference = new AtomicReference<>();
            final CountDownLatch latch = new CountDownLatch(1);

            consumer
                .accept(
                    request,
                    new LatchedActionListener<Response>(
                        ActionListener
                            .wrap(
                                response -> { respReference.set(response); },
                                exception -> { LOG.error("Cannot get response for request {}, error: {}", request, exception); }
                            ),
                        latch
                    )
                );

            if (!latch.await(requestTimeout.getSeconds(), TimeUnit.SECONDS)) {
                throw new OpenSearchTimeoutException("Cannot get response within time limit: " + request.toString());
            }
            return Optional.ofNullable(respReference.get());
        } catch (InterruptedException e1) {
            LOG.error(CommonMessages.WAIT_ERR_MSG);
            throw new IllegalStateException(e1);
        }
    }

    /**
     * Send an asynchronous request and handle response with the provided listener.
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param request request body
     * @param consumer request method, functional interface to operate as a client request like client::get
     * @param listener needed to handle response
     */
    public <Request extends ActionRequest, Response extends ActionResponse> void asyncRequest(
        Request request,
        BiConsumer<Request, ActionListener<Response>> consumer,
        ActionListener<Response> listener
    ) {
        consumer
            .accept(
                request,
                ActionListener.wrap(response -> { listener.onResponse(response); }, exception -> { listener.onFailure(exception); })
            );
    }

    /**
     * Execute a transport action and handle response with the provided listener.
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param action transport action
     * @param request request body
     * @param listener needed to handle response
     */
    public <Request extends ActionRequest, Response extends ActionResponse> void execute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        client
            .execute(
                action,
                request,
                ActionListener.wrap(response -> { listener.onResponse(response); }, exception -> { listener.onFailure(exception); })
            );
    }

    /**
     * Send an synchronous request and handle response with the provided listener.
     *
     * @deprecated use asyncRequest with listener instead.
     *
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param request request body
     * @param function request method, functional interface to operate as a client request like client::get
     * @return the response
     */
    @Deprecated
    public <Request extends ActionRequest, Response extends ActionResponse> Response syncRequest(
        Request request,
        Function<Request, ActionFuture<Response>> function
    ) {
        return function.apply(request).actionGet(requestTimeout);
    }

    /**
     * Send a nonblocking request with a timeout and return response.
     * If there is already a query running on given detector, it will try to
     * cancel the query. Otherwise it will add this query to the negative cache
     * and then attach the AnomalyDetection specific header to the request.
     * Once the request complete, it will be removed from the negative cache.
     * @param <Request> ActionRequest
     * @param <Response> ActionResponse
     * @param request request like index/search/get
     * @param LOG log
     * @param consumer functional interface to operate as a client request like client::get
     * @param detector Anomaly Detector
     * @return the response
     * @throws InternalFailure when there is already a query running
     * @throws OpenSearchTimeoutException when we cannot get response within time.
     * @throws IllegalStateException when the waiting thread is interrupted
     */
    public <Request extends ActionRequest, Response extends ActionResponse> Optional<Response> throttledTimedRequest(
        Request request,
        Logger LOG,
        BiConsumer<Request, ActionListener<Response>> consumer,
        AnomalyDetector detector
    ) {

        try {
            String detectorId = detector.getDetectorId();
            if (!throttler.insertFilteredQuery(detectorId, request)) {
                LOG.info("There is one query running for detectorId: {}. Trying to cancel the long running query", detectorId);
                cancelRunningQuery(client, detectorId, LOG);
                throw new InternalFailure(detector.getDetectorId(), "There is already a query running on AnomalyDetector");
            }
            AtomicReference<Response> respReference = new AtomicReference<>();
            final CountDownLatch latch = new CountDownLatch(1);

            try (ThreadContext.StoredContext context = threadPool.getThreadContext().stashContext()) {
                assert context != null;
                threadPool.getThreadContext().putHeader(Task.X_OPAQUE_ID, ADCommonName.ANOMALY_DETECTOR + ":" + detectorId);
                consumer.accept(request, new LatchedActionListener<Response>(ActionListener.wrap(response -> {
                    // clear negative cache
                    throttler.clearFilteredQuery(detectorId);
                    respReference.set(response);
                }, exception -> {
                    // clear negative cache
                    throttler.clearFilteredQuery(detectorId);
                    LOG.error("Cannot get response for request {}, error: {}", request, exception);
                }), latch));
            } catch (Exception e) {
                LOG.error("Failed to process the request for detectorId: {}.", detectorId);
                throttler.clearFilteredQuery(detectorId);
                throw e;
            }

            if (!latch.await(requestTimeout.getSeconds(), TimeUnit.SECONDS)) {
                throw new OpenSearchTimeoutException("Cannot get response within time limit: " + request.toString());
            }
            return Optional.ofNullable(respReference.get());
        } catch (InterruptedException e1) {
            LOG.error(CommonMessages.WAIT_ERR_MSG);
            throw new IllegalStateException(e1);
        }
    }

    /**
     * Check if there is running query on given detector
     * @param detector Anomaly Detector
     * @return true if given detector has a running query else false
     */
    public boolean hasRunningQuery(AnomalyDetector detector) {
        return throttler.getFilteredQuery(detector.getDetectorId()).isPresent();
    }

    /**
     * Cancel long running query for given detectorId
     * @param client OpenSearch client
     * @param detectorId Anomaly Detector Id
     * @param LOG Logger
     */
    private void cancelRunningQuery(Client client, String detectorId, Logger LOG) {
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setActions("*search*");
        client
            .execute(
                ListTasksAction.INSTANCE,
                listTasksRequest,
                ActionListener.wrap(response -> { onListTaskResponse(response, detectorId, LOG); }, exception -> {
                    LOG.error("List Tasks failed.", exception);
                    throw new InternalFailure(detectorId, "Failed to list current tasks", exception);
                })
            );
    }

    /**
     * Helper function to handle ListTasksResponse
     * @param listTasksResponse ListTasksResponse
     * @param detectorId Anomaly Detector Id
     * @param LOG Logger
     */
    private void onListTaskResponse(ListTasksResponse listTasksResponse, String detectorId, Logger LOG) {
        List<TaskInfo> tasks = listTasksResponse.getTasks();
        TaskId matchedParentTaskId = null;
        TaskId matchedSingleTaskId = null;
        for (TaskInfo task : tasks) {
            if (!task.getHeaders().isEmpty()
                && task.getHeaders().get(Task.X_OPAQUE_ID).equals(ADCommonName.ANOMALY_DETECTOR + ":" + detectorId)) {
                if (!task.getParentTaskId().equals(TaskId.EMPTY_TASK_ID)) {
                    // we found the parent task, don't need to check more
                    matchedParentTaskId = task.getParentTaskId();
                    break;
                } else {
                    // we found one task, keep checking other tasks
                    matchedSingleTaskId = task.getTaskId();
                }
            }
        }
        // case 1: given detectorId is not in current task list
        if (matchedParentTaskId == null && matchedSingleTaskId == null) {
            // log and then clear negative cache
            LOG.info("Couldn't find task for detectorId: {}. Clean this entry from Throttler", detectorId);
            throttler.clearFilteredQuery(detectorId);
            return;
        }
        // case 2: we can find the task for given detectorId
        CancelTasksRequest cancelTaskRequest = new CancelTasksRequest();
        if (matchedParentTaskId != null) {
            cancelTaskRequest.setParentTaskId(matchedParentTaskId);
            LOG.info("Start to cancel task for parentTaskId: {}", matchedParentTaskId.toString());
        } else {
            cancelTaskRequest.setTaskId(matchedSingleTaskId);
            LOG.info("Start to cancel task for taskId: {}", matchedSingleTaskId.toString());
        }

        client
            .execute(
                CancelTasksAction.INSTANCE,
                cancelTaskRequest,
                ActionListener.wrap(response -> { onCancelTaskResponse(response, detectorId, LOG); }, exception -> {
                    LOG.error("Failed to cancel task for detectorId: " + detectorId, exception);
                    throw new InternalFailure(detectorId, "Failed to cancel current tasks", exception);
                })
            );
    }

    /**
     * Helper function to handle CancelTasksResponse
     * @param cancelTasksResponse CancelTasksResponse
     * @param detectorId Anomaly Detector Id
     * @param LOG Logger
     */
    private void onCancelTaskResponse(CancelTasksResponse cancelTasksResponse, String detectorId, Logger LOG) {
        // todo: adding retry mechanism
        List<OpenSearchException> nodeFailures = cancelTasksResponse.getNodeFailures();
        List<TaskOperationFailure> taskFailures = cancelTasksResponse.getTaskFailures();
        if (nodeFailures.isEmpty() && taskFailures.isEmpty()) {
            LOG.info("Cancelling query for detectorId: {} succeeds. Clear entry from Throttler", detectorId);
            throttler.clearFilteredQuery(detectorId);
            return;
        }
        LOG.error("Failed to cancel task for detectorId: " + detectorId);
        throw new InternalFailure(detectorId, "Failed to cancel current tasks due to node or task failures");
    }
}
