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

import static org.opensearch.ad.constant.ADCommonMessages.FAIL_TO_DELETE_DETECTOR;
import static org.opensearch.ad.model.ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.ParseUtils.getUserContext;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

public class DeleteAnomalyDetectorTransportAction extends HandledTransportAction<DeleteAnomalyDetectorRequest, DeleteResponse> {

    private static final Logger LOG = LogManager.getLogger(DeleteAnomalyDetectorTransportAction.class);
    private final Client client;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private NamedXContentRegistry xContentRegistry;
    private final ADTaskManager adTaskManager;
    private volatile Boolean filterByEnabled;

    @Inject
    public DeleteAnomalyDetectorTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        ADTaskManager adTaskManager
    ) {
        super(DeleteAnomalyDetectorAction.NAME, transportService, actionFilters, DeleteAnomalyDetectorRequest::new);
        this.transportService = transportService;
        this.client = client;
        this.clusterService = clusterService;
        this.xContentRegistry = xContentRegistry;
        this.adTaskManager = adTaskManager;
        filterByEnabled = AnomalyDetectorSettings.AD_FILTER_BY_BACKEND_ROLES.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(AD_FILTER_BY_BACKEND_ROLES, it -> filterByEnabled = it);
    }

    @Override
    protected void doExecute(Task task, DeleteAnomalyDetectorRequest request, ActionListener<DeleteResponse> actionListener) {
        String detectorId = request.getDetectorID();
        LOG.info("Delete anomaly detector job {}", detectorId);
        User user = getUserContext(client);
        ActionListener<DeleteResponse> listener = wrapRestActionListener(actionListener, FAIL_TO_DELETE_DETECTOR);
        // By the time request reaches here, the user permissions are validated by Security plugin.
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            resolveUserAndExecute(
                user,
                detectorId,
                filterByEnabled,
                listener,
                (anomalyDetector) -> adTaskManager.getDetector(detectorId, detector -> {
                    if (!detector.isPresent()) {
                        // In a mixed cluster, if delete detector request routes to node running AD1.0, then it will
                        // not delete detector tasks. User can re-delete these deleted detector after cluster upgraded,
                        // in that case, the detector is not present.
                        LOG.info("Can't find anomaly detector {}", detectorId);
                        adTaskManager.deleteADTasks(detectorId, () -> deleteAnomalyDetectorJobDoc(detectorId, listener), listener);
                        return;
                    }
                    // Check if there is realtime job or historical analysis task running. If none of these running, we
                    // can delete the detector.
                    getDetectorJob(detectorId, listener, () -> {
                        adTaskManager.getAndExecuteOnLatestDetectorLevelTask(detectorId, HISTORICAL_DETECTOR_TASK_TYPES, adTask -> {
                            if (adTask.isPresent() && !adTask.get().isDone()) {
                                listener.onFailure(new OpenSearchStatusException("Detector is running", RestStatus.INTERNAL_SERVER_ERROR));
                            } else {
                                adTaskManager.deleteADTasks(detectorId, () -> deleteAnomalyDetectorJobDoc(detectorId, listener), listener);
                            }
                        }, transportService, true, listener);
                    });
                }, listener),
                client,
                clusterService,
                xContentRegistry,
                AnomalyDetector.class
            );
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    private void deleteAnomalyDetectorJobDoc(String detectorId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete anomaly detector job {}", detectorId);
        DeleteRequest deleteRequest = new DeleteRequest(CommonName.JOB_INDEX, detectorId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(deleteRequest, ActionListener.wrap(response -> {
            if (response.getResult() == DocWriteResponse.Result.DELETED || response.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                deleteDetectorStateDoc(detectorId, listener);
            } else {
                String message = "Fail to delete anomaly detector job " + detectorId;
                LOG.error(message);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, exception -> {
            LOG.error("Failed to delete AD job for " + detectorId, exception);
            if (exception instanceof IndexNotFoundException) {
                deleteDetectorStateDoc(detectorId, listener);
            } else {
                LOG.error("Failed to delete anomaly detector job", exception);
                listener.onFailure(exception);
            }
        }));
    }

    private void deleteDetectorStateDoc(String detectorId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete detector info {}", detectorId);
        DeleteRequest deleteRequest = new DeleteRequest(ADCommonName.DETECTION_STATE_INDEX, detectorId);
        client.delete(deleteRequest, ActionListener.wrap(response -> {
            // whether deleted state doc or not, continue as state doc may not exist
            deleteAnomalyDetectorDoc(detectorId, listener);
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                deleteAnomalyDetectorDoc(detectorId, listener);
            } else {
                LOG.error("Failed to delete detector state", exception);
                listener.onFailure(exception);
            }
        }));
    }

    private void deleteAnomalyDetectorDoc(String detectorId, ActionListener<DeleteResponse> listener) {
        LOG.info("Delete anomaly detector {}", detectorId);
        DeleteRequest deleteRequest = new DeleteRequest(CommonName.CONFIG_INDEX, detectorId)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        client.delete(deleteRequest, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                listener.onResponse(deleteResponse);
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private void getDetectorJob(String detectorId, ActionListener<DeleteResponse> listener, ExecutorFunction function) {
        if (clusterService.state().metadata().indices().containsKey(CommonName.JOB_INDEX)) {
            GetRequest request = new GetRequest(CommonName.JOB_INDEX).id(detectorId);
            client.get(request, ActionListener.wrap(response -> onGetAdJobResponseForWrite(response, listener, function), exception -> {
                LOG.error("Fail to get anomaly detector job: " + detectorId, exception);
                listener.onFailure(exception);
            }));
        } else {
            function.execute();
        }
    }

    private void onGetAdJobResponseForWrite(GetResponse response, ActionListener<DeleteResponse> listener, ExecutorFunction function)
        throws IOException {
        if (response.isExists()) {
            String adJobId = response.getId();
            if (adJobId != null) {
                // check if AD job is running on the detector, if yes, we can't delete the detector
                try (
                    XContentParser parser = RestHandlerUtils
                        .createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    Job adJob = Job.parse(parser);
                    if (adJob.isEnabled()) {
                        listener.onFailure(new OpenSearchStatusException("Detector job is running: " + adJobId, RestStatus.BAD_REQUEST));
                        return;
                    }
                } catch (IOException e) {
                    String message = "Failed to parse anomaly detector job " + adJobId;
                    LOG.error(message, e);
                }
            }
        }
        function.execute();
    }
}
