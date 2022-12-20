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

package org.opensearch.ad.rest.handler;

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.ad.util.ExceptionUtil.getShardsFailure;
import static org.opensearch.ad.util.RestHandlerUtils.createXContentParserFromRegistry;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.ExecuteADResultResponseRecorder;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.AnomalyDetectorJobResponse;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultRequest;
import org.opensearch.ad.transport.StopDetectorAction;
import org.opensearch.ad.transport.StopDetectorRequest;
import org.opensearch.ad.transport.StopDetectorResponse;
import org.opensearch.ad.util.RestHandlerUtils;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.rest.RestStatus;
import org.opensearch.transport.TransportService;

import com.google.common.base.Throwables;

/**
 * Anomaly detector job REST action handler to process POST/PUT request.
 */
public class IndexAnomalyDetectorJobActionHandler {

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final String detectorId;
    private final Long seqNo;
    private final Long primaryTerm;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final TransportService transportService;
    private final ADTaskManager adTaskManager;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectorJobActionHandler.class);
    private final TimeValue requestTimeout;
    private final ExecuteADResultResponseRecorder recorder;

    /**
     * Constructor function.
     *
     * @param client                  ES node client that executes actions on the local node
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param requestTimeout          request time out configuration
     * @param xContentRegistry        Registry which is used for XContentParser
     * @param transportService        transport service
     * @param adTaskManager           AD task manager
     * @param recorder                Utility to record AnomalyResultAction execution result
     */
    public IndexAnomalyDetectorJobActionHandler(
        Client client,
        AnomalyDetectionIndices anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        TimeValue requestTimeout,
        NamedXContentRegistry xContentRegistry,
        TransportService transportService,
        ADTaskManager adTaskManager,
        ExecuteADResultResponseRecorder recorder
    ) {
        this.client = client;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.detectorId = detectorId;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.requestTimeout = requestTimeout;
        this.xContentRegistry = xContentRegistry;
        this.transportService = transportService;
        this.adTaskManager = adTaskManager;
        this.recorder = recorder;
    }

    /**
     * Start anomaly detector job.
     * 1. If job doesn't exist, create new job.
     * 2. If job exists: a). if job enabled, return error message; b). if job disabled, enable job.
     * @param detector anomaly detector
     * @param listener Listener to send responses
     */
    public void startAnomalyDetectorJob(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) {
        // this start listener is created & injected throughout the job handler so that whenever the job response is received,
        // there's the extra step of trying to index results and update detector state with a 60s delay.
        ActionListener<AnomalyDetectorJobResponse> startListener = ActionListener.wrap(r -> {
            try {
                Instant executionEndTime = Instant.now();
                IntervalTimeConfiguration schedule = (IntervalTimeConfiguration) detector.getDetectionInterval();
                Instant executionStartTime = executionEndTime.minus(schedule.getInterval(), schedule.getUnit());
                AnomalyResultRequest getRequest = new AnomalyResultRequest(
                    detector.getDetectorId(),
                    executionStartTime.toEpochMilli(),
                    executionEndTime.toEpochMilli()
                );
                client
                    .execute(
                        AnomalyResultAction.INSTANCE,
                        getRequest,
                        ActionListener
                            .wrap(
                                response -> recorder.indexAnomalyResult(executionStartTime, executionEndTime, response, detector),
                                exception -> {

                                    recorder
                                        .indexAnomalyResultException(
                                            executionStartTime,
                                            executionEndTime,
                                            Throwables.getStackTraceAsString(exception),
                                            null,
                                            detector
                                        );
                                }
                            )
                    );
            } catch (Exception ex) {
                listener.onFailure(ex);
                return;
            }
            listener.onResponse(r);

        }, listener::onFailure);
        if (!anomalyDetectionIndices.doesAnomalyDetectorJobIndexExist()) {
            anomalyDetectionIndices.initAnomalyDetectorJobIndex(ActionListener.wrap(response -> {
                if (response.isAcknowledged()) {
                    logger.info("Created {} with mappings.", ANOMALY_DETECTORS_INDEX);
                    createJob(detector, startListener);
                } else {
                    logger.warn("Created {} with mappings call not acknowledged.", ANOMALY_DETECTORS_INDEX);
                    startListener
                        .onFailure(
                            new OpenSearchStatusException(
                                "Created " + ANOMALY_DETECTORS_INDEX + " with mappings call not acknowledged.",
                                RestStatus.INTERNAL_SERVER_ERROR
                            )
                        );
                }
            }, exception -> startListener.onFailure(exception)));
        } else {
            createJob(detector, startListener);
        }
    }

    private void createJob(AnomalyDetector detector, ActionListener<AnomalyDetectorJobResponse> listener) {
        try {
            IntervalTimeConfiguration interval = (IntervalTimeConfiguration) detector.getDetectionInterval();
            Schedule schedule = new IntervalSchedule(Instant.now(), (int) interval.getInterval(), interval.getUnit());
            Duration duration = Duration.of(interval.getInterval(), interval.getUnit());

            AnomalyDetectorJob job = new AnomalyDetectorJob(
                detector.getDetectorId(),
                schedule,
                detector.getWindowDelay(),
                true,
                Instant.now(),
                null,
                Instant.now(),
                duration.getSeconds(),
                detector.getUser(),
                detector.getResultIndex()
            );

            getAnomalyDetectorJobForWrite(detector, job, listener);
        } catch (Exception e) {
            String message = "Failed to parse anomaly detector job " + detectorId;
            logger.error(message, e);
            listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    private void getAnomalyDetectorJobForWrite(
        AnomalyDetector detector,
        AnomalyDetectorJob job,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

        client
            .get(
                getRequest,
                ActionListener
                    .wrap(
                        response -> onGetAnomalyDetectorJobForWrite(response, detector, job, listener),
                        exception -> listener.onFailure(exception)
                    )
            );
    }

    private void onGetAnomalyDetectorJobForWrite(
        GetResponse response,
        AnomalyDetector detector,
        AnomalyDetectorJob job,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) throws IOException {
        if (response.isExists()) {
            try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                AnomalyDetectorJob currentAdJob = AnomalyDetectorJob.parse(parser);
                if (currentAdJob.isEnabled()) {
                    listener
                        .onFailure(new OpenSearchStatusException("Anomaly detector job is already running: " + detectorId, RestStatus.OK));
                    return;
                } else {
                    AnomalyDetectorJob newJob = new AnomalyDetectorJob(
                        job.getName(),
                        job.getSchedule(),
                        job.getWindowDelay(),
                        job.isEnabled(),
                        Instant.now(),
                        currentAdJob.getDisabledTime(),
                        Instant.now(),
                        job.getLockDurationSeconds(),
                        job.getUser(),
                        job.getResultIndex()
                    );
                    // Get latest realtime task and check its state before index job. Will reset running realtime task
                    // as STOPPED first if job disabled, then start new job and create new realtime task.
                    adTaskManager
                        .startDetector(
                            detector,
                            null,
                            job.getUser(),
                            transportService,
                            ActionListener
                                .wrap(
                                    r -> { indexAnomalyDetectorJob(newJob, null, listener); },
                                    e -> {
                                        // Have logged error message in ADTaskManager#startDetector
                                        listener.onFailure(e);
                                    }
                                )
                        );
                }
            } catch (IOException e) {
                String message = "Failed to parse anomaly detector job " + job.getName();
                logger.error(message, e);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        } else {
            adTaskManager
                .startDetector(
                    detector,
                    null,
                    job.getUser(),
                    transportService,
                    ActionListener.wrap(r -> { indexAnomalyDetectorJob(job, null, listener); }, e -> listener.onFailure(e))
                );
        }
    }

    private void indexAnomalyDetectorJob(
        AnomalyDetectorJob job,
        AnomalyDetectorFunction function,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) throws IOException {
        IndexRequest indexRequest = new IndexRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(job.toXContent(XContentFactory.jsonBuilder(), RestHandlerUtils.XCONTENT_WITH_TYPE))
            .setIfSeqNo(seqNo)
            .setIfPrimaryTerm(primaryTerm)
            .timeout(requestTimeout)
            .id(detectorId);
        client
            .index(
                indexRequest,
                ActionListener
                    .wrap(
                        response -> onIndexAnomalyDetectorJobResponse(response, function, listener),
                        exception -> listener.onFailure(exception)
                    )
            );
    }

    private void onIndexAnomalyDetectorJobResponse(
        IndexResponse response,
        AnomalyDetectorFunction function,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        if (response == null || (response.getResult() != CREATED && response.getResult() != UPDATED)) {
            String errorMsg = getShardsFailure(response);
            listener.onFailure(new OpenSearchStatusException(errorMsg, response.status()));
            return;
        }
        if (function != null) {
            function.execute();
        } else {
            AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                response.getId(),
                response.getVersion(),
                response.getSeqNo(),
                response.getPrimaryTerm(),
                RestStatus.OK
            );
            listener.onResponse(anomalyDetectorJobResponse);
        }
    }

    /**
     * Stop anomaly detector job.
     * 1.If job not exists, return error message
     * 2.If job exists: a).if job state is disabled, return error message; b).if job state is enabled, disable job.
     *
     * @param detectorId detector identifier
     * @param listener Listener to send responses
     */
    public void stopAnomalyDetectorJob(String detectorId, ActionListener<AnomalyDetectorJobResponse> listener) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

        client.get(getRequest, ActionListener.wrap(response -> {
            if (response.isExists()) {
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                    if (!job.isEnabled()) {
                        adTaskManager.stopLatestRealtimeTask(detectorId, ADTaskState.STOPPED, null, transportService, listener);
                    } else {
                        AnomalyDetectorJob newJob = new AnomalyDetectorJob(
                            job.getName(),
                            job.getSchedule(),
                            job.getWindowDelay(),
                            false,
                            job.getEnabledTime(),
                            Instant.now(),
                            Instant.now(),
                            job.getLockDurationSeconds(),
                            job.getUser(),
                            job.getResultIndex()
                        );
                        indexAnomalyDetectorJob(
                            newJob,
                            () -> client
                                .execute(
                                    StopDetectorAction.INSTANCE,
                                    new StopDetectorRequest(detectorId),
                                    stopAdDetectorListener(detectorId, listener)
                                ),
                            listener
                        );
                    }
                } catch (IOException e) {
                    String message = "Failed to parse anomaly detector job " + detectorId;
                    logger.error(message, e);
                    listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
                }
            } else {
                listener.onFailure(new OpenSearchStatusException("Anomaly detector job not exist: " + detectorId, RestStatus.BAD_REQUEST));
            }
        }, exception -> listener.onFailure(exception)));
    }

    private ActionListener<StopDetectorResponse> stopAdDetectorListener(
        String detectorId,
        ActionListener<AnomalyDetectorJobResponse> listener
    ) {
        return new ActionListener<StopDetectorResponse>() {
            @Override
            public void onResponse(StopDetectorResponse stopDetectorResponse) {
                if (stopDetectorResponse.success()) {
                    logger.info("AD model deleted successfully for detector {}", detectorId);
                    // StopDetectorTransportAction will send out DeleteModelAction which will clear all realtime cache.
                    // Pass null transport service to method "stopLatestRealtimeTask" to not re-clear coordinating node cache.
                    adTaskManager.stopLatestRealtimeTask(detectorId, ADTaskState.STOPPED, null, null, listener);
                } else {
                    logger.error("Failed to delete AD model for detector {}", detectorId);
                    // If failed to clear all realtime cache, will try to re-clear coordinating node cache.
                    adTaskManager
                        .stopLatestRealtimeTask(
                            detectorId,
                            ADTaskState.FAILED,
                            new OpenSearchStatusException("Failed to delete AD model", RestStatus.INTERNAL_SERVER_ERROR),
                            transportService,
                            listener
                        );
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to delete AD model for detector " + detectorId, e);
                // If failed to clear all realtime cache, will try to re-clear coordinating node cache.
                adTaskManager
                    .stopLatestRealtimeTask(
                        detectorId,
                        ADTaskState.FAILED,
                        new OpenSearchStatusException("Failed to execute stop detector action", RestStatus.INTERNAL_SERVER_ERROR),
                        transportService,
                        listener
                    );
            }
        };
    }

}
