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

package org.opensearch.ad.rest.handler;

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static org.opensearch.ad.util.ExceptionUtil.getShardsFailure;
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
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.transport.AnomalyDetectorJobResponse;
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

/**
 * Anomaly detector job REST action handler to process POST/PUT request.
 */
public class IndexAnomalyDetectorJobActionHandler {

    private final AnomalyDetectionIndices anomalyDetectionIndices;
    private final String detectorId;
    private final Long seqNo;
    private final Long primaryTerm;
    private final Client client;
    private final ActionListener<AnomalyDetectorJobResponse> listener;
    private final NamedXContentRegistry xContentRegistry;

    private final Logger logger = LogManager.getLogger(IndexAnomalyDetectorJobActionHandler.class);
    private final TimeValue requestTimeout;

    /**
     * Constructor function.
     *
     * @param client                  ES node client that executes actions on the local node
     * @param listener                Listener to send responses
     * @param anomalyDetectionIndices anomaly detector index manager
     * @param detectorId              detector identifier
     * @param seqNo                   sequence number of last modification
     * @param primaryTerm             primary term of last modification
     * @param requestTimeout          request time out configuration
     * @param xContentRegistry        Registry which is used for XContentParser
     */
    public IndexAnomalyDetectorJobActionHandler(
        Client client,
        ActionListener<AnomalyDetectorJobResponse> listener,
        AnomalyDetectionIndices anomalyDetectionIndices,
        String detectorId,
        Long seqNo,
        Long primaryTerm,
        TimeValue requestTimeout,
        NamedXContentRegistry xContentRegistry
    ) {
        this.client = client;
        this.listener = listener;
        this.anomalyDetectionIndices = anomalyDetectionIndices;
        this.detectorId = detectorId;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.requestTimeout = requestTimeout;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Start anomaly detector job.
     * 1. If job doesn't exist, create new job.
     * 2. If job exists: a). if job enabled, return error message; b). if job disabled, enable job.
     * @param detector anomaly detector
     */
    public void startAnomalyDetectorJob(AnomalyDetector detector) {
        if (!anomalyDetectionIndices.doesAnomalyDetectorJobIndexExist()) {
            anomalyDetectionIndices.initAnomalyDetectorJobIndex(ActionListener.wrap(response -> {
                if (response.isAcknowledged()) {
                    logger.info("Created {} with mappings.", ANOMALY_DETECTORS_INDEX);
                    createJob(detector);
                } else {
                    logger.warn("Created {} with mappings call not acknowledged.", ANOMALY_DETECTORS_INDEX);
                    listener
                        .onFailure(
                            new OpenSearchStatusException(
                                "Created " + ANOMALY_DETECTORS_INDEX + " with mappings call not acknowledged.",
                                RestStatus.INTERNAL_SERVER_ERROR
                            )
                        );
                }
            }, exception -> listener.onFailure(exception)));
        } else {
            createJob(detector);
        }
    }

    private void createJob(AnomalyDetector detector) {
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
                detector.getUser()
            );

            getAnomalyDetectorJobForWrite(job);
        } catch (Exception e) {
            String message = "Failed to parse anomaly detector job " + detectorId;
            logger.error(message, e);
            listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    private void getAnomalyDetectorJobForWrite(AnomalyDetectorJob job) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

        client
            .get(
                getRequest,
                ActionListener.wrap(response -> onGetAnomalyDetectorJobForWrite(response, job), exception -> listener.onFailure(exception))
            );
    }

    private void onGetAnomalyDetectorJobForWrite(GetResponse response, AnomalyDetectorJob job) throws IOException {
        if (response.isExists()) {
            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
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
                        job.getUser()
                    );
                    indexAnomalyDetectorJob(newJob, null);
                }
            } catch (IOException e) {
                String message = "Failed to parse anomaly detector job " + job.getName();
                logger.error(message, e);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        } else {
            indexAnomalyDetectorJob(job, null);
        }
    }

    private void indexAnomalyDetectorJob(AnomalyDetectorJob job, AnomalyDetectorFunction function) throws IOException {
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
                    .wrap(response -> onIndexAnomalyDetectorJobResponse(response, function), exception -> listener.onFailure(exception))
            );
    }

    private void onIndexAnomalyDetectorJobResponse(IndexResponse response, AnomalyDetectorFunction function) throws IOException {
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
     */
    public void stopAnomalyDetectorJob(String detectorId) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

        client.get(getRequest, ActionListener.wrap(response -> {
            if (response.isExists()) {
                try (
                    XContentParser parser = RestHandlerUtils
                        .createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                    if (!job.isEnabled()) {
                        listener
                            .onFailure(
                                new OpenSearchStatusException("Anomaly detector job is already stopped: " + detectorId, RestStatus.OK)
                            );
                        return;
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
                            job.getUser()
                        );
                        indexAnomalyDetectorJob(
                            newJob,
                            () -> client
                                .execute(
                                    StopDetectorAction.INSTANCE,
                                    new StopDetectorRequest(detectorId),
                                    stopAdDetectorListener(detectorId)
                                )
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

    private ActionListener<StopDetectorResponse> stopAdDetectorListener(String detectorId) {
        return new ActionListener<StopDetectorResponse>() {
            @Override
            public void onResponse(StopDetectorResponse stopDetectorResponse) {
                if (stopDetectorResponse.success()) {
                    logger.info("AD model deleted successfully for detector {}", detectorId);
                    AnomalyDetectorJobResponse anomalyDetectorJobResponse = new AnomalyDetectorJobResponse(
                        detectorId,
                        0,
                        0,
                        0,
                        RestStatus.OK
                    );
                    listener.onResponse(anomalyDetectorJobResponse);
                } else {
                    logger.error("Failed to delete AD model for detector {}", detectorId);
                    listener.onFailure(new OpenSearchStatusException("Failed to delete AD model", RestStatus.INTERNAL_SERVER_ERROR));
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to delete AD model for detector " + detectorId, e);
                listener
                    .onFailure(new OpenSearchStatusException("Failed to execute stop detector action", RestStatus.INTERNAL_SERVER_ERROR));
            }
        };
    }

}
