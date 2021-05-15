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

package org.opensearch.ad;

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.ad.AnomalyDetectorPlugin.AD_THREAD_POOL_NAME;
import static org.opensearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.InternalFailure;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.model.FeatureData;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultRequest;
import org.opensearch.ad.transport.AnomalyResultResponse;
import org.opensearch.ad.transport.AnomalyResultTransportAction;
import org.opensearch.ad.transport.handler.AnomalyIndexHandler;
import org.opensearch.ad.transport.handler.DetectionStateHandler;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.commons.InjectSecurity;
import com.amazon.opendistroforelasticsearch.commons.authuser.User;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.LockModel;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.schedule.IntervalSchedule;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.utils.LockService;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

/**
 * JobScheduler will call AD job runner to get anomaly result periodically
 */
public class AnomalyDetectorJobRunner implements ScheduledJobRunner {
    private static final Logger log = LogManager.getLogger(AnomalyDetectorJobRunner.class);
    private static AnomalyDetectorJobRunner INSTANCE;
    private Settings settings;
    private int maxRetryForEndRunException;
    private Client client;
    private ClientUtil clientUtil;
    private ThreadPool threadPool;
    private AnomalyIndexHandler<AnomalyResult> anomalyResultHandler;
    private ConcurrentHashMap<String, Integer> detectorEndRunExceptionCount;
    private DetectionStateHandler detectionStateHandler;
    private AnomalyDetectionIndices indexUtil;

    public static AnomalyDetectorJobRunner getJobRunnerInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (AnomalyDetectorJobRunner.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new AnomalyDetectorJobRunner();
            return INSTANCE;
        }
    }

    private AnomalyDetectorJobRunner() {
        // Singleton class, use getJobRunnerInstance method instead of constructor
        this.detectorEndRunExceptionCount = new ConcurrentHashMap<>();
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public void setClientUtil(ClientUtil clientUtil) {
        this.clientUtil = clientUtil;
    }

    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public void setAnomalyResultHandler(AnomalyIndexHandler<AnomalyResult> anomalyResultHandler) {
        this.anomalyResultHandler = anomalyResultHandler;
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
        this.maxRetryForEndRunException = AnomalyDetectorSettings.MAX_RETRY_FOR_END_RUN_EXCEPTION.get(settings);
    }

    public void setDetectionStateHandler(DetectionStateHandler detectionStateHandler) {
        this.detectionStateHandler = detectionStateHandler;
    }

    public void setIndexUtil(AnomalyDetectionIndices indexUtil) {
        this.indexUtil = indexUtil;
    }

    @Override
    public void runJob(ScheduledJobParameter jobParameter, JobExecutionContext context) {
        String detectorId = jobParameter.getName();
        log.info("Start to run AD job {}", detectorId);
        if (!(jobParameter instanceof AnomalyDetectorJob)) {
            throw new IllegalArgumentException(
                "Job parameter is not instance of AnomalyDetectorJob, type: " + jobParameter.getClass().getCanonicalName()
            );
        }

        Instant executionStartTime = Instant.now();
        IntervalSchedule schedule = (IntervalSchedule) jobParameter.getSchedule();
        Instant detectionStartTime = executionStartTime.minus(schedule.getInterval(), schedule.getUnit());

        final LockService lockService = context.getLockService();

        Runnable runnable = () -> {
            if (jobParameter.getLockDurationSeconds() != null) {
                lockService
                    .acquireLock(
                        jobParameter,
                        context,
                        ActionListener
                            .wrap(lock -> runAdJob(jobParameter, lockService, lock, detectionStartTime, executionStartTime), exception -> {
                                indexAnomalyResultException(
                                    jobParameter,
                                    lockService,
                                    null,
                                    detectionStartTime,
                                    executionStartTime,
                                    exception,
                                    false
                                );
                                throw new IllegalStateException("Failed to acquire lock for AD job: " + detectorId);
                            })
                    );
            } else {
                log.warn("Can't get lock for AD job: " + detectorId);
            }
        };

        ExecutorService executor = threadPool.executor(AD_THREAD_POOL_NAME);
        executor.submit(runnable);
    }

    /**
     * Get anomaly result, index result or handle exception if failed.
     *
     * @param jobParameter scheduled job parameter
     * @param lockService lock service
     * @param lock lock to run job
     * @param detectionStartTime detection start time
     * @param executionStartTime detection end time
     */
    protected void runAdJob(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime
    ) {
        String detectorId = jobParameter.getName();
        if (lock == null) {
            indexAnomalyResultException(
                jobParameter,
                lockService,
                lock,
                detectionStartTime,
                executionStartTime,
                "Can't run AD job due to null lock",
                false
            );
            return;
        }
        indexUtil.updateMappingIfNecessary();
        /*
         * We need to handle 3 cases:
         * 1. Detectors created by older versions and never updated. These detectors wont have User details in the
         * detector object. `detector.user` will be null. Insert `all_access, AmazonES_all_access` role.
         * 2. Detectors are created when security plugin is disabled, these will have empty User object.
         * (`detector.user.name`, `detector.user.roles` are empty )
         * 3. Detectors are created when security plugin is enabled, these will have an User object.
         * This will inject user role and check if the user role has permissions to call the execute
         * Anomaly Result API.
         */
        String user;
        List<String> roles;
        if (((AnomalyDetectorJob) jobParameter).getUser() == null) {
            user = "";
            roles = settings.getAsList("", ImmutableList.of("all_access", "AmazonES_all_access"));
        } else {
            user = ((AnomalyDetectorJob) jobParameter).getUser().getName();
            roles = ((AnomalyDetectorJob) jobParameter).getUser().getRoles();
        }

        try (InjectSecurity injectSecurity = new InjectSecurity(detectorId, settings, client.threadPool().getThreadContext())) {
            // Injecting user role to verify if the user has permissions for our API.
            injectSecurity.inject(user, roles);

            AnomalyResultRequest request = new AnomalyResultRequest(
                detectorId,
                detectionStartTime.toEpochMilli(),
                executionStartTime.toEpochMilli()
            );
            client
                .execute(
                    AnomalyResultAction.INSTANCE,
                    request,
                    ActionListener
                        .wrap(
                            response -> {
                                indexAnomalyResult(jobParameter, lockService, lock, detectionStartTime, executionStartTime, response);
                            },
                            exception -> {
                                handleAdException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, exception);
                            }
                        )
                );
        } catch (Exception e) {
            indexAnomalyResultException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, e, true);
            log.error("Failed to execute AD job " + detectorId, e);
        }
    }

    /**
     * Handle exception from anomaly result action.
     *
     * 1. If exception is {@link EndRunException}
     *   a). if isEndNow == true, stop AD job and store exception in anomaly result
     *   b). if isEndNow == false, record count of {@link EndRunException} for this
     *       detector. If count of {@link EndRunException} exceeds upper limit, will
     *       stop AD job and store exception in anomaly result; otherwise, just
     *       store exception in anomaly result, not stop AD job for the detector.
     *
     * 2. If exception is not {@link EndRunException}, decrease count of
     *    {@link EndRunException} for the detector and index eception in Anomaly
     *    result. If exception is {@link InternalFailure}, will not log exception
     *    stack trace as already logged in {@link AnomalyResultTransportAction}.
     *
     * TODO: Handle finer granularity exception such as some exception may be
     *       transient and retry in current job may succeed. Currently, we don't
     *       know which exception is transient and retryable in
     *       {@link AnomalyResultTransportAction}. So we don't add backoff retry
     *       now to avoid bring extra load to cluster, expecially the code start
     *       process is relatively heavy by sending out 24 queries, initializing
     *       models, and saving checkpoints.
     *       Sometimes missing anomaly and notification is not acceptable. For example,
     *       current detection interval is 1hour, and there should be anomaly in
     *       current interval, some transient exception may fail current AD job,
     *       so no anomaly found and user never know it. Then we start next AD job,
     *       maybe there is no anomaly in next 1hour, user will never know something
     *       wrong happened. In one word, this is some tradeoff between protecting
     *       our performance, user experience and what we can do currently.
     *
     * @param jobParameter scheduled job parameter
     * @param lockService lock service
     * @param lock lock to run job
     * @param detectionStartTime detection start time
     * @param executionStartTime detection end time
     * @param exception exception
     */
    protected void handleAdException(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        Exception exception
    ) {
        String detectorId = jobParameter.getName();
        if (exception instanceof EndRunException) {
            log.error("EndRunException happened when executing anomaly result action for " + detectorId, exception);

            if (((EndRunException) exception).isEndNow()) {
                // Stop AD job if EndRunException shows we should end job now.
                log.info("JobRunner will stop AD job due to EndRunException for {}", detectorId);
                stopAdJobForEndRunException(
                    jobParameter,
                    lockService,
                    lock,
                    detectionStartTime,
                    executionStartTime,
                    (EndRunException) exception
                );
            } else {
                detectorEndRunExceptionCount.compute(detectorId, (k, v) -> {
                    if (v == null) {
                        return 1;
                    } else {
                        return v + 1;
                    }
                });
                log.info("EndRunException happened for {}", detectorId);
                // if AD job failed consecutively due to EndRunException and failed times exceeds upper limit, will stop AD job
                if (detectorEndRunExceptionCount.get(detectorId) > maxRetryForEndRunException) {
                    log
                        .info(
                            "JobRunner will stop AD job due to EndRunException retry exceeds upper limit {} for {}",
                            maxRetryForEndRunException,
                            detectorId
                        );
                    stopAdJobForEndRunException(
                        jobParameter,
                        lockService,
                        lock,
                        detectionStartTime,
                        executionStartTime,
                        (EndRunException) exception
                    );
                    return;
                }
                indexAnomalyResultException(
                    jobParameter,
                    lockService,
                    lock,
                    detectionStartTime,
                    executionStartTime,
                    exception.getMessage(),
                    true
                );
            }
        } else {
            detectorEndRunExceptionCount.remove(detectorId);
            if (exception instanceof InternalFailure) {
                log.error("InternalFailure happened when executing anomaly result action for " + detectorId, exception);
            } else {
                log.error("Failed to execute anomaly result action for " + detectorId, exception);
            }
            indexAnomalyResultException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, exception, true);
        }
    }

    private void stopAdJobForEndRunException(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        EndRunException exception
    ) {
        String detectorId = jobParameter.getName();
        detectorEndRunExceptionCount.remove(detectorId);
        stopAdJob(detectorId);
        String errorPrefix = exception.isEndNow()
            ? "Stopped detector: "
            : "Stopped detector as job failed consecutively for more than " + this.maxRetryForEndRunException + " times: ";
        indexAnomalyResultException(
            jobParameter,
            lockService,
            lock,
            detectionStartTime,
            executionStartTime,
            errorPrefix + exception.getMessage(),
            true
        );
    }

    private void stopAdJob(String detectorId) {
        try {
            GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);

            clientUtil.<GetRequest, GetResponse>asyncRequest(getRequest, client::get, ActionListener.wrap(response -> {
                if (response.isExists()) {
                    try (
                        XContentParser parser = XContentType.JSON
                            .xContent()
                            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
                    ) {
                        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                        AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
                        if (job.isEnabled()) {
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
                            IndexRequest indexRequest = new IndexRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
                                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                                .source(newJob.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), XCONTENT_WITH_TYPE))
                                .id(detectorId);
                            clientUtil
                                .<IndexRequest, IndexResponse>asyncRequest(
                                    indexRequest,
                                    client::index,
                                    ActionListener.wrap(indexResponse -> {
                                        if (indexResponse != null
                                            && (indexResponse.getResult() == CREATED || indexResponse.getResult() == UPDATED)) {
                                            log.info("AD Job was disabled by JobRunner for " + detectorId);
                                        } else {
                                            log.warn("Failed to disable AD job for " + detectorId);
                                        }
                                    }, exception -> log.error("JobRunner failed to update AD job as disabled for " + detectorId, exception))
                                );
                        }
                    } catch (IOException e) {
                        log.error("JobRunner failed to stop detector job " + detectorId, e);
                    }
                }
            }, exception -> log.error("JobRunner failed to get detector job " + detectorId, exception)));
        } catch (Exception e) {
            log.error("JobRunner failed to stop AD job " + detectorId, e);
        }
    }

    private void indexAnomalyResult(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        AnomalyResultResponse response
    ) {
        String detectorId = jobParameter.getName();
        detectorEndRunExceptionCount.remove(detectorId);
        try {
            // skipping writing to the result index if not necessary
            // For a single-entity detector, the result is not useful if error is null
            // and rcf score (thus anomaly grade/confidence) is null.
            // For a multi-entity detector, we don't need to save on the detector level.
            // We always return 0 rcf score if there is no error.
            if (response.getAnomalyScore() <= 0 && response.getError() == null) {
                return;
            }
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) ((AnomalyDetectorJob) jobParameter).getWindowDelay();
            Instant dataStartTime = detectionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = executionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            User user = ((AnomalyDetectorJob) jobParameter).getUser();

            if (response.getError() != null) {
                log.info("Anomaly result action run successfully for {} with error {}", detectorId, response.getError());
            }
            AnomalyResult anomalyResult = new AnomalyResult(
                detectorId,
                response.getAnomalyScore(),
                response.getAnomalyGrade(),
                response.getConfidence(),
                response.getFeatures(),
                dataStartTime,
                dataEndTime,
                executionStartTime,
                Instant.now(),
                response.getError(),
                user,
                indexUtil.getSchemaVersion(ADIndex.RESULT)
            );
            anomalyResultHandler.index(anomalyResult, detectorId);
            detectionStateHandler.saveError(response.getError(), detectorId);
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + detectorId, e);
        } finally {
            releaseLock(jobParameter, lockService, lock);
        }
    }

    private void indexAnomalyResultException(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        Exception exception,
        boolean releaseLock
    ) {
        try {
            String errorMessage = exception instanceof AnomalyDetectionException
                ? exception.getMessage()
                : Throwables.getStackTraceAsString(exception);
            indexAnomalyResultException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, errorMessage, releaseLock);
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + jobParameter.getName(), e);
        }
    }

    private void indexAnomalyResultException(
        ScheduledJobParameter jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        String errorMessage,
        boolean releaseLock
    ) {
        String detectorId = jobParameter.getName();
        try {
            IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) ((AnomalyDetectorJob) jobParameter).getWindowDelay();
            Instant dataStartTime = detectionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            Instant dataEndTime = executionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
            User user = ((AnomalyDetectorJob) jobParameter).getUser();

            AnomalyResult anomalyResult = new AnomalyResult(
                detectorId,
                Double.NaN,
                Double.NaN,
                Double.NaN,
                new ArrayList<FeatureData>(),
                dataStartTime,
                dataEndTime,
                executionStartTime,
                Instant.now(),
                errorMessage,
                user,
                indexUtil.getSchemaVersion(ADIndex.RESULT)
            );
            anomalyResultHandler.index(anomalyResult, detectorId);
            detectionStateHandler.saveError(errorMessage, detectorId);
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + detectorId, e);
        } finally {
            if (releaseLock) {
                releaseLock(jobParameter, lockService, lock);
            }
        }
    }

    private void releaseLock(ScheduledJobParameter jobParameter, LockService lockService, LockModel lock) {
        lockService
            .release(
                lock,
                ActionListener
                    .wrap(
                        released -> { log.info("Released lock for AD job {}", jobParameter.getName()); },
                        exception -> { log.error("Failed to release lock for AD job: " + jobParameter.getName(), exception); }
                    )
            );
    }
}
