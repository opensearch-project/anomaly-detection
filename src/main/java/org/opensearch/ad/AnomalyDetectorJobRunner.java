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

package org.opensearch.ad;

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.ad.AnomalyDetectorPlugin.AD_THREAD_POOL_NAME;
import static org.opensearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.common.exception.AnomalyDetectionException;
import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.ad.common.exception.InternalFailure;
import org.opensearch.ad.indices.AnomalyDetectionIndices;
import org.opensearch.ad.model.ADTaskState;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyDetectorJob;
import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.AnomalyResultAction;
import org.opensearch.ad.transport.AnomalyResultRequest;
import org.opensearch.ad.transport.AnomalyResultResponse;
import org.opensearch.ad.transport.AnomalyResultTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.commons.InjectSecurity;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.threadpool.ThreadPool;

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
    private ThreadPool threadPool;
    private ConcurrentHashMap<String, Integer> detectorEndRunExceptionCount;
    private AnomalyDetectionIndices anomalyDetectionIndices;
    private ADTaskManager adTaskManager;
    private NodeStateManager nodeStateManager;
    private ExecuteADResultResponseRecorder recorder;

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

    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public void setSettings(Settings settings) {
        this.settings = settings;
        this.maxRetryForEndRunException = AnomalyDetectorSettings.MAX_RETRY_FOR_END_RUN_EXCEPTION.get(settings);
    }

    public void setAdTaskManager(ADTaskManager adTaskManager) {
        this.adTaskManager = adTaskManager;
    }

    public void setAnomalyDetectionIndices(AnomalyDetectionIndices anomalyDetectionIndices) {
        this.anomalyDetectionIndices = anomalyDetectionIndices;
    }

    public void setNodeStateManager(NodeStateManager nodeStateManager) {
        this.nodeStateManager = nodeStateManager;
    }

    public void setExecuteADResultResponseRecorder(ExecuteADResultResponseRecorder recorder) {
        this.recorder = recorder;
    }

    @Override
    public void runJob(ScheduledJobParameter scheduledJobParameter, JobExecutionContext context) {
        String detectorId = scheduledJobParameter.getName();
        log.info("Start to run AD job {}", detectorId);
        adTaskManager.refreshRealtimeJobRunTime(detectorId);
        if (!(scheduledJobParameter instanceof AnomalyDetectorJob)) {
            throw new IllegalArgumentException(
                "Job parameter is not instance of AnomalyDetectorJob, type: " + scheduledJobParameter.getClass().getCanonicalName()
            );
        }
        AnomalyDetectorJob jobParameter = (AnomalyDetectorJob) scheduledJobParameter;
        Instant executionStartTime = Instant.now();
        IntervalSchedule schedule = (IntervalSchedule) jobParameter.getSchedule();
        Instant detectionStartTime = executionStartTime.minus(schedule.getInterval(), schedule.getUnit());

        final LockService lockService = context.getLockService();

        Runnable runnable = () -> {
            nodeStateManager.getAnomalyDetector(detectorId, ActionListener.wrap(detectorOptional -> {
                if (!detectorOptional.isPresent()) {
                    log.error(new ParameterizedMessage("fail to get detector [{}]", detectorId));
                    return;
                }
                AnomalyDetector detector = detectorOptional.get();

                if (jobParameter.getLockDurationSeconds() != null) {
                    lockService
                        .acquireLock(
                            jobParameter,
                            context,
                            ActionListener
                                .wrap(
                                    lock -> runAdJob(
                                        jobParameter,
                                        lockService,
                                        lock,
                                        detectionStartTime,
                                        executionStartTime,
                                        recorder,
                                        detector
                                    ),
                                    exception -> {
                                        indexAnomalyResultException(
                                            jobParameter,
                                            lockService,
                                            null,
                                            detectionStartTime,
                                            executionStartTime,
                                            exception,
                                            false,
                                            recorder,
                                            detector
                                        );
                                        throw new IllegalStateException("Failed to acquire lock for AD job: " + detectorId);
                                    }
                                )
                        );
                } else {
                    log.warn("Can't get lock for AD job: " + detectorId);
                }
            }, e -> log.error(new ParameterizedMessage("fail to get detector [{}]", detectorId), e)));
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
     * @param recorder utility to record job execution result
     * @param detector associated detector accessor
     */
    protected void runAdJob(
        AnomalyDetectorJob jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        ExecuteADResultResponseRecorder recorder,
        AnomalyDetector detector
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
                false,
                recorder,
                detector
            );
            return;
        }
        anomalyDetectionIndices.update();

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
        if (jobParameter.getUser() == null) {
            // It's possible that user create domain with security disabled, then enable security
            // after upgrading. This is for BWC, for old detectors which created when security
            // disabled, the user will be null.
            user = "";
            roles = settings.getAsList("", ImmutableList.of("all_access", "AmazonES_all_access"));
        } else {
            user = jobParameter.getUser().getName();
            roles = jobParameter.getUser().getRoles();
        }
        String resultIndex = jobParameter.getResultIndex();
        if (resultIndex == null) {
            runAnomalyDetectionJob(
                jobParameter,
                lockService,
                lock,
                detectionStartTime,
                executionStartTime,
                detectorId,
                user,
                roles,
                recorder,
                detector
            );
            return;
        }
        ActionListener<Boolean> listener = ActionListener.wrap(r -> { log.debug("Custom index is valid"); }, e -> {
            Exception exception = new EndRunException(detectorId, e.getMessage(), true);
            handleAdException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, exception, recorder, detector);
        });
        anomalyDetectionIndices.validateCustomIndexForBackendJob(resultIndex, detectorId, user, roles, () -> {
            listener.onResponse(true);
            runAnomalyDetectionJob(
                jobParameter,
                lockService,
                lock,
                detectionStartTime,
                executionStartTime,
                detectorId,
                user,
                roles,
                recorder,
                detector
            );
        }, listener);
    }

    private void runAnomalyDetectionJob(
        AnomalyDetectorJob jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        String detectorId,
        String user,
        List<String> roles,
        ExecuteADResultResponseRecorder recorder,
        AnomalyDetector detector
    ) {

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
                                indexAnomalyResult(
                                    jobParameter,
                                    lockService,
                                    lock,
                                    detectionStartTime,
                                    executionStartTime,
                                    response,
                                    recorder,
                                    detector
                                );
                            },
                            exception -> {
                                handleAdException(
                                    jobParameter,
                                    lockService,
                                    lock,
                                    detectionStartTime,
                                    executionStartTime,
                                    exception,
                                    recorder,
                                    detector
                                );
                            }
                        )
                );
        } catch (Exception e) {
            indexAnomalyResultException(
                jobParameter,
                lockService,
                lock,
                detectionStartTime,
                executionStartTime,
                e,
                true,
                recorder,
                detector
            );
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
     * @param recorder utility to record job execution result
     * @param detector associated detector accessor
     */
    protected void handleAdException(
        AnomalyDetectorJob jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        Exception exception,
        ExecuteADResultResponseRecorder recorder,
        AnomalyDetector detector
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
                    (EndRunException) exception,
                    recorder,
                    detector
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
                        (EndRunException) exception,
                        recorder,
                        detector
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
                    true,
                    recorder,
                    detector
                );
            }
        } else {
            detectorEndRunExceptionCount.remove(detectorId);
            if (exception instanceof InternalFailure) {
                log.error("InternalFailure happened when executing anomaly result action for " + detectorId, exception);
            } else {
                log.error("Failed to execute anomaly result action for " + detectorId, exception);
            }
            indexAnomalyResultException(
                jobParameter,
                lockService,
                lock,
                detectionStartTime,
                executionStartTime,
                exception,
                true,
                recorder,
                detector
            );
        }
    }

    private void stopAdJobForEndRunException(
        AnomalyDetectorJob jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        EndRunException exception,
        ExecuteADResultResponseRecorder recorder,
        AnomalyDetector detector
    ) {
        String detectorId = jobParameter.getName();
        detectorEndRunExceptionCount.remove(detectorId);
        String errorPrefix = exception.isEndNow()
            ? "Stopped detector: "
            : "Stopped detector as job failed consecutively for more than " + this.maxRetryForEndRunException + " times: ";
        String error = errorPrefix + exception.getMessage();
        stopAdJob(
            detectorId,
            () -> indexAnomalyResultException(
                jobParameter,
                lockService,
                lock,
                detectionStartTime,
                executionStartTime,
                error,
                true,
                ADTaskState.STOPPED.name(),
                recorder,
                detector
            )
        );
    }

    private void stopAdJob(String detectorId, AnomalyDetectorFunction function) {
        GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);
        ActionListener<GetResponse> listener = ActionListener.wrap(response -> {
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
                            job.getUser(),
                            job.getResultIndex()
                        );
                        IndexRequest indexRequest = new IndexRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                            .source(newJob.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), XCONTENT_WITH_TYPE))
                            .id(detectorId);

                        client.index(indexRequest, ActionListener.wrap(indexResponse -> {
                            if (indexResponse != null && (indexResponse.getResult() == CREATED || indexResponse.getResult() == UPDATED)) {
                                log.info("AD Job was disabled by JobRunner for " + detectorId);
                                // function.execute();
                            } else {
                                log.warn("Failed to disable AD job for " + detectorId);
                            }
                        }, exception -> { log.error("JobRunner failed to update AD job as disabled for " + detectorId, exception); }));
                    } else {
                        log.info("AD Job was disabled for " + detectorId);
                        // function.execute();
                    }
                } catch (IOException e) {
                    log.error("JobRunner failed to stop detector job " + detectorId, e);
                }
            } else {
                log.info("AD Job was not found for " + detectorId);
                // function.execute();
            }
        }, exception -> log.error("JobRunner failed to get detector job " + detectorId, exception));

        client.get(getRequest, ActionListener.runAfter(listener, () -> function.execute()));
    }

    private void indexAnomalyResult(
        AnomalyDetectorJob jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        AnomalyResultResponse response,
        ExecuteADResultResponseRecorder recorder,
        AnomalyDetector detector
    ) {
        String detectorId = jobParameter.getName();
        detectorEndRunExceptionCount.remove(detectorId);
        try {
            recorder.indexAnomalyResult(detectionStartTime, executionStartTime, response, detector);
        } catch (EndRunException e) {
            handleAdException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, e, recorder, detector);
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + detectorId, e);
        } finally {
            releaseLock(jobParameter, lockService, lock);
        }

    }

    private void indexAnomalyResultException(
        AnomalyDetectorJob jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        Exception exception,
        boolean releaseLock,
        ExecuteADResultResponseRecorder recorder,
        AnomalyDetector detector
    ) {
        try {
            String errorMessage = exception instanceof AnomalyDetectionException
                ? exception.getMessage()
                : Throwables.getStackTraceAsString(exception);
            indexAnomalyResultException(
                jobParameter,
                lockService,
                lock,
                detectionStartTime,
                executionStartTime,
                errorMessage,
                releaseLock,
                recorder,
                detector
            );
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + jobParameter.getName(), e);
        }
    }

    private void indexAnomalyResultException(
        AnomalyDetectorJob jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        String errorMessage,
        boolean releaseLock,
        ExecuteADResultResponseRecorder recorder,
        AnomalyDetector detector
    ) {
        indexAnomalyResultException(
            jobParameter,
            lockService,
            lock,
            detectionStartTime,
            executionStartTime,
            errorMessage,
            releaseLock,
            null,
            recorder,
            detector
        );
    }

    private void indexAnomalyResultException(
        AnomalyDetectorJob jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        String errorMessage,
        boolean releaseLock,
        String taskState,
        ExecuteADResultResponseRecorder recorder,
        AnomalyDetector detector
    ) {
        try {
            recorder.indexAnomalyResultException(detectionStartTime, executionStartTime, errorMessage, taskState, detector);
        } finally {
            if (releaseLock) {
                releaseLock(jobParameter, lockService, lock);
            }
        }
    }

    private void releaseLock(AnomalyDetectorJob jobParameter, LockService lockService, LockModel lock) {
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
