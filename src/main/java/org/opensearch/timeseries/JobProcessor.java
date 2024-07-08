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

package org.opensearch.timeseries;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionType;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.commons.InjectSecurity;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.InternalFailure;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.rest.handler.IndexJobActionHandler;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.timeseries.transport.ResultRequest;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.util.SecurityUtil;

import com.google.common.base.Throwables;

/**
 * JobScheduler will call job runner to get time series analysis result periodically
 */
public abstract class JobProcessor<IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, IndexableResultType extends IndexableResult, ProfileActionType extends ActionType<ProfileResponse>, ExecuteResultResponseRecorderType extends ExecuteResultResponseRecorder<IndexType, IndexManagementType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, IndexableResultType, ProfileActionType>, IndexJobActionHandlerType extends IndexJobActionHandler<IndexType, IndexManagementType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, IndexableResultType, ProfileActionType, ExecuteResultResponseRecorderType>> {

    private static final Logger log = LogManager.getLogger(JobProcessor.class);

    private Settings settings;
    private int maxRetryForEndRunException;
    private Client client;
    private ThreadPool threadPool;
    private ConcurrentHashMap<String, Integer> endRunExceptionCount;
    protected IndexManagementType indexManagement;
    private TaskManagerType taskManager;
    private NodeStateManager nodeStateManager;
    private ExecuteResultResponseRecorderType recorder;
    private AnalysisType analysisType;
    private String threadPoolName;
    private ActionType<? extends ResultResponse<IndexableResultType>> resultAction;
    private IndexJobActionHandlerType indexJobActionHandler;

    protected JobProcessor(
        AnalysisType analysisType,
        String threadPoolName,
        ActionType<? extends ResultResponse<IndexableResultType>> resultAction
    ) {
        // Singleton class, use getJobRunnerInstance method instead of constructor
        this.endRunExceptionCount = new ConcurrentHashMap<>();
        this.analysisType = analysisType;
        this.threadPoolName = threadPoolName;
        this.resultAction = resultAction;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public void setThreadPool(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    protected void registerSettings(Settings settings, Setting<Integer> maxRetryForEndRunExceptionSetting) {
        this.settings = settings;
        this.maxRetryForEndRunException = maxRetryForEndRunExceptionSetting.get(settings);
    }

    public void setTaskManager(TaskManagerType adTaskManager) {
        this.taskManager = adTaskManager;
    }

    public void setIndexManagement(IndexManagementType anomalyDetectionIndices) {
        this.indexManagement = anomalyDetectionIndices;
    }

    public void setNodeStateManager(NodeStateManager nodeStateManager) {
        this.nodeStateManager = nodeStateManager;
    }

    public void setExecuteResultResponseRecorder(ExecuteResultResponseRecorderType recorder) {
        this.recorder = recorder;
    }

    public void setIndexJobActionHandler(IndexJobActionHandlerType indexJobActionHandler) {
        this.indexJobActionHandler = indexJobActionHandler;
    }

    public void process(Job jobParameter, JobExecutionContext context) {
        String configId = jobParameter.getName();

        log.info("Start to run {} job {}", analysisType, configId);

        taskManager.refreshRealtimeJobRunTime(configId);

        Instant executionEndTime = Instant.now();
        IntervalSchedule schedule = (IntervalSchedule) jobParameter.getSchedule();
        Instant executionStartTime = executionEndTime.minus(schedule.getInterval(), schedule.getUnit());

        final LockService lockService = context.getLockService();

        Runnable runnable = () -> {
            try {
                nodeStateManager.getConfig(configId, analysisType, ActionListener.wrap(configOptional -> {
                    if (!configOptional.isPresent()) {
                        log.error(new ParameterizedMessage("fail to get config [{}]", configId));
                        return;
                    }
                    Config config = configOptional.get();

                    if (jobParameter.getLockDurationSeconds() != null) {
                        lockService
                            .acquireLock(
                                jobParameter,
                                context,
                                ActionListener
                                    .wrap(
                                        lock -> runJob(
                                            jobParameter,
                                            lockService,
                                            lock,
                                            executionStartTime,
                                            executionEndTime,
                                            recorder,
                                            config
                                        ),
                                        exception -> {
                                            indexResultException(
                                                jobParameter,
                                                lockService,
                                                null,
                                                executionStartTime,
                                                executionEndTime,
                                                exception,
                                                false,
                                                recorder,
                                                config
                                            );
                                            throw new IllegalStateException("Failed to acquire lock for job: " + configId);
                                        }
                                    )
                            );
                    } else {
                        log.warn("Can't get lock for job: " + configId);
                    }

                }, e -> log.error(new ParameterizedMessage("fail to get config [{}]", configId), e)));
            } catch (Exception e) {
                // os log won't show anything if there is an exception happens (maybe due to running on a ExecutorService)
                // we at least log the error.
                log.error("Can't start job: " + configId, e);
                throw e;
            }
        };

        ExecutorService executor = threadPool.executor(threadPoolName);
        executor.submit(runnable);
    }

    /**
     * Get analysis result, index result or handle exception if failed.
     *
     * @param jobParameter scheduled job parameter
     * @param lockService lock service
     * @param lock lock to run job
     * @param executionStartTime analysis start time
     * @param executionEndTime analysis end time
     * @param recorder utility to record job execution result
     * @param detector associated detector accessor
     */
    public void runJob(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant executionStartTime,
        Instant executionEndTime,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        String configId = jobParameter.getName();
        if (lock == null) {
            indexResultException(
                jobParameter,
                lockService,
                lock,
                executionStartTime,
                executionEndTime,
                "Can't run job due to null lock",
                false,
                recorder,
                detector
            );
            return;
        }
        indexManagement.update();

        User userInfo = SecurityUtil.getUserFromJob(jobParameter, settings);

        String user = userInfo.getName();
        List<String> roles = userInfo.getRoles();

        validateResultIndexAndRunJob(
            jobParameter,
            lockService,
            lock,
            executionStartTime,
            executionEndTime,
            configId,
            user,
            roles,
            recorder,
            detector
        );
    }

    protected abstract void validateResultIndexAndRunJob(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant executionStartTime,
        Instant executionEndTime,
        String configId,
        String user,
        List<String> roles,
        ExecuteResultResponseRecorderType recorder2,
        Config detector
    );

    protected void runJob(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant executionStartTime,
        Instant executionEndTime,
        String configId,
        String user,
        List<String> roles,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        // using one thread in the write threadpool
        try (InjectSecurity injectSecurity = new InjectSecurity(configId, settings, client.threadPool().getThreadContext())) {
            // Injecting user role to verify if the user has permissions for our API.
            injectSecurity.inject(user, roles);

            ResultRequest request = createResultRequest(configId, executionStartTime.toEpochMilli(), executionEndTime.toEpochMilli());
            client.execute(resultAction, request, ActionListener.wrap(response -> {
                indexResult(jobParameter, lockService, lock, executionStartTime, executionEndTime, response, recorder, detector);
            },
                exception -> {
                    handleException(jobParameter, lockService, lock, executionStartTime, executionEndTime, exception, recorder, detector);
                }
            ));
        } catch (Exception e) {
            indexResultException(jobParameter, lockService, lock, executionStartTime, executionEndTime, e, true, recorder, detector);
            log.error("Failed to execute AD job " + configId, e);
        }
    }

    /**
     * Handle exception from anomaly result action.
     *
     * 1. If exception is {@link EndRunException}
     *   a). if isEndNow == true, stop job and store exception in result
     *   b). if isEndNow == false, record count of {@link EndRunException} for this
     *       analysis. If count of {@link EndRunException} exceeds upper limit, will
     *       stop job and store exception in result; otherwise, just
     *       store exception in result, not stop job for the config.
     *
     * 2. If exception is not {@link EndRunException}, decrease count of
     *    {@link EndRunException} for the config and index exception in
     *    result. If exception is {@link InternalFailure}, will not log exception
     *    stack trace as already logged in {@link JobProcessor}.
     *
     * TODO: Handle finer granularity exception such as some exception may be
     *       transient and retry in current job may succeed. Currently, we don't
     *       know which exception is transient and retryable in
     *       {@link JobProcessor}. So we don't add backoff retry
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
     * @param config associated config accessor
     */
    public void handleException(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        Exception exception,
        ExecuteResultResponseRecorderType recorder,
        Config config
    ) {
        String configId = jobParameter.getName();
        if (exception instanceof EndRunException) {
            log.error("EndRunException happened when executing result action for " + configId, exception);

            if (((EndRunException) exception).isEndNow()) {
                // Stop AD job if EndRunException shows we should end job now.
                log.info("JobRunner will stop job due to EndRunException for {}", configId);
                stopJobForEndRunException(
                    jobParameter,
                    lockService,
                    lock,
                    detectionStartTime,
                    executionStartTime,
                    (EndRunException) exception,
                    recorder,
                    config
                );
            } else {
                endRunExceptionCount.compute(configId, (k, v) -> {
                    if (v == null) {
                        return 1;
                    } else {
                        return v + 1;
                    }
                });
                log.info("EndRunException happened for {}", configId);
                // if AD job failed consecutively due to EndRunException and failed times exceeds upper limit, will stop AD job
                if (endRunExceptionCount.get(configId) > maxRetryForEndRunException) {
                    log
                        .info(
                            "JobRunner will stop job due to EndRunException retry exceeds upper limit {} for {}",
                            maxRetryForEndRunException,
                            configId
                        );
                    stopJobForEndRunException(
                        jobParameter,
                        lockService,
                        lock,
                        detectionStartTime,
                        executionStartTime,
                        (EndRunException) exception,
                        recorder,
                        config
                    );
                    return;
                }
                indexResultException(
                    jobParameter,
                    lockService,
                    lock,
                    detectionStartTime,
                    executionStartTime,
                    exception.getMessage(),
                    true,
                    recorder,
                    config
                );
            }
        } else {
            endRunExceptionCount.remove(configId);
            if (exception instanceof InternalFailure) {
                log.error("InternalFailure happened when executing result action for " + configId, exception);
            } else {
                log.error("Failed to execute result action for " + configId, exception);
            }
            indexResultException(
                jobParameter,
                lockService,
                lock,
                detectionStartTime,
                executionStartTime,
                exception,
                true,
                recorder,
                config
            );
        }
    }

    private void stopJobForEndRunException(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        EndRunException exception,
        ExecuteResultResponseRecorderType recorder,
        Config config
    ) {
        String configId = jobParameter.getName();
        endRunExceptionCount.remove(configId);
        String errorPrefix = exception.isEndNow()
            ? "Stopped analysis: "
            : "Stopped analysis as job failed consecutively for more than " + this.maxRetryForEndRunException + " times: ";
        String error = errorPrefix + exception.getMessage();

        ExecutorFunction runAfer = () -> indexResultException(
            jobParameter,
            lockService,
            lock,
            detectionStartTime,
            executionStartTime,
            error,
            true,
            TaskState.STOPPED.name(),
            recorder,
            config
        );

        ActionListener<JobResponse> stopListener = ActionListener.wrap(jobResponse -> {
            log.info(new ParameterizedMessage("Job {} was disabled by JobRunner", configId));
            runAfer.execute();
        }, exp -> {
            log.error(new ParameterizedMessage("JobRunner failed to update job {} to disabled.", configId), exp);
            runAfer.execute();
        });

        // transport service is null as we cannot access transport service outside of transport action
        // to reset real time job we don't need transport service and we have guarded against the null
        // reference in task manager
        indexJobActionHandler.stopJob(configId, null, stopListener);
    }

    private void indexResult(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant executionStartTime,
        Instant executionEndTime,
        ResultResponse<IndexableResultType> response,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        String detectorId = jobParameter.getName();
        endRunExceptionCount.remove(detectorId);
        try {
            recorder.indexResult(executionStartTime, executionEndTime, response, detector);
        } catch (EndRunException e) {
            handleException(jobParameter, lockService, lock, executionStartTime, executionEndTime, e, recorder, detector);
        } catch (Exception e) {
            log.error("Failed to index anomaly result for " + detectorId, e);
        } finally {
            releaseLock(jobParameter, lockService, lock);
        }

    }

    private void indexResultException(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        Exception exception,
        boolean releaseLock,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        try {
            String errorMessage = exception instanceof TimeSeriesException
                ? exception.getMessage()
                : Throwables.getStackTraceAsString(exception);
            indexResultException(
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
            log.error("Failed to index result for " + jobParameter.getName(), e);
        }
    }

    private void indexResultException(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        String errorMessage,
        boolean releaseLock,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        indexResultException(
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

    private void indexResultException(
        Job jobParameter,
        LockService lockService,
        LockModel lock,
        Instant detectionStartTime,
        Instant executionStartTime,
        String errorMessage,
        boolean releaseLock,
        String taskState,
        ExecuteResultResponseRecorderType recorder,
        Config detector
    ) {
        try {
            recorder.indexResultException(detectionStartTime, executionStartTime, errorMessage, taskState, detector);
        } finally {
            if (releaseLock) {
                releaseLock(jobParameter, lockService, lock);
            }
        }
    }

    private void releaseLock(Job jobParameter, LockService lockService, LockModel lock) {
        lockService
            .release(
                lock,
                ActionListener
                    .wrap(released -> { log.info("Released lock for {} job {}", analysisType, jobParameter.getName()); }, exception -> {
                        log
                            .error(
                                new ParameterizedMessage("Failed to release lock for [{}] job [{}]", analysisType, jobParameter.getName()),
                                exception
                            );
                    })
            );
    }

    public Integer getEndRunExceptionCount(String configId) {
        return endRunExceptionCount.getOrDefault(configId, 0);
    }

    protected abstract ResultRequest createResultRequest(String configID, long start, long end);
}
