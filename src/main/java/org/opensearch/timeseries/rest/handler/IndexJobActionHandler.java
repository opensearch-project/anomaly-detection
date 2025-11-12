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

package org.opensearch.timeseries.rest.handler;

import static org.opensearch.action.DocWriteResponse.Result.CREATED;
import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.util.RestHandlerUtils.createXContentParserFromRegistry;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ExecuteResultResponseRecorder;
import org.opensearch.timeseries.NodeStateManager;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.TaskState;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.JobResponse;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.timeseries.transport.ResultRequest;
import org.opensearch.timeseries.transport.ResultResponse;
import org.opensearch.timeseries.transport.StopConfigRequest;
import org.opensearch.timeseries.transport.StopConfigResponse;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import com.google.common.base.Throwables;

/**
 * job REST action handler to process POST/PUT request.
 */
public abstract class IndexJobActionHandler<IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, IndexableResultType extends IndexableResult, ProfileActionType extends ActionType<ProfileResponse>, ExecuteResultResponseRecorderType extends ExecuteResultResponseRecorder<IndexType, IndexManagementType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, IndexableResultType, ProfileActionType>> {

    private final IndexManagementType indexManagement;
    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    protected final TaskManagerType taskManager;

    private final Logger logger = LogManager.getLogger(IndexJobActionHandler.class);
    private final TimeValue requestTimeout;
    private final ExecuteResultResponseRecorderType recorder;
    private final ActionType<? extends ResultResponse<IndexableResultType>> resultAction;
    private final AnalysisType analysisType;
    private final String stateIndex;
    private final ActionType<StopConfigResponse> stopConfigAction;
    protected final NodeStateManager nodeStateManager;

    /** 
     * Constructor function.
     *
     * @param client                  ES node client that executes actions on the local node
     * @param indexManagement         index manager
     * @param xContentRegistry        Registry which is used for XContentParser
     * @param taskManager             task manager
     * @param recorder                Utility to record AnomalyResultAction execution result
     * @param resultAction            result action
     * @param analysisType            analysis type
     * @param stateIndex              State index name
     * @param stopConfigAction        Stop config action
     * @param nodeStateManager        Node state manager
     * @param settings                Node settings
     * @param timeoutSetting          timeout setting
     */
    public IndexJobActionHandler(
        Client client,
        IndexManagementType indexManagement,
        NamedXContentRegistry xContentRegistry,
        TaskManagerType taskManager,
        ExecuteResultResponseRecorderType recorder,
        ActionType<? extends ResultResponse<IndexableResultType>> resultAction,
        AnalysisType analysisType,
        String stateIndex,
        ActionType<StopConfigResponse> stopConfigAction,
        NodeStateManager nodeStateManager,
        Settings settings,
        Setting<TimeValue> timeoutSetting
    ) {
        this.client = client;
        this.indexManagement = indexManagement;
        this.xContentRegistry = xContentRegistry;
        this.taskManager = taskManager;
        this.recorder = recorder;
        this.resultAction = resultAction;
        this.analysisType = analysisType;
        this.stateIndex = stateIndex;
        this.stopConfigAction = stopConfigAction;
        this.nodeStateManager = nodeStateManager;
        this.requestTimeout = timeoutSetting.get(settings);
    }

    /**
     * Start job.
     * 1. If job doesn't exist, create new job.
     * 2. If job exists: a). if job enabled, return error message; b). if job disabled, enable job.
     * @param config config accessor
     * @param transportService transport service
     * @param clock clock to get current time
     * @param listener Listener to send responses
     */
    public void startJob(Config config, TransportService transportService, Clock clock, ActionListener<JobResponse> listener) {
        // this start listener is created & injected throughout the job handler so that whenever the job response is received,
        // there's the extra step of trying to index results and update detector state with a 60s delay.
        ActionListener<JobResponse> startListener = ActionListener.wrap(r -> {
            try {
                Instant executionEndTime = Instant.now();
                IntervalTimeConfiguration schedule = (IntervalTimeConfiguration) config.getInterval();
                Instant executionStartTime = executionEndTime.minus(schedule.getInterval(), schedule.getUnit());
                ResultRequest getRequest = createResultRequest(
                    config.getId(),
                    executionStartTime.toEpochMilli(),
                    executionEndTime.toEpochMilli()
                );
                client.execute(resultAction, getRequest, ActionListener.wrap(response -> {
                    recorder.indexResult(executionStartTime, executionEndTime, response, config);
                }, exception -> {
                    recorder
                        .indexResultException(
                            executionStartTime,
                            executionEndTime,
                            Throwables.getStackTraceAsString(exception),
                            null,
                            config,
                            clock
                        );
                }));
            } catch (Exception ex) {
                listener.onFailure(ex);
                return;
            }
            listener.onResponse(r);

        }, listener::onFailure);
        if (!indexManagement.doesJobIndexExist()) {
            indexManagement.initJobIndex(ActionListener.wrap(response -> {
                if (response.isAcknowledged()) {
                    logger.info("Created {} with mappings.", CommonName.JOB_INDEX);
                    createJob(config, transportService, startListener);
                } else {
                    logger.warn("Created {} with mappings call not acknowledged.", CommonName.JOB_INDEX);
                    startListener
                        .onFailure(
                            new OpenSearchStatusException(
                                "Created " + CommonName.JOB_INDEX + " with mappings call not acknowledged.",
                                RestStatus.INTERNAL_SERVER_ERROR
                            )
                        );
                }
            }, exception -> startListener.onFailure(exception)));
        } else {
            createJob(config, transportService, startListener);
        }
    }

    private void createJob(Config config, TransportService transportService, ActionListener<JobResponse> listener) {
        try {
            IntervalTimeConfiguration frequency = (IntervalTimeConfiguration) config.getInferredFrequency();
            Schedule schedule = new IntervalSchedule(Instant.now(), (int) frequency.getInterval(), frequency.getUnit());
            Duration duration = Duration.of(frequency.getInterval(), frequency.getUnit());

            Job job = new Job(
                config.getId(),
                schedule,
                config.getWindowDelay(),
                true,
                Instant.now(),
                null,
                Instant.now(),
                duration.getSeconds(),
                config.getUser(),
                config.getCustomResultIndexOrAlias(),
                analysisType
            );

            getJobForWrite(config, job, transportService, listener);
        } catch (Exception e) {
            String message = "Failed to parse job " + config.getId();
            logger.error(message, e);
            listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    private void getJobForWrite(Config config, Job job, TransportService transportService, ActionListener<JobResponse> listener) {
        GetRequest getRequest = new GetRequest(CommonName.JOB_INDEX).id(config.getId());

        client
            .get(
                getRequest,
                ActionListener
                    .wrap(
                        response -> onGetJobForWrite(response, config, job, transportService, listener),
                        exception -> listener.onFailure(exception)
                    )
            );
    }

    private void onGetJobForWrite(
        GetResponse response,
        Config config,
        Job job,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) throws IOException {
        if (response.isExists()) {
            try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                Job currentAdJob = Job.parse(parser);
                if (currentAdJob.isEnabled()) {
                    listener
                        .onFailure(
                            new OpenSearchStatusException("Anomaly detector job is already running: " + config.getId(), RestStatus.OK)
                        );
                    return;
                } else {
                    Job newJob = new Job(
                        job.getName(),
                        job.getSchedule(),
                        job.getWindowDelay(),
                        job.isEnabled(),
                        Instant.now(),
                        currentAdJob.getDisabledTime(),
                        Instant.now(),
                        job.getLockDurationSeconds(),
                        job.getUser(),
                        job.getCustomResultIndexOrAlias(),
                        job.getAnalysisType()
                    );
                    // Get latest realtime task and check its state before index job. Will reset running realtime task
                    // as STOPPED first if job disabled, then start new job and create new realtime task.
                    startConfig(
                        config,
                        null,
                        job.getUser(),
                        transportService,
                        ActionListener.wrap(r -> { indexJob(newJob, null, listener); }, e -> {
                            // Have logged error message in ADTaskManager#startDetector
                            listener.onFailure(e);
                        })
                    );
                }
            } catch (IOException e) {
                String message = "Failed to parse job " + job.getName();
                logger.error(message, e);
                listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
            }
        } else {
            startConfig(
                config,
                null,
                job.getUser(),
                transportService,
                ActionListener.wrap(r -> { indexJob(job, null, listener); }, e -> listener.onFailure(e))
            );
        }
    }

    /**
     * Start config.
     * For historical analysis, this method will be called on coordinating node.
     * For realtime task, we won't know AD job coordinating node until AD job starts. So
     * this method will be called on vanilla node.
     *
     * Will init task index if not exist and write new AD task to index. If task index
     * exists, will check if there is task running. If no running task, reset old task
     * as not latest and clean old tasks which exceeds max old task doc limitation.
     * Then find out node with least load and dispatch task to that node(worker node).
     *
     * @param config anomaly detector
     * @param dateRange detection date range
     * @param user user
     * @param transportService transport service
     * @param listener action listener
     */
    public void startConfig(
        Config config,
        DateRange dateRange,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        try {
            if (indexManagement.doesStateIndexExist()) {
                // If state index exist, check if latest AD task is running
                taskManager.getAndExecuteOnLatestConfigLevelTask(config, dateRange, false, user, transportService, listener);
            } else {
                // If state index doesn't exist, create index and execute detector.
                indexManagement.initStateIndex(ActionListener.wrap(r -> {
                    if (r.isAcknowledged()) {
                        logger.info("Created {} with mappings.", stateIndex);
                        taskManager.updateLatestFlagOfOldTasksAndCreateNewTask(config, dateRange, false, user, TaskState.CREATED, listener);
                    } else {
                        String error = String.format(Locale.ROOT, CommonMessages.CREATE_INDEX_NOT_ACKNOWLEDGED, stateIndex);
                        logger.warn(error);
                        listener.onFailure(new OpenSearchStatusException(error, RestStatus.INTERNAL_SERVER_ERROR));
                    }
                }, e -> {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        taskManager.updateLatestFlagOfOldTasksAndCreateNewTask(config, dateRange, false, user, TaskState.CREATED, listener);
                    } else {
                        logger.error("Failed to init state index", e);
                        listener.onFailure(e);
                    }
                }));
            }
        } catch (Exception e) {
            logger.error("Failed to start config " + config.getId(), e);
            listener.onFailure(e);
        }
    }

    private void indexJob(Job job, ExecutorFunction function, ActionListener<JobResponse> listener) throws IOException {
        IndexRequest indexRequest = new IndexRequest(CommonName.JOB_INDEX)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(job.toXContent(XContentFactory.jsonBuilder(), RestHandlerUtils.XCONTENT_WITH_TYPE))
            .timeout(requestTimeout)
            .id(job.getName());
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
        ExecutorFunction function,
        ActionListener<JobResponse> listener
    ) {
        if (response == null || (response.getResult() != CREATED && response.getResult() != UPDATED)) {
            String errorMsg = ExceptionUtil.getShardsFailure(response);
            listener
                .onFailure(
                    new OpenSearchStatusException(errorMsg, response == null ? RestStatus.INTERNAL_SERVER_ERROR : response.status())
                );
            return;
        }
        if (function != null) {
            function.execute();
        } else {
            JobResponse anomalyDetectorJobResponse = new JobResponse(response.getId());
            listener.onResponse(anomalyDetectorJobResponse);
        }
    }

    /**
     * Stop config job.
     * 1.If job not exists, return error message
     * 2.If job exists: a).if job state is disabled, return error message; b).if job state is enabled, disable job.
     *
     * @param configId config identifier
     * @param listener Listener to send responses
     */
    public void stopJob(String configId, TransportService transportService, ActionListener<JobResponse> listener) {
        GetRequest getRequest = new GetRequest(CommonName.JOB_INDEX).id(configId);

        client.get(getRequest, ActionListener.wrap(response -> {
            if (response.isExists()) {
                try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    Job job = Job.parse(parser);
                    if (!job.isEnabled()) {
                        taskManager.stopLatestRealtimeTask(configId, TaskState.STOPPED, null, transportService, listener);
                    } else {
                        Job newJob = new Job(
                            job.getName(),
                            job.getSchedule(),
                            job.getWindowDelay(),
                            false, // disable job
                            job.getEnabledTime(),
                            Instant.now(),
                            Instant.now(),
                            job.getLockDurationSeconds(),
                            job.getUser(),
                            job.getCustomResultIndexOrAlias(),
                            job.getAnalysisType()
                        );
                        indexJob(
                            newJob,
                            () -> client
                                .execute(
                                    stopConfigAction,
                                    new StopConfigRequest(configId),
                                    stopConfigListener(configId, transportService, listener)
                                ),
                            listener
                        );
                    }
                } catch (IOException e) {
                    String message = "Failed to parse job " + configId;
                    logger.error(message, e);
                    listener.onFailure(new OpenSearchStatusException(message, RestStatus.INTERNAL_SERVER_ERROR));
                }
            } else {
                logger.info(new ParameterizedMessage("Job {} was not found", configId));
                listener.onResponse(new JobResponse(configId));
            }
        }, exception -> {
            logger.error("JobRunner failed to get job " + configId, exception);
            if (exception instanceof IndexNotFoundException) {
                listener.onResponse(new JobResponse(configId));
            } else {
                listener.onFailure(exception);
            }
        }));
    }

    public ActionListener<StopConfigResponse> stopConfigListener(
        String configId,
        TransportService transportService,
        ActionListener<JobResponse> listener
    ) {
        return new ActionListener<StopConfigResponse>() {
            @Override
            public void onResponse(StopConfigResponse stopDetectorResponse) {
                if (stopDetectorResponse.success()) {
                    logger.info("model deleted successfully for config {}", configId);
                    // e.g., StopDetectorTransportAction will send out DeleteModelAction which will clear all realtime cache.
                    // Pass null transport service to method "stopLatestRealtimeTask" to not re-clear coordinating node cache.
                    taskManager.stopLatestRealtimeTask(configId, TaskState.STOPPED, null, null, listener);
                } else {
                    logger.error("Failed to delete model for config {}", configId);
                    // If failed to clear all realtime cache, will try to re-clear coordinating node cache.
                    taskManager
                        .stopLatestRealtimeTask(
                            configId,
                            TaskState.FAILED,
                            new OpenSearchStatusException("Failed to delete model", RestStatus.INTERNAL_SERVER_ERROR),
                            transportService,
                            listener
                        );
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Failed to delete model for config " + configId, e);
                // If failed to clear all realtime cache, will try to re-clear coordinating node cache.
                taskManager
                    .stopLatestRealtimeTask(
                        configId,
                        TaskState.FAILED,
                        new OpenSearchStatusException("Failed to execute stop config action", RestStatus.INTERNAL_SERVER_ERROR),
                        transportService,
                        listener
                    );
            }
        };
    }

    /**
     * Start config. Will create schedule job for realtime analysis,
     * and start task for historical/run once.
     *
     * @param configId config id
     * @param dateRange historical analysis date range
     * @param user user
     * @param transportService transport service
     * @param context thread context
     * @param clock Clock to get current time
     * @param listener action listener
     */
    public void startConfig(
        String configId,
        DateRange dateRange,
        User user,
        TransportService transportService,
        ThreadContext.StoredContext context,
        Clock clock,
        ActionListener<JobResponse> listener
    ) {
        // upgrade index mapping
        indexManagement.update();

        nodeStateManager.getConfig(configId, analysisType, (config) -> {
            if (!config.isPresent()) {
                listener.onFailure(new OpenSearchStatusException(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + configId, RestStatus.NOT_FOUND));
                return;
            }
            // Validate if config is ready to start. Will return null if ready to start.
            String errorMessage = validateConfig(config.get());
            if (errorMessage != null) {
                listener.onFailure(new OpenSearchStatusException(errorMessage, RestStatus.BAD_REQUEST));
                return;
            }
            String resultIndex = config.get().getCustomResultIndexOrAlias();
            if (resultIndex == null) {
                startRealtimeOrHistoricalAnalysis(dateRange, user, transportService, listener, config, clock);
                return;
            }
            context.restore();
            indexManagement
                .initCustomResultIndexAndExecute(
                    resultIndex,
                    () -> startRealtimeOrHistoricalAnalysis(dateRange, user, transportService, listener, config, clock),
                    listener
                );

        }, listener);
    }

    private String validateConfig(Config detector) {
        String error = null;
        if (detector.getFeatureAttributes().size() == 0) {
            error = "Can't start job as no features configured";
        } else if (detector.getEnabledFeatureIds().size() == 0) {
            error = "Can't start job as no enabled features configured";
        }
        return error;
    }

    private void startRealtimeOrHistoricalAnalysis(
        DateRange dateRange,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener,
        Optional<? extends Config> config,
        Clock clock
    ) {
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            if (dateRange == null) {
                // start realtime job
                startJob(config.get(), transportService, clock, listener);
            } else {
                // start historical analysis task
                taskManager.startHistorical(config.get(), dateRange, user, transportService, listener);
            }
        } catch (Exception e) {
            logger.error("Failed to stash context", e);
            listener.onFailure(e);
        }
    }

    protected abstract ResultRequest createResultRequest(String configID, long start, long end);

    protected abstract List<TaskTypeEnum> getBatchConfigTaskTypes();

    public abstract void stopConfig(
        String configId,
        boolean historical,
        User user,
        TransportService transportService,
        ActionListener<JobResponse> listener
    );
}
