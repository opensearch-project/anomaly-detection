/// *
// * SPDX-License-Identifier: Apache-2.0
// *
// * The OpenSearch Contributors require contributions made to
// * this file be licensed under the Apache-2.0 license or a
// * compatible open source license.
// *
// * Modifications Copyright OpenSearch Contributors. See
// * GitHub history for details.
// */
//
// package org.opensearch.ad;
//
// import static org.opensearch.action.DocWriteResponse.Result.CREATED;
// import static org.opensearch.action.DocWriteResponse.Result.UPDATED;
// import static org.opensearch.ad.AnomalyDetectorPlugin.AD_THREAD_POOL_NAME;
// import static org.opensearch.ad.constant.CommonErrorMessages.CAN_NOT_FIND_LATEST_TASK;
// import static org.opensearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
// import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
//
// import java.io.IOException;
// import java.time.Instant;
// import java.util.ArrayList;
// import java.util.HashSet;
// import java.util.List;
// import java.util.Set;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.concurrent.ExecutorService;
//
// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;
// import org.opensearch.action.ActionListener;
// import org.opensearch.action.get.GetRequest;
// import org.opensearch.action.get.GetResponse;
// import org.opensearch.action.index.IndexRequest;
// import org.opensearch.action.support.WriteRequest;
// import org.opensearch.ad.auth.UserIdentity;
// import org.opensearch.ad.common.exception.AnomalyDetectionException;
// import org.opensearch.ad.common.exception.EndRunException;
// import org.opensearch.ad.common.exception.InternalFailure;
// import org.opensearch.ad.common.exception.ResourceNotFoundException;
// import org.opensearch.ad.indices.ADIndex;
// import org.opensearch.ad.indices.AnomalyDetectionIndices;
// import org.opensearch.ad.model.ADTaskState;
// import org.opensearch.ad.model.AnomalyDetectorJob;
// import org.opensearch.ad.model.AnomalyResult;
// import org.opensearch.ad.model.DetectorProfileName;
// import org.opensearch.ad.model.FeatureData;
// import org.opensearch.ad.model.IntervalTimeConfiguration;
// import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
// import org.opensearch.ad.settings.AnomalyDetectorSettings;
// import org.opensearch.ad.task.ADTaskManager;
// import org.opensearch.ad.transport.AnomalyResultAction;
// import org.opensearch.ad.transport.AnomalyResultRequest;
// import org.opensearch.ad.transport.AnomalyResultResponse;
// import org.opensearch.ad.transport.AnomalyResultTransportAction;
// import org.opensearch.ad.transport.ProfileAction;
// import org.opensearch.ad.transport.ProfileRequest;
// import org.opensearch.ad.transport.handler.AnomalyIndexHandler;
// import org.opensearch.ad.util.DiscoveryNodeFilterer;
// import org.opensearch.client.Client;
// import org.opensearch.cluster.node.DiscoveryNode;
// import org.opensearch.common.settings.Settings;
// import org.opensearch.common.xcontent.LoggingDeprecationHandler;
// import org.opensearch.common.xcontent.NamedXContentRegistry;
// import org.opensearch.common.xcontent.XContentBuilder;
// import org.opensearch.common.xcontent.XContentParser;
// import org.opensearch.common.xcontent.XContentType;
// import org.opensearch.jobscheduler.spi.JobExecutionContext;
// import org.opensearch.jobscheduler.spi.LockModel;
// import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
// import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
// import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
// import org.opensearch.jobscheduler.spi.utils.LockService;
// import org.opensearch.threadpool.ThreadPool;
//
// import com.google.common.base.Throwables;
// import com.google.common.collect.ImmutableList;
//
/// **
// * JobScheduler will call AD job runner to get anomaly result periodically
// */
// public class AnomalyDetectorJobRunner implements ScheduledJobRunner {
// private static final Logger log = LogManager.getLogger(AnomalyDetectorJobRunner.class);
// private static AnomalyDetectorJobRunner INSTANCE;
// private Settings settings;
// private int maxRetryForEndRunException;
// private Client client;
// private ThreadPool threadPool;
// private AnomalyIndexHandler<AnomalyResult> anomalyResultHandler;
// private ConcurrentHashMap<String, Integer> detectorEndRunExceptionCount;
// private AnomalyDetectionIndices anomalyDetectionIndices;
// private DiscoveryNodeFilterer nodeFilter;
// private ADTaskManager adTaskManager;
//
// public static AnomalyDetectorJobRunner getJobRunnerInstance() {
// if (INSTANCE != null) {
// return INSTANCE;
// }
// synchronized (AnomalyDetectorJobRunner.class) {
// if (INSTANCE != null) {
// return INSTANCE;
// }
// INSTANCE = new AnomalyDetectorJobRunner();
// return INSTANCE;
// }
// }
//
// private AnomalyDetectorJobRunner() {
// // Singleton class, use getJobRunnerInstance method instead of constructor
// this.detectorEndRunExceptionCount = new ConcurrentHashMap<>();
// }
//
// public void setClient(Client client) {
// this.client = client;
// }
//
// public void setThreadPool(ThreadPool threadPool) {
// this.threadPool = threadPool;
// }
//
// public void setAnomalyResultHandler(AnomalyIndexHandler<AnomalyResult> anomalyResultHandler) {
// this.anomalyResultHandler = anomalyResultHandler;
// }
//
// public void setSettings(Settings settings) {
// this.settings = settings;
// this.maxRetryForEndRunException = AnomalyDetectorSettings.MAX_RETRY_FOR_END_RUN_EXCEPTION.get(settings);
// }
//
// public void setAdTaskManager(ADTaskManager adTaskManager) {
// this.adTaskManager = adTaskManager;
// }
//
// public void setAnomalyDetectionIndices(AnomalyDetectionIndices anomalyDetectionIndices) {
// this.anomalyDetectionIndices = anomalyDetectionIndices;
// }
//
// public void setNodeFilter(DiscoveryNodeFilterer nodeFilter) {
// this.nodeFilter = nodeFilter;
// }
//
// @Override
// public void runJob(ScheduledJobParameter scheduledJobParameter, JobExecutionContext context) {
// String detectorId = scheduledJobParameter.getName();
// log.info("Start to run AD job {}", detectorId);
// adTaskManager.refreshRealtimeJobRunTime(detectorId);
// if (!(scheduledJobParameter instanceof AnomalyDetectorJob)) {
// throw new IllegalArgumentException(
// "Job parameter is not instance of AnomalyDetectorJob, type: " + scheduledJobParameter.getClass().getCanonicalName()
// );
// }
// AnomalyDetectorJob jobParameter = (AnomalyDetectorJob) scheduledJobParameter;
// Instant executionStartTime = Instant.now();
// IntervalSchedule schedule = (IntervalSchedule) jobParameter.getSchedule();
// Instant detectionStartTime = executionStartTime.minus(schedule.getInterval(), schedule.getUnit());
//
// final LockService lockService = context.getLockService();
//
// Runnable runnable = () -> {
// if (jobParameter.getLockDurationSeconds() != null) {
// lockService
// .acquireLock(
// jobParameter,
// context,
// ActionListener
// .wrap(lock -> runAdJob(jobParameter, lockService, lock, detectionStartTime, executionStartTime), exception -> {
// indexAnomalyResultException(
// jobParameter,
// lockService,
// null,
// detectionStartTime,
// executionStartTime,
// exception,
// false
// );
// throw new IllegalStateException("Failed to acquire lock for AD job: " + detectorId);
// })
// );
// } else {
// log.warn("Can't get lock for AD job: " + detectorId);
// }
// };
//
// ExecutorService executor = threadPool.executor(AD_THREAD_POOL_NAME);
// executor.submit(runnable);
// }
//
// /**
// * Get anomaly result, index result or handle exception if failed.
// *
// * @param jobParameter scheduled job parameter
// * @param lockService lock service
// * @param lock lock to run job
// * @param detectionStartTime detection start time
// * @param executionStartTime detection end time
// */
// protected void runAdJob(
// AnomalyDetectorJob jobParameter,
// LockService lockService,
// LockModel lock,
// Instant detectionStartTime,
// Instant executionStartTime
// ) {
// String detectorId = jobParameter.getName();
// if (lock == null) {
// indexAnomalyResultException(
// jobParameter,
// lockService,
// lock,
// detectionStartTime,
// executionStartTime,
// "Can't run AD job due to null lock",
// false
// );
// return;
// }
// anomalyDetectionIndices.update();
//
// /*
// * We need to handle 3 cases:
// * 1. Detectors created by older versions and never updated. These detectors wont have User details in the
// * detector object. `detector.user` will be null. Insert `all_access, AmazonES_all_access` role.
// * 2. Detectors are created when security plugin is disabled, these will have empty User object.
// * (`detector.user.name`, `detector.user.roles` are empty )
// * 3. Detectors are created when security plugin is enabled, these will have an User object.
// * This will inject user role and check if the user role has permissions to call the execute
// * Anomaly Result API.
// */
// String user;
// List<String> roles;
// if (jobParameter.getUser() == null) {
// // It's possible that user create domain with security disabled, then enable security
// // after upgrading. This is for BWC, for old detectors which created when security
// // disabled, the user will be null.
// user = "";
// roles = settings.getAsList("", ImmutableList.of("all_access", "AmazonES_all_access"));
// } else {
// user = jobParameter.getUser().getName();
// roles = jobParameter.getUser().getRoles();
// }
// String resultIndex = jobParameter.getResultIndex();
// if (resultIndex == null) {
// runAnomalyDetectionJob(jobParameter, lockService, lock, detectionStartTime, executionStartTime, detectorId, user, roles);
// return;
// }
// ActionListener<Boolean> listener = ActionListener.wrap(r -> { log.debug("Custom index is valid"); }, e -> {
// Exception exception = new EndRunException(detectorId, e.getMessage(), true);
// handleAdException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, exception);
// });
// anomalyDetectionIndices.validateCustomIndexForBackendJob(resultIndex, detectorId, user, roles, () -> {
// listener.onResponse(true);
// runAnomalyDetectionJob(jobParameter, lockService, lock, detectionStartTime, executionStartTime, detectorId, user, roles);
// }, listener);
// }
//
// private void runAnomalyDetectionJob(
// AnomalyDetectorJob jobParameter,
// LockService lockService,
// LockModel lock,
// Instant detectionStartTime,
// Instant executionStartTime,
// String detectorId,
// String user,
// List<String> roles
// ) {
// try {
// AnomalyResultRequest request = new AnomalyResultRequest(
// detectorId,
// detectionStartTime.toEpochMilli(),
// executionStartTime.toEpochMilli()
// );
// client
// .execute(
// AnomalyResultAction.INSTANCE,
// request,
// ActionListener
// .wrap(
// response -> {
// indexAnomalyResult(jobParameter, lockService, lock, detectionStartTime, executionStartTime, response);
// },
// exception -> {
// handleAdException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, exception);
// }
// )
// );
// } catch (Exception e) {
// indexAnomalyResultException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, e, true);
// log.error("Failed to execute AD job " + detectorId, e);
// }
// }
//
// /**
// * Handle exception from anomaly result action.
// *
// * 1. If exception is {@link EndRunException}
// * a). if isEndNow == true, stop AD job and store exception in anomaly result
// * b). if isEndNow == false, record count of {@link EndRunException} for this
// * detector. If count of {@link EndRunException} exceeds upper limit, will
// * stop AD job and store exception in anomaly result; otherwise, just
// * store exception in anomaly result, not stop AD job for the detector.
// *
// * 2. If exception is not {@link EndRunException}, decrease count of
// * {@link EndRunException} for the detector and index eception in Anomaly
// * result. If exception is {@link InternalFailure}, will not log exception
// * stack trace as already logged in {@link AnomalyResultTransportAction}.
// *
// * TODO: Handle finer granularity exception such as some exception may be
// * transient and retry in current job may succeed. Currently, we don't
// * know which exception is transient and retryable in
// * {@link AnomalyResultTransportAction}. So we don't add backoff retry
// * now to avoid bring extra load to cluster, expecially the code start
// * process is relatively heavy by sending out 24 queries, initializing
// * models, and saving checkpoints.
// * Sometimes missing anomaly and notification is not acceptable. For example,
// * current detection interval is 1hour, and there should be anomaly in
// * current interval, some transient exception may fail current AD job,
// * so no anomaly found and user never know it. Then we start next AD job,
// * maybe there is no anomaly in next 1hour, user will never know something
// * wrong happened. In one word, this is some tradeoff between protecting
// * our performance, user experience and what we can do currently.
// *
// * @param jobParameter scheduled job parameter
// * @param lockService lock service
// * @param lock lock to run job
// * @param detectionStartTime detection start time
// * @param executionStartTime detection end time
// * @param exception exception
// */
// protected void handleAdException(
// AnomalyDetectorJob jobParameter,
// LockService lockService,
// LockModel lock,
// Instant detectionStartTime,
// Instant executionStartTime,
// Exception exception
// ) {
// String detectorId = jobParameter.getName();
// if (exception instanceof EndRunException) {
// log.error("EndRunException happened when executing anomaly result action for " + detectorId, exception);
//
// if (((EndRunException) exception).isEndNow()) {
// // Stop AD job if EndRunException shows we should end job now.
// log.info("JobRunner will stop AD job due to EndRunException for {}", detectorId);
// stopAdJobForEndRunException(
// jobParameter,
// lockService,
// lock,
// detectionStartTime,
// executionStartTime,
// (EndRunException) exception
// );
// } else {
// detectorEndRunExceptionCount.compute(detectorId, (k, v) -> {
// if (v == null) {
// return 1;
// } else {
// return v + 1;
// }
// });
// log.info("EndRunException happened for {}", detectorId);
// // if AD job failed consecutively due to EndRunException and failed times exceeds upper limit, will stop AD job
// if (detectorEndRunExceptionCount.get(detectorId) > maxRetryForEndRunException) {
// log
// .info(
// "JobRunner will stop AD job due to EndRunException retry exceeds upper limit {} for {}",
// maxRetryForEndRunException,
// detectorId
// );
// stopAdJobForEndRunException(
// jobParameter,
// lockService,
// lock,
// detectionStartTime,
// executionStartTime,
// (EndRunException) exception
// );
// return;
// }
// indexAnomalyResultException(
// jobParameter,
// lockService,
// lock,
// detectionStartTime,
// executionStartTime,
// exception.getMessage(),
// true
// );
// }
// } else {
// detectorEndRunExceptionCount.remove(detectorId);
// if (exception instanceof InternalFailure) {
// log.error("InternalFailure happened when executing anomaly result action for " + detectorId, exception);
// } else {
// log.error("Failed to execute anomaly result action for " + detectorId, exception);
// }
// indexAnomalyResultException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, exception, true);
// }
// }
//
// private void stopAdJobForEndRunException(
// AnomalyDetectorJob jobParameter,
// LockService lockService,
// LockModel lock,
// Instant detectionStartTime,
// Instant executionStartTime,
// EndRunException exception
// ) {
// String detectorId = jobParameter.getName();
// detectorEndRunExceptionCount.remove(detectorId);
// String errorPrefix = exception.isEndNow()
// ? "Stopped detector: "
// : "Stopped detector as job failed consecutively for more than " + this.maxRetryForEndRunException + " times: ";
// String error = errorPrefix + exception.getMessage();
// stopAdJob(
// detectorId,
// () -> indexAnomalyResultException(
// jobParameter,
// lockService,
// lock,
// detectionStartTime,
// executionStartTime,
// error,
// true,
// ADTaskState.STOPPED.name()
// )
// );
// }
//
// private void stopAdJob(String detectorId, AnomalyDetectorFunction function) {
// GetRequest getRequest = new GetRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX).id(detectorId);
// ActionListener<GetResponse> listener = ActionListener.wrap(response -> {
// if (response.isExists()) {
// try (
// XContentParser parser = XContentType.JSON
// .xContent()
// .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, response.getSourceAsString())
// ) {
// ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
// AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
// if (job.isEnabled()) {
// AnomalyDetectorJob newJob = new AnomalyDetectorJob(
// job.getName(),
// job.getSchedule(),
// job.getWindowDelay(),
// false,
// job.getEnabledTime(),
// Instant.now(),
// Instant.now(),
// job.getLockDurationSeconds(),
// job.getUser(),
// job.getResultIndex()
// );
// IndexRequest indexRequest = new IndexRequest(AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX)
// .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
// .source(newJob.toXContent(XContentBuilder.builder(XContentType.JSON.xContent()), XCONTENT_WITH_TYPE))
// .id(detectorId);
//
// client.index(indexRequest, ActionListener.wrap(indexResponse -> {
// if (indexResponse != null && (indexResponse.getResult() == CREATED || indexResponse.getResult() == UPDATED)) {
// log.info("AD Job was disabled by JobRunner for " + detectorId);
// // function.execute();
// } else {
// log.warn("Failed to disable AD job for " + detectorId);
// }
// }, exception -> { log.error("JobRunner failed to update AD job as disabled for " + detectorId, exception); }));
// } else {
// log.info("AD Job was disabled for " + detectorId);
// // function.execute();
// }
// } catch (IOException e) {
// log.error("JobRunner failed to stop detector job " + detectorId, e);
// }
// } else {
// log.info("AD Job was not found for " + detectorId);
// // function.execute();
// }
// }, exception -> log.error("JobRunner failed to get detector job " + detectorId, exception));
//
// client.get(getRequest, ActionListener.runAfter(listener, () -> function.execute()));
// }
//
// private void indexAnomalyResult(
// AnomalyDetectorJob jobParameter,
// LockService lockService,
// LockModel lock,
// Instant detectionStartTime,
// Instant executionStartTime,
// AnomalyResultResponse response
// ) {
// String detectorId = jobParameter.getName();
// detectorEndRunExceptionCount.remove(detectorId);
// try {
// // skipping writing to the result index if not necessary
// // For a single-entity detector, the result is not useful if error is null
// // and rcf score (thus anomaly grade/confidence) is null.
// // For a HCAD detector, we don't need to save on the detector level.
// // We return 0 or Double.NaN rcf score if there is no error.
// if ((response.getAnomalyScore() <= 0 || Double.isNaN(response.getAnomalyScore())) && response.getError() == null) {
// updateRealtimeTask(response, detectorId);
// return;
// }
// IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) jobParameter.getWindowDelay();
// Instant dataStartTime = detectionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
// Instant dataEndTime = executionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
// UserIdentity user = jobParameter.getUser();
//
// if (response.getError() != null) {
// log.info("Anomaly result action run successfully for {} with error {}", detectorId, response.getError());
// }
//
// AnomalyResult anomalyResult = response
// .toAnomalyResult(
// detectorId,
// dataStartTime,
// dataEndTime,
// executionStartTime,
// Instant.now(),
// anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT),
// user,
// response.getError()
// );
//
// String resultIndex = jobParameter.getResultIndex();
// anomalyResultHandler.index(anomalyResult, detectorId, resultIndex);
// updateRealtimeTask(response, detectorId);
// } catch (EndRunException e) {
// handleAdException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, e);
// } catch (Exception e) {
// log.error("Failed to index anomaly result for " + detectorId, e);
// } finally {
// releaseLock(jobParameter, lockService, lock);
// }
// }
//
// private void updateRealtimeTask(AnomalyResultResponse response, String detectorId) {
// if (response.isHCDetector() != null
// && response.isHCDetector()
// && !adTaskManager.skipUpdateHCRealtimeTask(detectorId, response.getError())) {
// DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
// Set<DetectorProfileName> profiles = new HashSet<>();
// profiles.add(DetectorProfileName.INIT_PROGRESS);
// ProfileRequest profileRequest = new ProfileRequest(detectorId, profiles, true, dataNodes);
// client.execute(ProfileAction.INSTANCE, profileRequest, ActionListener.wrap(r -> {
// log.debug("Update latest realtime task for HC detector {}, total updates: {}", detectorId, r.getTotalUpdates());
// updateLatestRealtimeTask(
// detectorId,
// null,
// r.getTotalUpdates(),
// response.getDetectorIntervalInMinutes(),
// response.getError()
// );
// }, e -> { log.error("Failed to update latest realtime task for " + detectorId, e); }));
// } else {
// log.debug("Update latest realtime task for SINGLE detector {}, total updates: {}", detectorId, response.getRcfTotalUpdates());
// updateLatestRealtimeTask(
// detectorId,
// null,
// response.getRcfTotalUpdates(),
// response.getDetectorIntervalInMinutes(),
// response.getError()
// );
// }
// }
//
// private void indexAnomalyResultException(
// AnomalyDetectorJob jobParameter,
// LockService lockService,
// LockModel lock,
// Instant detectionStartTime,
// Instant executionStartTime,
// Exception exception,
// boolean releaseLock
// ) {
// try {
// String errorMessage = exception instanceof AnomalyDetectionException
// ? exception.getMessage()
// : Throwables.getStackTraceAsString(exception);
// indexAnomalyResultException(jobParameter, lockService, lock, detectionStartTime, executionStartTime, errorMessage, releaseLock);
// } catch (Exception e) {
// log.error("Failed to index anomaly result for " + jobParameter.getName(), e);
// }
// }
//
// private void indexAnomalyResultException(
// AnomalyDetectorJob jobParameter,
// LockService lockService,
// LockModel lock,
// Instant detectionStartTime,
// Instant executionStartTime,
// String errorMessage,
// boolean releaseLock
// ) {
// indexAnomalyResultException(
// jobParameter,
// lockService,
// lock,
// detectionStartTime,
// executionStartTime,
// errorMessage,
// releaseLock,
// null
// );
// }
//
// private void indexAnomalyResultException(
// AnomalyDetectorJob jobParameter,
// LockService lockService,
// LockModel lock,
// Instant detectionStartTime,
// Instant executionStartTime,
// String errorMessage,
// boolean releaseLock,
// String taskState
// ) {
// String detectorId = jobParameter.getName();
// try {
// IntervalTimeConfiguration windowDelay = (IntervalTimeConfiguration) jobParameter.getWindowDelay();
// Instant dataStartTime = detectionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
// Instant dataEndTime = executionStartTime.minus(windowDelay.getInterval(), windowDelay.getUnit());
// UserIdentity user = jobParameter.getUser();
//
// AnomalyResult anomalyResult = new AnomalyResult(
// detectorId,
// null, // no task id
// new ArrayList<FeatureData>(),
// dataStartTime,
// dataEndTime,
// executionStartTime,
// Instant.now(),
// errorMessage,
// null, // single-stream detectors have no entity
// user,
// anomalyDetectionIndices.getSchemaVersion(ADIndex.RESULT),
// null // no model id
// );
// String resultIndex = jobParameter.getResultIndex();
// if (resultIndex != null && !anomalyDetectionIndices.doesIndexExist(resultIndex)) {
// // Set result index as null, will write exception to default result index.
// anomalyResultHandler.index(anomalyResult, detectorId, null);
// } else {
// anomalyResultHandler.index(anomalyResult, detectorId, resultIndex);
// }
//
// updateLatestRealtimeTask(detectorId, taskState, null, null, errorMessage);
// } catch (Exception e) {
// log.error("Failed to index anomaly result for " + detectorId, e);
// } finally {
// if (releaseLock) {
// releaseLock(jobParameter, lockService, lock);
// }
// }
// }
//
// private void updateLatestRealtimeTask(
// String detectorId,
// String taskState,
// Long rcfTotalUpdates,
// Long detectorIntervalInMinutes,
// String error
// ) {
// // Don't need info as this will be printed repeatedly in each interval
// adTaskManager
// .updateLatestRealtimeTaskOnCoordinatingNode(
// detectorId,
// taskState,
// rcfTotalUpdates,
// detectorIntervalInMinutes,
// error,
// ActionListener.wrap(r -> {
// if (r != null) {
// log.debug("Updated latest realtime task successfully for detector {}, taskState: {}", detectorId, taskState);
// }
// }, e -> {
// if ((e instanceof ResourceNotFoundException) && e.getMessage().contains(CAN_NOT_FIND_LATEST_TASK)) {
// // Clear realtime task cache, will recreate AD task in next run, check AnomalyResultTransportAction.
// log.error("Can't find latest realtime task of detector " + detectorId);
// adTaskManager.removeRealtimeTaskCache(detectorId);
// } else {
// log.error("Failed to update latest realtime task for detector " + detectorId, e);
// }
// })
// );
// }
//
// private void releaseLock(AnomalyDetectorJob jobParameter, LockService lockService, LockModel lock) {
// lockService
// .release(
// lock,
// ActionListener
// .wrap(
// released -> { log.info("Released lock for AD job {}", jobParameter.getName()); },
// exception -> { log.error("Failed to release lock for AD job: " + jobParameter.getName(), exception); }
// )
// );
// }
// }
