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
// package org.opensearch.ad.cluster;
//
// import static org.opensearch.ad.constant.CommonName.DETECTION_STATE_INDEX;
// import static org.opensearch.ad.model.ADTask.DETECTOR_ID_FIELD;
// import static org.opensearch.ad.model.ADTask.IS_LATEST_FIELD;
// import static org.opensearch.ad.model.ADTask.TASK_TYPE_FIELD;
// import static org.opensearch.ad.model.ADTaskType.taskTypeToString;
// import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
//// import static org.opensearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
// import static org.opensearch.ad.settings.AnomalyDetectorSettings.MAX_DETECTOR_UPPER_LIMIT;
// import static org.opensearch.ad.util.RestHandlerUtils.XCONTENT_WITH_TYPE;
// import static org.opensearch.ad.util.RestHandlerUtils.createXContentParserFromRegistry;
// import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
//
// import java.io.IOException;
// import java.time.Instant;
// import java.util.Iterator;
// import java.util.concurrent.ConcurrentLinkedQueue;
// import java.util.concurrent.atomic.AtomicBoolean;
//
// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;
// import org.opensearch.ExceptionsHelper;
// import org.opensearch.ResourceAlreadyExistsException;
// import org.opensearch.action.ActionListener;
// import org.opensearch.action.get.GetRequest;
// import org.opensearch.action.index.IndexRequest;
// import org.opensearch.action.search.SearchRequest;
// import org.opensearch.action.support.WriteRequest;
// import org.opensearch.ad.common.exception.ResourceNotFoundException;
// import org.opensearch.ad.constant.CommonName;
// import org.opensearch.ad.indices.AnomalyDetectionIndices;
// import org.opensearch.ad.model.ADTask;
// import org.opensearch.ad.model.ADTaskState;
// import org.opensearch.ad.model.ADTaskType;
// import org.opensearch.ad.model.AnomalyDetector;
//// import org.opensearch.ad.model.AnomalyDetectorJob;
// import org.opensearch.ad.model.DetectorInternalState;
// import org.opensearch.ad.rest.handler.AnomalyDetectorFunction;
// import org.opensearch.ad.util.ExceptionUtil;
// import org.opensearch.client.Client;
// import org.opensearch.cluster.service.ClusterService;
// import org.opensearch.common.xcontent.NamedXContentRegistry;
// import org.opensearch.common.xcontent.XContentFactory;
// import org.opensearch.common.xcontent.XContentParser;
// import org.opensearch.index.IndexNotFoundException;
// import org.opensearch.index.query.BoolQueryBuilder;
// import org.opensearch.index.query.MatchAllQueryBuilder;
// import org.opensearch.index.query.TermQueryBuilder;
// import org.opensearch.index.query.TermsQueryBuilder;
// import org.opensearch.search.SearchHit;
// import org.opensearch.search.builder.SearchSourceBuilder;
//
/// **
// * Migrate AD data to support backward compatibility.
// * Currently we need to migrate:
// * 1. Detector internal state (used to track realtime job error) to realtime data.
// */
// public class ADDataMigrator {
// private final Logger logger = LogManager.getLogger(this.getClass());
// private final Client client;
// private final ClusterService clusterService;
// private final NamedXContentRegistry xContentRegistry;
// private final AnomalyDetectionIndices detectionIndices;
// private final AtomicBoolean dataMigrated;
//
// public ADDataMigrator(
// Client client,
// ClusterService clusterService,
// NamedXContentRegistry xContentRegistry,
// AnomalyDetectionIndices detectionIndices
// ) {
// this.client = client;
// this.clusterService = clusterService;
// this.xContentRegistry = xContentRegistry;
// this.detectionIndices = detectionIndices;
// this.dataMigrated = new AtomicBoolean(false);
// }
//
// /**
// * Migrate AD data. Currently only need to migrate detector internal state {@link DetectorInternalState}
// */
// public void migrateData() {
// if (!dataMigrated.getAndSet(true)) {
// logger.info("Start migrating AD data");
//
// if (!detectionIndices.doesAnomalyDetectorJobIndexExist()) {
// logger.info("AD job index doesn't exist, no need to migrate");
// return;
// }
//
// if (detectionIndices.doesDetectorStateIndexExist()) {
// migrateDetectorInternalStateToRealtimeTask();
// } else {
// // If detection index doesn't exist, create index and backfill realtime task.
// detectionIndices.initDetectionStateIndex(ActionListener.wrap(r -> {
// if (r.isAcknowledged()) {
// logger.info("Created {} with mappings.", CommonName.DETECTION_STATE_INDEX);
// migrateDetectorInternalStateToRealtimeTask();
// } else {
// String error = "Create index " + CommonName.DETECTION_STATE_INDEX + " with mappings not acknowledged";
// logger.warn(error);
// }
// }, e -> {
// if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
// // When migrate data, it's possible that user run some historical analysis and it will create detection
// // state index. Then we will see ResourceAlreadyExistsException.
// migrateDetectorInternalStateToRealtimeTask();
// } else {
// logger.error("Failed to init anomaly detection state index", e);
// }
// }));
// }
// }
// }
//
// /**
// * Migrate detector internal state to realtime task.
// */
// public void migrateDetectorInternalStateToRealtimeTask() {
// SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
// .query(new MatchAllQueryBuilder())
// .size(MAX_DETECTOR_UPPER_LIMIT);
// SearchRequest searchRequest = new SearchRequest(ANOMALY_DETECTOR_JOB_INDEX).source(searchSourceBuilder);
// client.search(searchRequest, ActionListener.wrap(r -> {
// if (r == null || r.getHits().getTotalHits() == null || r.getHits().getTotalHits().value == 0) {
// logger.info("No anomaly detector job found, no need to migrate");
// return;
// }
// ConcurrentLinkedQueue<AnomalyDetectorJob> detectorJobs = new ConcurrentLinkedQueue<>();
// Iterator<SearchHit> iterator = r.getHits().iterator();
// while (iterator.hasNext()) {
// SearchHit searchHit = iterator.next();
// try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, searchHit.getSourceRef())) {
// ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
// AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
// detectorJobs.add(job);
// } catch (IOException e) {
// logger.error("Fail to parse AD job " + searchHit.getId(), e);
// }
// }
// logger.info("Total AD jobs to backfill realtime task: {}", detectorJobs.size());
// backfillRealtimeTask(detectorJobs, true);
// }, e -> {
// if (ExceptionUtil.getErrorMessage(e).contains("all shards failed")) {
// // This error may happen when AD job index not ready for query as some nodes not in cluster yet.
// // Will recreate realtime task when AD job starts.
// logger.warn("No available shards of AD job index, reset dataMigrated as false");
// this.dataMigrated.set(false);
// } else if (!(e instanceof IndexNotFoundException)) {
// logger.error("Failed to migrate AD data", e);
// }
// }));
// }
//
// /**
// * Backfill realtiem task for realtime job.
// * @param detectorJobs realtime AD jobs
// * @param backfillAllJob backfill task for all realtime job or not
// */
// public void backfillRealtimeTask(ConcurrentLinkedQueue<AnomalyDetectorJob> detectorJobs, boolean backfillAllJob) {
// AnomalyDetectorJob job = detectorJobs.poll();
// if (job == null) {
// logger.info("AD data migration done.");
// if (backfillAllJob) {
// this.dataMigrated.set(true);
// }
// return;
// }
// String jobId = job.getName();
//
// AnomalyDetectorFunction createRealtimeTaskFunction = () -> {
// GetRequest getRequest = new GetRequest(DETECTION_STATE_INDEX, jobId);
// client.get(getRequest, ActionListener.wrap(r -> {
// if (r != null && r.isExists()) {
// try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
// ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
// DetectorInternalState detectorState = DetectorInternalState.parse(parser);
// createRealtimeADTask(job, detectorState.getError(), detectorJobs, backfillAllJob);
// } catch (IOException e) {
// logger.error("Failed to parse detector internal state " + jobId, e);
// createRealtimeADTask(job, null, detectorJobs, backfillAllJob);
// }
// } else {
// createRealtimeADTask(job, null, detectorJobs, backfillAllJob);
// }
// }, e -> {
// logger.error("Failed to query detector internal state " + jobId, e);
// createRealtimeADTask(job, null, detectorJobs, backfillAllJob);
// }));
// };
// checkIfRealtimeTaskExistsAndBackfill(job, createRealtimeTaskFunction, detectorJobs, backfillAllJob);
// }
//
// private void checkIfRealtimeTaskExistsAndBackfill(
// AnomalyDetectorJob job,
// AnomalyDetectorFunction createRealtimeTaskFunction,
// ConcurrentLinkedQueue<AnomalyDetectorJob> detectorJobs,
// boolean migrateAll
// ) {
// String jobId = job.getName();
// BoolQueryBuilder query = new BoolQueryBuilder();
// query.filter(new TermQueryBuilder(DETECTOR_ID_FIELD, jobId));
// if (job.isEnabled()) {
// query.filter(new TermQueryBuilder(IS_LATEST_FIELD, true));
// }
//
// query.filter(new TermsQueryBuilder(TASK_TYPE_FIELD, taskTypeToString(ADTaskType.REALTIME_TASK_TYPES)));
// SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(query).size(1);
// SearchRequest searchRequest = new SearchRequest(DETECTION_STATE_INDEX).source(searchSourceBuilder);
// client.search(searchRequest, ActionListener.wrap(r -> {
// if (r != null && r.getHits().getTotalHits().value > 0) {
// // Backfill next realtime job
// backfillRealtimeTask(detectorJobs, migrateAll);
// return;
// }
// createRealtimeTaskFunction.execute();
// }, e -> {
// if (e instanceof ResourceNotFoundException) {
// createRealtimeTaskFunction.execute();
// }
// logger.error("Failed to search tasks of detector " + jobId);
// }));
// }
//
// private void createRealtimeADTask(
// AnomalyDetectorJob job,
// String error,
// ConcurrentLinkedQueue<AnomalyDetectorJob> detectorJobs,
// boolean migrateAll
// ) {
// client.get(new GetRequest(ANOMALY_DETECTORS_INDEX, job.getName()), ActionListener.wrap(r -> {
// if (r != null && r.isExists()) {
// try (XContentParser parser = createXContentParserFromRegistry(xContentRegistry, r.getSourceAsBytesRef())) {
// ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
// AnomalyDetector detector = AnomalyDetector.parse(parser, r.getId());
// ADTaskType taskType = detector.isMultientityDetector()
// ? ADTaskType.REALTIME_HC_DETECTOR
// : ADTaskType.REALTIME_SINGLE_ENTITY;
// Instant now = Instant.now();
// String userName = job.getUser() != null ? job.getUser().getName() : null;
// ADTask adTask = new ADTask.Builder()
// .detectorId(detector.getDetectorId())
// .detector(detector)
// .error(error)
// .isLatest(true)
// .taskType(taskType.name())
// .executionStartTime(now)
// .taskProgress(0.0f)
// .initProgress(0.0f)
// .state(ADTaskState.CREATED.name())
// .lastUpdateTime(now)
// .startedBy(userName)
// .coordinatingNode(null)
// .detectionDateRange(null)
// .user(job.getUser())
// .build();
// IndexRequest indexRequest = new IndexRequest(DETECTION_STATE_INDEX)
// .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
// .source(adTask.toXContent(XContentFactory.jsonBuilder(), XCONTENT_WITH_TYPE));
// client.index(indexRequest, ActionListener.wrap(indexResponse -> {
// logger.info("Backfill realtime task successfully for detector {}", job.getName());
// backfillRealtimeTask(detectorJobs, migrateAll);
// }, ex -> {
// logger.error("Failed to backfill realtime task for detector " + job.getName(), ex);
// backfillRealtimeTask(detectorJobs, migrateAll);
// }));
// } catch (IOException e) {
// logger.error("Fail to parse detector " + job.getName(), e);
// backfillRealtimeTask(detectorJobs, migrateAll);
// }
// } else {
// logger.error("Detector doesn't exist " + job.getName());
// backfillRealtimeTask(detectorJobs, migrateAll);
// }
// }, e -> {
// logger.error("Fail to get detector " + job.getName(), e);
// backfillRealtimeTask(detectorJobs, migrateAll);
// }));
// }
//
// public void skipMigration() {
// this.dataMigrated.set(true);
// }
//
// public boolean isMigrated() {
// return this.dataMigrated.get();
// }
// }
