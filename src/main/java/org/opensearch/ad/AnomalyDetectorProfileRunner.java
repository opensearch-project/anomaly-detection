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
// import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG;
// import static org.opensearch.ad.constant.CommonErrorMessages.FAIL_TO_PARSE_DETECTOR_MSG;
// import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
// import static org.opensearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
// import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
// import static org.opensearch.rest.RestStatus.BAD_REQUEST;
// import static org.opensearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
//
// import java.util.List;
// import java.util.Map;
// import java.util.Set;
// import java.util.stream.Collectors;
//
// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;
// import org.apache.logging.log4j.core.util.Throwables;
// import org.apache.logging.log4j.message.ParameterizedMessage;
// import org.opensearch.OpenSearchStatusException;
// import org.opensearch.action.ActionListener;
// import org.opensearch.action.get.GetRequest;
// import org.opensearch.action.search.SearchRequest;
// import org.opensearch.action.search.SearchResponse;
// import org.opensearch.ad.common.exception.NotSerializedADExceptionName;
// import org.opensearch.ad.common.exception.ResourceNotFoundException;
// import org.opensearch.ad.constant.CommonErrorMessages;
// import org.opensearch.ad.constant.CommonName;
// import org.opensearch.ad.model.ADTaskType;
// import org.opensearch.ad.model.AnomalyDetector;
// import org.opensearch.ad.model.AnomalyDetectorJob;
// import org.opensearch.ad.model.AnomalyResult;
// import org.opensearch.ad.model.DetectorProfile;
// import org.opensearch.ad.model.DetectorProfileName;
// import org.opensearch.ad.model.DetectorState;
// import org.opensearch.ad.model.InitProgressProfile;
// import org.opensearch.ad.model.IntervalTimeConfiguration;
// import org.opensearch.ad.settings.AnomalyDetectorSettings;
// import org.opensearch.ad.settings.NumericSetting;
// import org.opensearch.ad.task.ADTaskManager;
// import org.opensearch.ad.transport.ProfileAction;
// import org.opensearch.ad.transport.ProfileRequest;
// import org.opensearch.ad.transport.ProfileResponse;
// import org.opensearch.ad.transport.RCFPollingAction;
// import org.opensearch.ad.transport.RCFPollingRequest;
// import org.opensearch.ad.transport.RCFPollingResponse;
// import org.opensearch.ad.util.DiscoveryNodeFilterer;
// import org.opensearch.ad.util.ExceptionUtil;
// import org.opensearch.ad.util.MultiResponsesDelegateActionListener;
// import org.opensearch.client.Client;
// import org.opensearch.cluster.node.DiscoveryNode;
// import org.opensearch.common.xcontent.LoggingDeprecationHandler;
// import org.opensearch.common.xcontent.NamedXContentRegistry;
// import org.opensearch.common.xcontent.XContentParser;
// import org.opensearch.common.xcontent.XContentType;
// import org.opensearch.index.query.BoolQueryBuilder;
// import org.opensearch.index.query.QueryBuilders;
// import org.opensearch.search.SearchHits;
// import org.opensearch.search.aggregations.Aggregation;
// import org.opensearch.search.aggregations.AggregationBuilder;
// import org.opensearch.search.aggregations.AggregationBuilders;
// import org.opensearch.search.aggregations.Aggregations;
// import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
// import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
// import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
// import org.opensearch.search.aggregations.metrics.InternalCardinality;
// import org.opensearch.search.builder.SearchSourceBuilder;
// import org.opensearch.transport.TransportService;
//
// public class AnomalyDetectorProfileRunner extends AbstractProfileRunner {
// private final Logger logger = LogManager.getLogger(AnomalyDetectorProfileRunner.class);
// private Client client;
// private NamedXContentRegistry xContentRegistry;
// private DiscoveryNodeFilterer nodeFilter;
// private final TransportService transportService;
// private final ADTaskManager adTaskManager;
// private final int maxTotalEntitiesToTrack;
//
// public AnomalyDetectorProfileRunner(
// Client client,
// NamedXContentRegistry xContentRegistry,
// DiscoveryNodeFilterer nodeFilter,
// long requiredSamples,
// TransportService transportService,
// ADTaskManager adTaskManager
// ) {
// super(requiredSamples);
// this.client = client;
// this.xContentRegistry = xContentRegistry;
// this.nodeFilter = nodeFilter;
// if (requiredSamples <= 0) {
// throw new IllegalArgumentException("required samples should be a positive number, but was " + requiredSamples);
// }
// this.transportService = transportService;
// this.adTaskManager = adTaskManager;
// this.maxTotalEntitiesToTrack = AnomalyDetectorSettings.MAX_TOTAL_ENTITIES_TO_TRACK;
// }
//
// public void profile(String detectorId, ActionListener<DetectorProfile> listener, Set<DetectorProfileName> profilesToCollect) {
// if (profilesToCollect.isEmpty()) {
// listener.onFailure(new IllegalArgumentException(CommonErrorMessages.EMPTY_PROFILES_COLLECT));
// return;
// }
// calculateTotalResponsesToWait(detectorId, profilesToCollect, listener);
// }
//
// private void calculateTotalResponsesToWait(
// String detectorId,
// Set<DetectorProfileName> profilesToCollect,
// ActionListener<DetectorProfile> listener
// ) {
// GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
// client.get(getDetectorRequest, ActionListener.wrap(getDetectorResponse -> {
// if (getDetectorResponse != null && getDetectorResponse.isExists()) {
// try (
// XContentParser xContentParser = XContentType.JSON
// .xContent()
// .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getDetectorResponse.getSourceAsString())
// ) {
// ensureExpectedToken(XContentParser.Token.START_OBJECT, xContentParser.nextToken(), xContentParser);
// AnomalyDetector detector = AnomalyDetector.parse(xContentParser, detectorId);
// prepareProfile(detector, listener, profilesToCollect);
// } catch (Exception e) {
// logger.error(FAIL_TO_PARSE_DETECTOR_MSG + detectorId, e);
// listener.onFailure(new OpenSearchStatusException(FAIL_TO_PARSE_DETECTOR_MSG + detectorId, BAD_REQUEST));
// }
// } else {
// listener.onFailure(new OpenSearchStatusException(FAIL_TO_FIND_DETECTOR_MSG + detectorId, BAD_REQUEST));
// }
// }, exception -> {
// logger.error(FAIL_TO_FIND_DETECTOR_MSG + detectorId, exception);
// listener.onFailure(new OpenSearchStatusException(FAIL_TO_FIND_DETECTOR_MSG + detectorId, INTERNAL_SERVER_ERROR));
// }));
// }
//
// private void prepareProfile(
// AnomalyDetector detector,
// ActionListener<DetectorProfile> listener,
// Set<DetectorProfileName> profilesToCollect
// ) {
// String detectorId = detector.getDetectorId();
// GetRequest getRequest = new GetRequest(ANOMALY_DETECTOR_JOB_INDEX, detectorId);
// client.get(getRequest, ActionListener.wrap(getResponse -> {
// if (getResponse != null && getResponse.isExists()) {
// try (
// XContentParser parser = XContentType.JSON
// .xContent()
// .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
// ) {
// ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
// AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);
// long enabledTimeMs = job.getEnabledTime().toEpochMilli();
//
// boolean isMultiEntityDetector = detector.isMultientityDetector();
//
// int totalResponsesToWait = 0;
// if (profilesToCollect.contains(DetectorProfileName.ERROR)) {
// totalResponsesToWait++;
// }
//
// // total number of listeners we need to define. Needed by MultiResponsesDelegateActionListener to decide
// // when to consolidate results and return to users
// if (isMultiEntityDetector) {
// if (profilesToCollect.contains(DetectorProfileName.TOTAL_ENTITIES)) {
// totalResponsesToWait++;
// }
// if (profilesToCollect.contains(DetectorProfileName.COORDINATING_NODE)
// || profilesToCollect.contains(DetectorProfileName.SHINGLE_SIZE)
// || profilesToCollect.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES)
// || profilesToCollect.contains(DetectorProfileName.MODELS)
// || profilesToCollect.contains(DetectorProfileName.ACTIVE_ENTITIES)
// || profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)
// || profilesToCollect.contains(DetectorProfileName.STATE)) {
// totalResponsesToWait++;
// }
// if (profilesToCollect.contains(DetectorProfileName.AD_TASK)) {
// totalResponsesToWait++;
// }
// } else {
// if (profilesToCollect.contains(DetectorProfileName.STATE)
// || profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)) {
// totalResponsesToWait++;
// }
// if (profilesToCollect.contains(DetectorProfileName.COORDINATING_NODE)
// || profilesToCollect.contains(DetectorProfileName.SHINGLE_SIZE)
// || profilesToCollect.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES)
// || profilesToCollect.contains(DetectorProfileName.MODELS)) {
// totalResponsesToWait++;
// }
// if (profilesToCollect.contains(DetectorProfileName.AD_TASK)) {
// totalResponsesToWait++;
// }
// }
//
// MultiResponsesDelegateActionListener<DetectorProfile> delegateListener =
// new MultiResponsesDelegateActionListener<DetectorProfile>(
// listener,
// totalResponsesToWait,
// CommonErrorMessages.FAIL_FETCH_ERR_MSG + detectorId,
// false
// );
// if (profilesToCollect.contains(DetectorProfileName.ERROR)) {
// adTaskManager.getAndExecuteOnLatestDetectorLevelTask(detectorId, ADTaskType.REALTIME_TASK_TYPES, adTask -> {
// DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
// if (adTask.isPresent()) {
// long lastUpdateTimeMs = adTask.get().getLastUpdateTime().toEpochMilli();
//
// // if state index hasn't been updated, we should not use the error field
// // For example, before a detector is enabled, if the error message contains
// // the phrase "stopped due to blah", we should not show this when the detector
// // is enabled.
// if (lastUpdateTimeMs > enabledTimeMs && adTask.get().getError() != null) {
// profileBuilder.error(adTask.get().getError());
// }
// delegateListener.onResponse(profileBuilder.build());
// } else {
// // detector state for this detector does not exist
// delegateListener.onResponse(profileBuilder.build());
// }
// }, transportService, false, delegateListener);
// }
//
// // total number of listeners we need to define. Needed by MultiResponsesDelegateActionListener to decide
// // when to consolidate results and return to users
// if (isMultiEntityDetector) {
// if (profilesToCollect.contains(DetectorProfileName.TOTAL_ENTITIES)) {
// profileEntityStats(delegateListener, detector);
// }
// if (profilesToCollect.contains(DetectorProfileName.COORDINATING_NODE)
// || profilesToCollect.contains(DetectorProfileName.SHINGLE_SIZE)
// || profilesToCollect.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES)
// || profilesToCollect.contains(DetectorProfileName.MODELS)
// || profilesToCollect.contains(DetectorProfileName.ACTIVE_ENTITIES)
// || profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)
// || profilesToCollect.contains(DetectorProfileName.STATE)) {
// profileModels(detector, profilesToCollect, job, true, delegateListener);
// }
// if (profilesToCollect.contains(DetectorProfileName.AD_TASK)) {
// adTaskManager.getLatestHistoricalTaskProfile(detectorId, transportService, null, delegateListener);
// }
// } else {
// if (profilesToCollect.contains(DetectorProfileName.STATE)
// || profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)) {
// profileStateRelated(detector, delegateListener, job.isEnabled(), profilesToCollect);
// }
// if (profilesToCollect.contains(DetectorProfileName.COORDINATING_NODE)
// || profilesToCollect.contains(DetectorProfileName.SHINGLE_SIZE)
// || profilesToCollect.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES)
// || profilesToCollect.contains(DetectorProfileName.MODELS)) {
// profileModels(detector, profilesToCollect, job, false, delegateListener);
// }
// if (profilesToCollect.contains(DetectorProfileName.AD_TASK)) {
// adTaskManager.getLatestHistoricalTaskProfile(detectorId, transportService, null, delegateListener);
// }
// }
//
// } catch (Exception e) {
// logger.error(CommonErrorMessages.FAIL_TO_GET_PROFILE_MSG, e);
// listener.onFailure(e);
// }
// } else {
// onGetDetectorForPrepare(detectorId, listener, profilesToCollect);
// }
// }, exception -> {
// if (ExceptionUtil.isIndexNotAvailable(exception)) {
// logger.info(exception.getMessage());
// onGetDetectorForPrepare(detectorId, listener, profilesToCollect);
// } else {
// logger.error(CommonErrorMessages.FAIL_TO_GET_PROFILE_MSG + detectorId);
// listener.onFailure(exception);
// }
// }));
// }
//
// private void profileEntityStats(MultiResponsesDelegateActionListener<DetectorProfile> listener, AnomalyDetector detector) {
// List<String> categoryField = detector.getCategoryField();
// if (!detector.isMultientityDetector() || categoryField.size() > NumericSetting.maxCategoricalFields()) {
// listener.onResponse(new DetectorProfile.Builder().build());
// } else {
// if (categoryField.size() == 1) {
// // Run a cardinality aggregation to count the cardinality of single category fields
// SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
// CardinalityAggregationBuilder aggBuilder = new CardinalityAggregationBuilder(CommonName.TOTAL_ENTITIES);
// aggBuilder.field(categoryField.get(0));
// searchSourceBuilder.aggregation(aggBuilder);
//
// SearchRequest request = new SearchRequest(detector.getIndices().toArray(new String[0]), searchSourceBuilder);
// client.search(request, ActionListener.wrap(searchResponse -> {
// Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
// InternalCardinality totalEntities = (InternalCardinality) aggMap.get(CommonName.TOTAL_ENTITIES);
// long value = totalEntities.getValue();
// DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
// DetectorProfile profile = profileBuilder.totalEntities(value).build();
// listener.onResponse(profile);
// }, searchException -> {
// logger.warn(CommonErrorMessages.FAIL_TO_GET_TOTAL_ENTITIES + detector.getDetectorId());
// listener.onFailure(searchException);
// }));
// } else {
// // Run a composite query and count the number of buckets to decide cardinality of multiple category fields
// AggregationBuilder bucketAggs = AggregationBuilders
// .composite(
// CommonName.TOTAL_ENTITIES,
// detector.getCategoryField().stream().map(f -> new TermsValuesSourceBuilder(f).field(f)).collect(Collectors.toList())
// )
// .size(maxTotalEntitiesToTrack);
// SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().aggregation(bucketAggs).trackTotalHits(false).size(0);
// SearchRequest searchRequest = new SearchRequest()
// .indices(detector.getIndices().toArray(new String[0]))
// .source(searchSourceBuilder);
// client.search(searchRequest, ActionListener.wrap(searchResponse -> {
// DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
// Aggregations aggs = searchResponse.getAggregations();
// if (aggs == null) {
// // This would indicate some bug or some opensearch core changes that we are not aware of (we don't keep up-to-date
// // with
// // the large amounts of changes there). For example, they may change to if there are results return it; otherwise
// // return
// // null instead of an empty Aggregations as they currently do.
// logger.warn("Unexpected null aggregation.");
// listener.onResponse(profileBuilder.totalEntities(0L).build());
// return;
// }
//
// Aggregation aggrResult = aggs.get(CommonName.TOTAL_ENTITIES);
// if (aggrResult == null) {
// listener.onFailure(new IllegalArgumentException("Fail to find valid aggregation result"));
// return;
// }
//
// CompositeAggregation compositeAgg = (CompositeAggregation) aggrResult;
// DetectorProfile profile = profileBuilder.totalEntities(Long.valueOf(compositeAgg.getBuckets().size())).build();
// listener.onResponse(profile);
// }, searchException -> {
// logger.warn(CommonErrorMessages.FAIL_TO_GET_TOTAL_ENTITIES + detector.getDetectorId());
// listener.onFailure(searchException);
// }));
// }
//
// }
// }
//
// private void onGetDetectorForPrepare(String detectorId, ActionListener<DetectorProfile> listener, Set<DetectorProfileName> profiles) {
// DetectorProfile.Builder profileBuilder = new DetectorProfile.Builder();
// if (profiles.contains(DetectorProfileName.STATE)) {
// profileBuilder.state(DetectorState.DISABLED);
// }
// if (profiles.contains(DetectorProfileName.AD_TASK)) {
// adTaskManager.getLatestHistoricalTaskProfile(detectorId, transportService, profileBuilder.build(), listener);
// } else {
// listener.onResponse(profileBuilder.build());
// }
// }
//
// /**
// * We expect three kinds of states:
// * -Disabled: if get ad job api says the job is disabled;
// * -Init: if rcf model's total updates is less than required
// * -Running: if neither of the above applies and no exceptions.
// * @param detector anomaly detector
// * @param listener listener to process the returned state or exception
// * @param enabled whether the detector job is enabled or not
// * @param profilesToCollect target profiles to fetch
// */
// private void profileStateRelated(
// AnomalyDetector detector,
// MultiResponsesDelegateActionListener<DetectorProfile> listener,
// boolean enabled,
// Set<DetectorProfileName> profilesToCollect
// ) {
// if (enabled) {
// RCFPollingRequest request = new RCFPollingRequest(detector.getDetectorId());
// client.execute(RCFPollingAction.INSTANCE, request, onPollRCFUpdates(detector, profilesToCollect, listener));
// } else {
// DetectorProfile.Builder builder = new DetectorProfile.Builder();
// if (profilesToCollect.contains(DetectorProfileName.STATE)) {
// builder.state(DetectorState.DISABLED);
// }
// listener.onResponse(builder.build());
// }
// }
//
// private void profileModels(
// AnomalyDetector detector,
// Set<DetectorProfileName> profiles,
// AnomalyDetectorJob job,
// boolean forMultiEntityDetector,
// MultiResponsesDelegateActionListener<DetectorProfile> listener
// ) {
// DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
// ProfileRequest profileRequest = new ProfileRequest(detector.getDetectorId(), profiles, forMultiEntityDetector, dataNodes);
// client.execute(ProfileAction.INSTANCE, profileRequest, onModelResponse(detector, profiles, job, listener));// get init progress
// }
//
// private ActionListener<ProfileResponse> onModelResponse(
// AnomalyDetector detector,
// Set<DetectorProfileName> profilesToCollect,
// AnomalyDetectorJob job,
// MultiResponsesDelegateActionListener<DetectorProfile> listener
// ) {
// boolean isMultientityDetector = detector.isMultientityDetector();
// return ActionListener.wrap(profileResponse -> {
// DetectorProfile.Builder profile = new DetectorProfile.Builder();
// if (profilesToCollect.contains(DetectorProfileName.COORDINATING_NODE)) {
// profile.coordinatingNode(profileResponse.getCoordinatingNode());
// }
// if (profilesToCollect.contains(DetectorProfileName.SHINGLE_SIZE)) {
// profile.shingleSize(profileResponse.getShingleSize());
// }
// if (profilesToCollect.contains(DetectorProfileName.TOTAL_SIZE_IN_BYTES)) {
// profile.totalSizeInBytes(profileResponse.getTotalSizeInBytes());
// }
// if (profilesToCollect.contains(DetectorProfileName.MODELS)) {
// profile.modelProfile(profileResponse.getModelProfile());
// profile.modelCount(profileResponse.getModelCount());
// }
// if (isMultientityDetector && profilesToCollect.contains(DetectorProfileName.ACTIVE_ENTITIES)) {
// profile.activeEntities(profileResponse.getActiveEntities());
// }
//
// if (isMultientityDetector
// && (profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)
// || profilesToCollect.contains(DetectorProfileName.STATE))) {
// profileMultiEntityDetectorStateRelated(job, profilesToCollect, profileResponse, profile, detector, listener);
// } else {
// listener.onResponse(profile.build());
// }
// }, listener::onFailure);
// }
//
// private void profileMultiEntityDetectorStateRelated(
// AnomalyDetectorJob job,
// Set<DetectorProfileName> profilesToCollect,
// ProfileResponse profileResponse,
// DetectorProfile.Builder profileBuilder,
// AnomalyDetector detector,
// MultiResponsesDelegateActionListener<DetectorProfile> listener
// ) {
// if (job.isEnabled()) {
// if (profileResponse.getTotalUpdates() < requiredSamples) {
// // need to double check since what ProfileResponse returns is the highest priority entity currently in memory, but
// // another entity might have already been initialized and sit somewhere else (in memory or on disk).
// confirmMultiEntityDetectorInitStatus(
// detector,
// job.getEnabledTime().toEpochMilli(),
// profileBuilder,
// profilesToCollect,
// profileResponse.getTotalUpdates(),
// listener
// );
// } else {
// createRunningStateAndInitProgress(profilesToCollect, profileBuilder);
// listener.onResponse(profileBuilder.build());
// }
// } else {
// if (profilesToCollect.contains(DetectorProfileName.STATE)) {
// profileBuilder.state(DetectorState.DISABLED);
// }
// listener.onResponse(profileBuilder.build());
// }
// }
//
// private void confirmMultiEntityDetectorInitStatus(
// AnomalyDetector detector,
// long enabledTime,
// DetectorProfile.Builder profile,
// Set<DetectorProfileName> profilesToCollect,
// long totalUpdates,
// MultiResponsesDelegateActionListener<DetectorProfile> listener
// ) {
// SearchRequest searchLatestResult = createInittedEverRequest(detector.getDetectorId(), enabledTime, detector.getResultIndex());
// client.search(searchLatestResult, onInittedEver(enabledTime, profile, profilesToCollect, detector, totalUpdates, listener));
// }
//
// private ActionListener<SearchResponse> onInittedEver(
// long lastUpdateTimeMs,
// DetectorProfile.Builder profileBuilder,
// Set<DetectorProfileName> profilesToCollect,
// AnomalyDetector detector,
// long totalUpdates,
// MultiResponsesDelegateActionListener<DetectorProfile> listener
// ) {
// return ActionListener.wrap(searchResponse -> {
// SearchHits hits = searchResponse.getHits();
// if (hits.getTotalHits().value == 0L) {
// processInitResponse(detector, profilesToCollect, totalUpdates, false, profileBuilder, listener);
// } else {
// createRunningStateAndInitProgress(profilesToCollect, profileBuilder);
// listener.onResponse(profileBuilder.build());
// }
// }, exception -> {
// if (ExceptionUtil.isIndexNotAvailable(exception)) {
// // anomaly result index is not created yet
// processInitResponse(detector, profilesToCollect, totalUpdates, false, profileBuilder, listener);
// } else {
// logger
// .error(
// "Fail to find any anomaly result with anomaly score larger than 0 after AD job enabled time for detector {}",
// detector.getDetectorId()
// );
// listener.onFailure(exception);
// }
// });
// }
//
// /**
// * Listener for polling rcf updates through transport messaging
// * @param detector anomaly detector
// * @param profilesToCollect profiles to collect like state
// * @param listener delegate listener
// * @return Listener for polling rcf updates through transport messaging
// */
// private ActionListener<RCFPollingResponse> onPollRCFUpdates(
// AnomalyDetector detector,
// Set<DetectorProfileName> profilesToCollect,
// MultiResponsesDelegateActionListener<DetectorProfile> listener
// ) {
// return ActionListener.wrap(rcfPollResponse -> {
// long totalUpdates = rcfPollResponse.getTotalUpdates();
// if (totalUpdates < requiredSamples) {
// processInitResponse(detector, profilesToCollect, totalUpdates, false, new DetectorProfile.Builder(), listener);
// } else {
// DetectorProfile.Builder builder = new DetectorProfile.Builder();
// createRunningStateAndInitProgress(profilesToCollect, builder);
// listener.onResponse(builder.build());
// }
// }, exception -> {
// // we will get an AnomalyDetectionException wrapping the real exception inside
// Throwable cause = Throwables.getRootCause(exception);
//
// // exception can be a RemoteTransportException
// Exception causeException = (Exception) cause;
// if (ExceptionUtil
// .isException(
// causeException,
// ResourceNotFoundException.class,
// NotSerializedADExceptionName.RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE.getName()
// )
// || (ExceptionUtil.isIndexNotAvailable(causeException)
// && causeException.getMessage().contains(CommonName.CHECKPOINT_INDEX_NAME))) {
// // cannot find checkpoint
// // We don't want to show the estimated time remaining to initialize
// // a detector before cold start finishes, where the actual
// // initialization time may be much shorter if sufficient historical
// // data exists.
// processInitResponse(detector, profilesToCollect, 0L, true, new DetectorProfile.Builder(), listener);
// } else {
// logger
// .error(
// new ParameterizedMessage("Fail to get init progress through messaging for {}", detector.getDetectorId()),
// exception
// );
// listener.onFailure(exception);
// }
// });
// }
//
// private void createRunningStateAndInitProgress(Set<DetectorProfileName> profilesToCollect, DetectorProfile.Builder builder) {
// if (profilesToCollect.contains(DetectorProfileName.STATE)) {
// builder.state(DetectorState.RUNNING).build();
// }
//
// if (profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)) {
// InitProgressProfile initProgress = new InitProgressProfile("100%", 0, 0);
// builder.initProgress(initProgress);
// }
// }
//
// private void processInitResponse(
// AnomalyDetector detector,
// Set<DetectorProfileName> profilesToCollect,
// long totalUpdates,
// boolean hideMinutesLeft,
// DetectorProfile.Builder builder,
// MultiResponsesDelegateActionListener<DetectorProfile> listener
// ) {
// if (profilesToCollect.contains(DetectorProfileName.STATE)) {
// builder.state(DetectorState.INIT);
// }
//
// if (profilesToCollect.contains(DetectorProfileName.INIT_PROGRESS)) {
// if (hideMinutesLeft) {
// InitProgressProfile initProgress = computeInitProgressProfile(totalUpdates, 0);
// builder.initProgress(initProgress);
// } else {
// long intervalMins = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMinutes();
// InitProgressProfile initProgress = computeInitProgressProfile(totalUpdates, intervalMins);
// builder.initProgress(initProgress);
// }
// }
//
// listener.onResponse(builder.build());
// }
//
// /**
// * Create search request to check if we have at least 1 anomaly score larger than 0 after AD job enabled time
// * @param detectorId detector id
// * @param enabledTime the time when AD job is enabled in milliseconds
// * @return the search request
// */
// private SearchRequest createInittedEverRequest(String detectorId, long enabledTime, String resultIndex) {
// BoolQueryBuilder filterQuery = new BoolQueryBuilder();
// filterQuery.filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId));
// filterQuery.filter(QueryBuilders.rangeQuery(AnomalyResult.EXECUTION_END_TIME_FIELD).gte(enabledTime));
// filterQuery.filter(QueryBuilders.rangeQuery(AnomalyResult.ANOMALY_SCORE_FIELD).gt(0));
//
// SearchSourceBuilder source = new SearchSourceBuilder().query(filterQuery).size(1);
//
// SearchRequest request = new SearchRequest(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
// request.source(source);
// if (resultIndex != null) {
// request.indices(resultIndex);
// }
// return request;
// }
// }
