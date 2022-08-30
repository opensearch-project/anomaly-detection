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
// import static org.opensearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
// import static org.opensearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
// import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
//
// import java.util.List;
// import java.util.Map;
// import java.util.Optional;
// import java.util.Set;
//
// import org.apache.logging.log4j.LogManager;
// import org.apache.logging.log4j.Logger;
// import org.apache.lucene.search.join.ScoreMode;
// import org.opensearch.action.ActionListener;
// import org.opensearch.action.get.GetRequest;
// import org.opensearch.action.search.SearchRequest;
// import org.opensearch.ad.constant.CommonErrorMessages;
// import org.opensearch.ad.constant.CommonName;
// import org.opensearch.ad.model.AnomalyDetector;
// import org.opensearch.ad.model.AnomalyDetectorJob;
// import org.opensearch.ad.model.AnomalyResult;
// import org.opensearch.ad.model.Entity;
// import org.opensearch.ad.model.EntityProfile;
// import org.opensearch.ad.model.EntityProfileName;
// import org.opensearch.ad.model.EntityState;
// import org.opensearch.ad.model.InitProgressProfile;
// import org.opensearch.ad.model.IntervalTimeConfiguration;
// import org.opensearch.ad.settings.NumericSetting;
// import org.opensearch.ad.transport.EntityProfileAction;
// import org.opensearch.ad.transport.EntityProfileRequest;
// import org.opensearch.ad.transport.EntityProfileResponse;
// import org.opensearch.ad.util.MultiResponsesDelegateActionListener;
// import org.opensearch.ad.util.ParseUtils;
// import org.opensearch.client.Client;
// import org.opensearch.cluster.routing.Preference;
// import org.opensearch.common.xcontent.LoggingDeprecationHandler;
// import org.opensearch.common.xcontent.NamedXContentRegistry;
// import org.opensearch.common.xcontent.XContentParser;
// import org.opensearch.common.xcontent.XContentType;
// import org.opensearch.index.IndexNotFoundException;
// import org.opensearch.index.query.BoolQueryBuilder;
// import org.opensearch.index.query.NestedQueryBuilder;
// import org.opensearch.index.query.QueryBuilders;
// import org.opensearch.index.query.TermQueryBuilder;
// import org.opensearch.search.aggregations.AggregationBuilders;
// import org.opensearch.search.builder.SearchSourceBuilder;
//
// public class EntityProfileRunner extends AbstractProfileRunner {
// private final Logger logger = LogManager.getLogger(EntityProfileRunner.class);
//
// static final String NOT_HC_DETECTOR_ERR_MSG = "This is not a high cardinality detector";
// static final String EMPTY_ENTITY_ATTRIBUTES = "Empty entity attributes";
// static final String NO_ENTITY = "Cannot find entity";
// private Client client;
// private NamedXContentRegistry xContentRegistry;
//
// public EntityProfileRunner(Client client, NamedXContentRegistry xContentRegistry, long requiredSamples) {
// super(requiredSamples);
// this.client = client;
// this.xContentRegistry = xContentRegistry;
// }
//
// /**
// * Get profile info of specific entity.
// *
// * @param detectorId detector identifier
// * @param entityValue entity value
// * @param profilesToCollect profiles to collect
// * @param listener action listener to handle exception and process entity profile response
// */
// public void profile(
// String detectorId,
// Entity entityValue,
// Set<EntityProfileName> profilesToCollect,
// ActionListener<EntityProfile> listener
// ) {
// if (profilesToCollect == null || profilesToCollect.size() == 0) {
// listener.onFailure(new IllegalArgumentException(CommonErrorMessages.EMPTY_PROFILES_COLLECT));
// return;
// }
// GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);
//
// client.get(getDetectorRequest, ActionListener.wrap(getResponse -> {
// if (getResponse != null && getResponse.isExists()) {
// try (
// XContentParser parser = XContentType.JSON
// .xContent()
// .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
// ) {
// ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
// AnomalyDetector detector = AnomalyDetector.parse(parser, detectorId);
// List<String> categoryFields = detector.getCategoryField();
// int maxCategoryFields = NumericSetting.maxCategoricalFields();
// if (categoryFields == null || categoryFields.size() == 0) {
// listener.onFailure(new IllegalArgumentException(NOT_HC_DETECTOR_ERR_MSG));
// } else if (categoryFields.size() > maxCategoryFields) {
// listener
// .onFailure(new IllegalArgumentException(CommonErrorMessages.getTooManyCategoricalFieldErr(maxCategoryFields)));
// } else {
// validateEntity(entityValue, categoryFields, detectorId, profilesToCollect, detector, listener);
// }
// } catch (Exception t) {
// listener.onFailure(t);
// }
// } else {
// listener.onFailure(new IllegalArgumentException(CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG + detectorId));
// }
// }, listener::onFailure));
// }
//
// /**
// * Verify if the input entity exists or not in case of typos.
// *
// * If a user deletes the entity after job start, then we will not be able to
// * get this entity in the index. For this case, we will not return a profile
// * for this entity even if it's running on some data node. the entity's model
// * will be deleted by another entity or by maintenance due to long inactivity.
// *
// * @param entity Entity accessor
// * @param categoryFields category fields defined for a detector
// * @param detectorId Detector Id
// * @param profilesToCollect Profile to collect from the input
// * @param detector Detector config accessor
// * @param listener Callback to send responses.
// */
// private void validateEntity(
// Entity entity,
// List<String> categoryFields,
// String detectorId,
// Set<EntityProfileName> profilesToCollect,
// AnomalyDetector detector,
// ActionListener<EntityProfile> listener
// ) {
// Map<String, String> attributes = entity.getAttributes();
// if (attributes == null || attributes.size() != categoryFields.size()) {
// listener.onFailure(new IllegalArgumentException(EMPTY_ENTITY_ATTRIBUTES));
// return;
// }
// for (String field : categoryFields) {
// if (false == attributes.containsKey(field)) {
// listener.onFailure(new IllegalArgumentException("Cannot find " + field));
// return;
// }
// }
//
// BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().filter(detector.getFilterQuery());
//
// for (TermQueryBuilder term : entity.getTermQueryBuilders()) {
// internalFilterQuery.filter(term);
// }
//
// SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery).size(1);
//
// SearchRequest searchRequest = new SearchRequest(detector.getIndices().toArray(new String[0]), searchSourceBuilder)
// .preference(Preference.LOCAL.toString());
//
// client.search(searchRequest, ActionListener.wrap(searchResponse -> {
// try {
// if (searchResponse.getHits().getHits().length == 0) {
// listener.onFailure(new IllegalArgumentException(NO_ENTITY));
// return;
// }
// prepareEntityProfile(listener, detectorId, entity, profilesToCollect, detector, categoryFields.get(0));
// } catch (Exception e) {
// listener.onFailure(new IllegalArgumentException(NO_ENTITY));
// return;
// }
// }, e -> listener.onFailure(new IllegalArgumentException(NO_ENTITY))));
//
// }
//
// private void prepareEntityProfile(
// ActionListener<EntityProfile> listener,
// String detectorId,
// Entity entityValue,
// Set<EntityProfileName> profilesToCollect,
// AnomalyDetector detector,
// String categoryField
// ) {
// EntityProfileRequest request = new EntityProfileRequest(detectorId, entityValue, profilesToCollect);
//
// client
// .execute(
// EntityProfileAction.INSTANCE,
// request,
// ActionListener.wrap(r -> getJob(detectorId, entityValue, profilesToCollect, detector, r, listener), listener::onFailure)
// );
// }
//
// private void getJob(
// String detectorId,
// Entity entityValue,
// Set<EntityProfileName> profilesToCollect,
// AnomalyDetector detector,
// EntityProfileResponse entityProfileResponse,
// ActionListener<EntityProfile> listener
// ) {
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
//
// int totalResponsesToWait = 0;
// if (profilesToCollect.contains(EntityProfileName.INIT_PROGRESS)
// || profilesToCollect.contains(EntityProfileName.STATE)) {
// totalResponsesToWait++;
// }
// if (profilesToCollect.contains(EntityProfileName.ENTITY_INFO)) {
// totalResponsesToWait++;
// }
// if (profilesToCollect.contains(EntityProfileName.MODELS)) {
// totalResponsesToWait++;
// }
// MultiResponsesDelegateActionListener<EntityProfile> delegateListener =
// new MultiResponsesDelegateActionListener<EntityProfile>(
// listener,
// totalResponsesToWait,
// CommonErrorMessages.FAIL_FETCH_ERR_MSG + entityValue + " of detector " + detectorId,
// false
// );
//
// if (profilesToCollect.contains(EntityProfileName.MODELS)) {
// EntityProfile.Builder builder = new EntityProfile.Builder();
// if (false == job.isEnabled()) {
// delegateListener.onResponse(builder.build());
// } else {
// delegateListener.onResponse(builder.modelProfile(entityProfileResponse.getModelProfile()).build());
// }
// }
//
// if (profilesToCollect.contains(EntityProfileName.INIT_PROGRESS)
// || profilesToCollect.contains(EntityProfileName.STATE)) {
// profileStateRelated(
// entityProfileResponse.getTotalUpdates(),
// detectorId,
// entityValue,
// profilesToCollect,
// detector,
// job,
// delegateListener
// );
// }
//
// if (profilesToCollect.contains(EntityProfileName.ENTITY_INFO)) {
// long enabledTimeMs = job.getEnabledTime().toEpochMilli();
// SearchRequest lastSampleTimeRequest = createLastSampleTimeRequest(
// detectorId,
// enabledTimeMs,
// entityValue,
// detector.getResultIndex()
// );
//
// EntityProfile.Builder builder = new EntityProfile.Builder();
//
// Optional<Boolean> isActiveOp = entityProfileResponse.isActive();
// if (isActiveOp.isPresent()) {
// builder.isActive(isActiveOp.get());
// }
// builder.lastActiveTimestampMs(entityProfileResponse.getLastActiveMs());
//
// client.search(lastSampleTimeRequest, ActionListener.wrap(searchResponse -> {
// Optional<Long> latestSampleTimeMs = ParseUtils.getLatestDataTime(searchResponse);
//
// if (latestSampleTimeMs.isPresent()) {
// builder.lastSampleTimestampMs(latestSampleTimeMs.get());
// }
//
// delegateListener.onResponse(builder.build());
// }, exception -> {
// // sth wrong like result index not created. Return what we have
// if (exception instanceof IndexNotFoundException) {
// // don't print out stack trace since it is not helpful
// logger.info("Result index hasn't been created", exception.getMessage());
// } else {
// logger.warn("fail to get last sample time", exception);
// }
// delegateListener.onResponse(builder.build());
// }));
// }
// } catch (Exception e) {
// logger.error(CommonErrorMessages.FAIL_TO_GET_PROFILE_MSG, e);
// listener.onFailure(e);
// }
// } else {
// sendUnknownState(profilesToCollect, entityValue, true, listener);
// }
// }, exception -> {
// if (exception instanceof IndexNotFoundException) {
// logger.info(exception.getMessage());
// sendUnknownState(profilesToCollect, entityValue, true, listener);
// } else {
// logger.error(CommonErrorMessages.FAIL_TO_GET_PROFILE_MSG + detectorId, exception);
// listener.onFailure(exception);
// }
// }));
// }
//
// private void profileStateRelated(
// long totalUpdates,
// String detectorId,
// Entity entityValue,
// Set<EntityProfileName> profilesToCollect,
// AnomalyDetector detector,
// AnomalyDetectorJob job,
// MultiResponsesDelegateActionListener<EntityProfile> delegateListener
// ) {
// if (totalUpdates == 0) {
// sendUnknownState(profilesToCollect, entityValue, false, delegateListener);
// } else if (false == job.isEnabled()) {
// sendUnknownState(profilesToCollect, entityValue, false, delegateListener);
// } else if (totalUpdates >= requiredSamples) {
// sendRunningState(profilesToCollect, entityValue, delegateListener);
// } else {
// sendInitState(profilesToCollect, entityValue, detector, totalUpdates, delegateListener);
// }
// }
//
// /**
// * Send unknown state back
// * @param profilesToCollect Profiles to Collect
// * @param entityValue Entity value
// * @param immediate whether we should terminate workflow and respond immediately
// * @param delegateListener Delegate listener
// */
// private void sendUnknownState(
// Set<EntityProfileName> profilesToCollect,
// Entity entityValue,
// boolean immediate,
// ActionListener<EntityProfile> delegateListener
// ) {
// EntityProfile.Builder builder = new EntityProfile.Builder();
// if (profilesToCollect.contains(EntityProfileName.STATE)) {
// builder.state(EntityState.UNKNOWN);
// }
// if (immediate) {
// delegateListener.onResponse(builder.build());
// } else {
// delegateListener.onResponse(builder.build());
// }
// }
//
// private void sendRunningState(
// Set<EntityProfileName> profilesToCollect,
// Entity entityValue,
// MultiResponsesDelegateActionListener<EntityProfile> delegateListener
// ) {
// EntityProfile.Builder builder = new EntityProfile.Builder();
// if (profilesToCollect.contains(EntityProfileName.STATE)) {
// builder.state(EntityState.RUNNING);
// }
// if (profilesToCollect.contains(EntityProfileName.INIT_PROGRESS)) {
// InitProgressProfile initProgress = new InitProgressProfile("100%", 0, 0);
// builder.initProgress(initProgress);
// }
// delegateListener.onResponse(builder.build());
// }
//
// private void sendInitState(
// Set<EntityProfileName> profilesToCollect,
// Entity entityValue,
// AnomalyDetector detector,
// long updates,
// MultiResponsesDelegateActionListener<EntityProfile> delegateListener
// ) {
// EntityProfile.Builder builder = new EntityProfile.Builder();
// if (profilesToCollect.contains(EntityProfileName.STATE)) {
// builder.state(EntityState.INIT);
// }
// if (profilesToCollect.contains(EntityProfileName.INIT_PROGRESS)) {
// long intervalMins = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMinutes();
// InitProgressProfile initProgress = computeInitProgressProfile(updates, intervalMins);
// builder.initProgress(initProgress);
// }
// delegateListener.onResponse(builder.build());
// }
//
// private SearchRequest createLastSampleTimeRequest(String detectorId, long enabledTime, Entity entity, String resultIndex) {
// BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
//
// String path = "entity";
// String entityName = path + ".name";
// String entityValue = path + ".value";
//
// for (Map.Entry<String, String> attribute : entity.getAttributes().entrySet()) {
// /*
// * each attribute pair corresponds to a nested query like
// "nested": {
// "query": {
// "bool": {
// "filter": [
// {
// "term": {
// "entity.name": {
// "value": "turkey4",
// "boost": 1
// }
// }
// },
// {
// "term": {
// "entity.value": {
// "value": "Turkey",
// "boost": 1
// }
// }
// }
// ]
// }
// },
// "path": "entity",
// "ignore_unmapped": false,
// "score_mode": "none",
// "boost": 1
// }
// },*/
// BoolQueryBuilder nestedBoolQueryBuilder = new BoolQueryBuilder();
//
// TermQueryBuilder entityNameFilterQuery = QueryBuilders.termQuery(entityName, attribute.getKey());
// nestedBoolQueryBuilder.filter(entityNameFilterQuery);
// TermQueryBuilder entityValueFilterQuery = QueryBuilders.termQuery(entityValue, attribute.getValue());
// nestedBoolQueryBuilder.filter(entityValueFilterQuery);
//
// NestedQueryBuilder nestedNameQueryBuilder = new NestedQueryBuilder(path, nestedBoolQueryBuilder, ScoreMode.None);
// boolQueryBuilder.filter(nestedNameQueryBuilder);
// }
//
// boolQueryBuilder.filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId));
//
// boolQueryBuilder.filter(QueryBuilders.rangeQuery(AnomalyResult.EXECUTION_END_TIME_FIELD).gte(enabledTime));
//
// SearchSourceBuilder source = new SearchSourceBuilder()
// .query(boolQueryBuilder)
// .aggregation(AggregationBuilders.max(CommonName.AGG_NAME_MAX_TIME).field(AnomalyResult.EXECUTION_END_TIME_FIELD))
// .trackTotalHits(false)
// .size(0);
//
// SearchRequest request = new SearchRequest(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
// request.source(source);
// if (resultIndex != null) {
// request.indices(resultIndex);
// }
// return request;
// }
// }
