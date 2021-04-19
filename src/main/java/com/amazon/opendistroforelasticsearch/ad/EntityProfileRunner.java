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

package com.amazon.opendistroforelasticsearch.ad;

import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector.ANOMALY_DETECTORS_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob.ANOMALY_DETECTOR_JOB_INDEX;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.CATEGORY_FIELD_LIMIT;
import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.security.InvalidParameterException;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.ActionListener;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.client.Client;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetectorJob;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.model.EntityProfile;
import com.amazon.opendistroforelasticsearch.ad.model.EntityProfileName;
import com.amazon.opendistroforelasticsearch.ad.model.EntityState;
import com.amazon.opendistroforelasticsearch.ad.model.InitProgressProfile;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.transport.EntityProfileAction;
import com.amazon.opendistroforelasticsearch.ad.transport.EntityProfileRequest;
import com.amazon.opendistroforelasticsearch.ad.transport.EntityProfileResponse;
import com.amazon.opendistroforelasticsearch.ad.util.MultiResponsesDelegateActionListener;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;

public class EntityProfileRunner extends AbstractProfileRunner {
    private final Logger logger = LogManager.getLogger(EntityProfileRunner.class);

    static final String NOT_HC_DETECTOR_ERR_MSG = "This is not a high cardinality detector";
    private Client client;
    private NamedXContentRegistry xContentRegistry;

    public EntityProfileRunner(Client client, NamedXContentRegistry xContentRegistry, long requiredSamples) {
        super(requiredSamples);
        this.client = client;
        this.xContentRegistry = xContentRegistry;
    }

    /**
     * Get profile info of specific entity.
     *
     * @param detectorId detector identifier
     * @param entityValue entity value
     * @param profilesToCollect profiles to collect
     * @param listener action listener to handle exception and process entity profile response
     */
    public void profile(
        String detectorId,
        String entityValue,
        Set<EntityProfileName> profilesToCollect,
        ActionListener<EntityProfile> listener
    ) {
        if (profilesToCollect == null || profilesToCollect.size() == 0) {
            listener.onFailure(new InvalidParameterException(CommonErrorMessages.EMPTY_PROFILES_COLLECT));
            return;
        }
        GetRequest getDetectorRequest = new GetRequest(ANOMALY_DETECTORS_INDEX, detectorId);

        client.get(getDetectorRequest, ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    AnomalyDetector detector = AnomalyDetector.parse(parser, detectorId);
                    List<String> categoryField = detector.getCategoryField();
                    if (categoryField == null || categoryField.size() == 0) {
                        listener.onFailure(new InvalidParameterException(NOT_HC_DETECTOR_ERR_MSG));
                    } else if (categoryField.size() > CATEGORY_FIELD_LIMIT) {
                        listener
                            .onFailure(
                                new InvalidParameterException(CommonErrorMessages.CATEGORICAL_FIELD_NUMBER_SURPASSED + CATEGORY_FIELD_LIMIT)
                            );
                    } else {
                        prepareEntityProfile(listener, detectorId, entityValue, profilesToCollect, detector, categoryField.get(0));
                    }
                } catch (Exception t) {
                    listener.onFailure(t);
                }
            } else {
                listener.onFailure(new InvalidParameterException(CommonErrorMessages.FAIL_TO_FIND_DETECTOR_MSG + detectorId));
            }
        }, listener::onFailure));
    }

    private void prepareEntityProfile(
        ActionListener<EntityProfile> listener,
        String detectorId,
        String entityValue,
        Set<EntityProfileName> profilesToCollect,
        AnomalyDetector detector,
        String categoryField
    ) {
        EntityProfileRequest request = new EntityProfileRequest(detectorId, entityValue, profilesToCollect);

        client
            .execute(
                EntityProfileAction.INSTANCE,
                request,
                ActionListener
                    .wrap(
                        r -> getJob(detectorId, categoryField, entityValue, profilesToCollect, detector, r, listener),
                        listener::onFailure
                    )
            );
    }

    private void getJob(
        String detectorId,
        String categoryField,
        String entityValue,
        Set<EntityProfileName> profilesToCollect,
        AnomalyDetector detector,
        EntityProfileResponse entityProfileResponse,
        ActionListener<EntityProfile> listener
    ) {
        GetRequest getRequest = new GetRequest(ANOMALY_DETECTOR_JOB_INDEX, detectorId);
        client.get(getRequest, ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    AnomalyDetectorJob job = AnomalyDetectorJob.parse(parser);

                    int totalResponsesToWait = 0;
                    if (profilesToCollect.contains(EntityProfileName.INIT_PROGRESS)
                        || profilesToCollect.contains(EntityProfileName.STATE)) {
                        totalResponsesToWait++;
                    }
                    if (profilesToCollect.contains(EntityProfileName.ENTITY_INFO)) {
                        totalResponsesToWait++;
                    }
                    if (profilesToCollect.contains(EntityProfileName.MODELS)) {
                        totalResponsesToWait++;
                    }
                    MultiResponsesDelegateActionListener<EntityProfile> delegateListener =
                        new MultiResponsesDelegateActionListener<EntityProfile>(
                            listener,
                            totalResponsesToWait,
                            CommonErrorMessages.FAIL_FETCH_ERR_MSG + entityValue + " of detector " + detectorId,
                            false
                        );

                    if (profilesToCollect.contains(EntityProfileName.MODELS)) {
                        EntityProfile.Builder builder = new EntityProfile.Builder(categoryField, entityValue);
                        if (false == job.isEnabled()) {
                            delegateListener.onResponse(builder.build());
                        } else {
                            delegateListener.onResponse(builder.modelProfile(entityProfileResponse.getModelProfile()).build());
                        }
                    }

                    if (profilesToCollect.contains(EntityProfileName.INIT_PROGRESS)
                        || profilesToCollect.contains(EntityProfileName.STATE)) {
                        profileStateRelated(
                            entityProfileResponse.getTotalUpdates(),
                            detectorId,
                            categoryField,
                            entityValue,
                            profilesToCollect,
                            detector,
                            job,
                            delegateListener
                        );
                    }

                    if (profilesToCollect.contains(EntityProfileName.ENTITY_INFO)) {
                        long enabledTimeMs = job.getEnabledTime().toEpochMilli();
                        SearchRequest lastSampleTimeRequest = createLastSampleTimeRequest(detectorId, enabledTimeMs, entityValue);

                        EntityProfile.Builder builder = new EntityProfile.Builder(categoryField, entityValue);

                        Optional<Boolean> isActiveOp = entityProfileResponse.isActive();
                        if (isActiveOp.isPresent()) {
                            builder.isActive(isActiveOp.get());
                        }
                        builder.lastActiveTimestampMs(entityProfileResponse.getLastActiveMs());

                        client.search(lastSampleTimeRequest, ActionListener.wrap(searchResponse -> {
                            Optional<Long> latestSampleTimeMs = ParseUtils.getLatestDataTime(searchResponse);

                            if (latestSampleTimeMs.isPresent()) {
                                builder.lastSampleTimestampMs(latestSampleTimeMs.get());
                            }

                            delegateListener.onResponse(builder.build());
                        }, exception -> {
                            // sth wrong like result index not created. Return what we have
                            if (exception instanceof IndexNotFoundException) {
                                // don't print out stack trace since it is not helpful
                                logger.info("Result index hasn't been created", exception.getMessage());
                            } else {
                                logger.warn("fail to get last sample time", exception);
                            }
                            delegateListener.onResponse(builder.build());
                        }));
                    }
                } catch (Exception e) {
                    logger.error(CommonErrorMessages.FAIL_TO_GET_PROFILE_MSG, e);
                    listener.onFailure(e);
                }
            } else {
                sendUnknownState(profilesToCollect, categoryField, entityValue, true, listener);
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                logger.info(exception.getMessage());
                sendUnknownState(profilesToCollect, categoryField, entityValue, true, listener);
            } else {
                logger.error(CommonErrorMessages.FAIL_TO_GET_PROFILE_MSG + detectorId, exception);
                listener.onFailure(exception);
            }
        }));
    }

    private void profileStateRelated(
        long totalUpdates,
        String detectorId,
        String categoryField,
        String entityValue,
        Set<EntityProfileName> profilesToCollect,
        AnomalyDetector detector,
        AnomalyDetectorJob job,
        MultiResponsesDelegateActionListener<EntityProfile> delegateListener
    ) {
        if (totalUpdates == 0) {
            sendUnknownState(profilesToCollect, categoryField, entityValue, false, delegateListener);
        } else if (false == job.isEnabled()) {
            sendUnknownState(profilesToCollect, categoryField, entityValue, false, delegateListener);
        } else if (totalUpdates >= requiredSamples) {
            sendRunningState(profilesToCollect, categoryField, entityValue, delegateListener);
        } else {
            sendInitState(profilesToCollect, categoryField, entityValue, detector, totalUpdates, delegateListener);
        }
    }

    /**
     * Send unknown state back
     * @param profilesToCollect Profiles to Collect
     * @param categoryField Category field
     * @param entityValue Entity value
     * @param immediate whether we should terminate workflow and respond immediately
     * @param delegateListener Delegate listener
     */
    private void sendUnknownState(
        Set<EntityProfileName> profilesToCollect,
        String categoryField,
        String entityValue,
        boolean immediate,
        ActionListener<EntityProfile> delegateListener
    ) {
        EntityProfile.Builder builder = new EntityProfile.Builder(categoryField, entityValue);
        if (profilesToCollect.contains(EntityProfileName.STATE)) {
            builder.state(EntityState.UNKNOWN);
        }
        if (immediate) {
            delegateListener.onResponse(builder.build());
        } else {
            delegateListener.onResponse(builder.build());
        }
    }

    private void sendRunningState(
        Set<EntityProfileName> profilesToCollect,
        String categoryField,
        String entityValue,
        MultiResponsesDelegateActionListener<EntityProfile> delegateListener
    ) {
        EntityProfile.Builder builder = new EntityProfile.Builder(categoryField, entityValue);
        if (profilesToCollect.contains(EntityProfileName.STATE)) {
            builder.state(EntityState.RUNNING);
        }
        if (profilesToCollect.contains(EntityProfileName.INIT_PROGRESS)) {
            InitProgressProfile initProgress = new InitProgressProfile("100%", 0, 0);
            builder.initProgress(initProgress);
        }
        delegateListener.onResponse(builder.build());
    }

    private void sendInitState(
        Set<EntityProfileName> profilesToCollect,
        String categoryField,
        String entityValue,
        AnomalyDetector detector,
        long updates,
        MultiResponsesDelegateActionListener<EntityProfile> delegateListener
    ) {
        EntityProfile.Builder builder = new EntityProfile.Builder(categoryField, entityValue);
        if (profilesToCollect.contains(EntityProfileName.STATE)) {
            builder.state(EntityState.INIT);
        }
        if (profilesToCollect.contains(EntityProfileName.INIT_PROGRESS)) {
            long intervalMins = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMinutes();
            InitProgressProfile initProgress = computeInitProgressProfile(updates, intervalMins);
            builder.initProgress(initProgress);
        }
        delegateListener.onResponse(builder.build());
    }

    private SearchRequest createLastSampleTimeRequest(String detectorId, long enabledTime, String entityValue) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        String path = "entity";
        String entityValueFieldName = path + ".value";
        TermQueryBuilder entityValueFilterQuery = QueryBuilders.termQuery(entityValueFieldName, entityValue);
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(path, entityValueFilterQuery, ScoreMode.None);
        boolQueryBuilder.filter(nestedQueryBuilder);

        boolQueryBuilder.filter(QueryBuilders.termQuery(AnomalyResult.DETECTOR_ID_FIELD, detectorId));

        boolQueryBuilder.filter(QueryBuilders.rangeQuery(AnomalyResult.EXECUTION_END_TIME_FIELD).gte(enabledTime));

        SearchSourceBuilder source = new SearchSourceBuilder()
            .query(boolQueryBuilder)
            .aggregation(AggregationBuilders.max(CommonName.AGG_NAME_MAX_TIME).field(AnomalyResult.EXECUTION_END_TIME_FIELD))
            .trackTotalHits(false)
            .size(0);

        SearchRequest request = new SearchRequest(CommonName.ANOMALY_RESULT_INDEX_ALIAS);
        request.source(source);
        return request;
    }
}
