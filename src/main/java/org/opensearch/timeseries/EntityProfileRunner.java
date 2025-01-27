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

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.EntityProfile;
import org.opensearch.timeseries.model.EntityProfileName;
import org.opensearch.timeseries.model.EntityState;
import org.opensearch.timeseries.model.InitProgressProfile;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.transport.EntityProfileRequest;
import org.opensearch.timeseries.transport.EntityProfileResponse;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

public class EntityProfileRunner<EntityProfileActionType extends ActionType<EntityProfileResponse>> extends AbstractProfileRunner {
    private final Logger logger = LogManager.getLogger(EntityProfileRunner.class);

    public static final String NOT_HC_DETECTOR_ERR_MSG = "This is not a high cardinality detector";
    static final String EMPTY_ENTITY_ATTRIBUTES = "Empty entity attributes";
    static final String NO_ENTITY = "Cannot find entity";
    private Client client;
    private SecurityClientUtil clientUtil;
    private NamedXContentRegistry xContentRegistry;
    private BiCheckedFunction<XContentParser, String, ? extends Config, IOException> configParser;
    private int maxCategoryFields;
    private AnalysisType analysisType;
    private EntityProfileActionType entityProfileAction;
    private String resultIndexAlias;
    private String configIdField;
    private String configIndexName;

    public EntityProfileRunner(
        Client client,
        SecurityClientUtil clientUtil,
        NamedXContentRegistry xContentRegistry,
        long requiredSamples,
        BiCheckedFunction<XContentParser, String, ? extends Config, IOException> configParser,
        int maxCategoryFields,
        AnalysisType analysisType,
        EntityProfileActionType entityProfileAction,
        String resultIndexAlias,
        String configIdField,
        String configIndexName
    ) {
        super(requiredSamples);
        this.client = client;
        this.clientUtil = clientUtil;
        this.xContentRegistry = xContentRegistry;
        this.configParser = configParser;
        this.maxCategoryFields = maxCategoryFields;
        this.analysisType = analysisType;
        this.entityProfileAction = entityProfileAction;
        this.resultIndexAlias = resultIndexAlias;
        this.configIdField = configIdField;
        this.configIndexName = configIndexName;
    }

    /**
     * Get profile info of specific entity.
     *
     * @param configId config identifier
     * @param entityValue entity value
     * @param profilesToCollect profiles to collect
     * @param listener action listener to handle exception and process entity profile response
     */
    public void profile(
        String configId,
        Entity entityValue,
        Set<EntityProfileName> profilesToCollect,
        ActionListener<EntityProfile> listener
    ) {
        if (profilesToCollect == null || profilesToCollect.size() == 0) {
            listener.onFailure(new IllegalArgumentException(CommonMessages.EMPTY_PROFILES_COLLECT));
            return;
        }
        GetRequest getDetectorRequest = new GetRequest(configIndexName, configId);

        client.get(getDetectorRequest, ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    Config config = configParser.apply(parser, configId);
                    List<String> categoryFields = config.getCategoryFields();
                    if (categoryFields == null || categoryFields.size() == 0) {
                        listener.onFailure(new IllegalArgumentException(NOT_HC_DETECTOR_ERR_MSG));
                    } else if (categoryFields.size() > maxCategoryFields) {
                        listener.onFailure(new IllegalArgumentException(CommonMessages.getTooManyCategoricalFieldErr(maxCategoryFields)));
                    } else {
                        validateEntity(entityValue, categoryFields, configId, profilesToCollect, config, listener);
                    }
                } catch (Exception t) {
                    listener.onFailure(t);
                }
            } else {
                listener.onFailure(new OpenSearchStatusException(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + configId, RestStatus.NOT_FOUND));
            }
        }, listener::onFailure));
    }

    /**
     * Verify if the input entity exists or not in case of typos.
     *
     * If a user deletes the entity after job start, then we will not be able to
     * get this entity in the index. For this case, we will not return a profile
     * for this entity even if it's running on some data node. the entity's model
     * will be deleted by another entity or by maintenance due to long inactivity.
     *
     * @param entity Entity accessor
     * @param categoryFields category fields defined for a detector
     * @param detectorId Detector Id
     * @param profilesToCollect Profile to collect from the input
     * @param detector Detector config accessor
     * @param listener Callback to send responses.
     */
    private void validateEntity(
        Entity entity,
        List<String> categoryFields,
        String detectorId,
        Set<EntityProfileName> profilesToCollect,
        Config config,
        ActionListener<EntityProfile> listener
    ) {
        Map<String, String> attributes = entity.getAttributes();
        if (attributes == null) {
            listener.onFailure(new IllegalArgumentException(EMPTY_ENTITY_ATTRIBUTES));
            return;
        }
        if (attributes.size() != categoryFields.size()) {
            listener.onFailure(new IllegalArgumentException(NO_ENTITY));
            return;
        }
        for (String field : categoryFields) {
            if (false == attributes.containsKey(field)) {
                listener.onFailure(new IllegalArgumentException("Cannot find " + field));
                return;
            }
        }

        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().filter(config.getFilterQuery());

        for (TermQueryBuilder term : entity.getTermQueryForCustomerIndex()) {
            internalFilterQuery.filter(term);
        }

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(internalFilterQuery).size(1);

        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0]), searchSourceBuilder)
            .preference(Preference.LOCAL.toString());
        final ActionListener<SearchResponse> searchResponseListener = ActionListener.wrap(searchResponse -> {
            try {
                if (searchResponse.getHits().getHits().length == 0) {
                    listener.onFailure(new IllegalArgumentException(NO_ENTITY));
                    return;
                }
                prepareEntityProfile(listener, detectorId, entity, profilesToCollect, config, categoryFields.get(0));
            } catch (Exception e) {
                listener.onFailure(new IllegalArgumentException(NO_ENTITY));
                return;
            }
        }, e -> listener.onFailure(new IllegalArgumentException(NO_ENTITY)));
        // using the original context in listener as user roles have no permissions for internal operations like fetching a
        // checkpoint
        clientUtil
            .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                searchRequest,
                client::search,
                config.getId(),
                client,
                analysisType,
                searchResponseListener
            );

    }

    private void prepareEntityProfile(
        ActionListener<EntityProfile> listener,
        String detectorId,
        Entity entityValue,
        Set<EntityProfileName> profilesToCollect,
        Config config,
        String categoryField
    ) {
        EntityProfileRequest request = new EntityProfileRequest(detectorId, entityValue, profilesToCollect);

        client
            .execute(
                entityProfileAction,
                request,
                ActionListener.wrap(r -> getJob(detectorId, entityValue, profilesToCollect, config, r, listener), listener::onFailure)
            );
    }

    private void getJob(
        String detectorId,
        Entity entityValue,
        Set<EntityProfileName> profilesToCollect,
        Config config,
        EntityProfileResponse entityProfileResponse,
        ActionListener<EntityProfile> listener
    ) {
        GetRequest getRequest = new GetRequest(CommonName.JOB_INDEX, detectorId);
        client.get(getRequest, ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    Job job = Job.parse(parser);

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
                            CommonMessages.FAIL_FETCH_ERR_MSG + entityValue + " of detector " + detectorId,
                            false
                        );

                    if (profilesToCollect.contains(EntityProfileName.MODELS)) {
                        EntityProfile.Builder builder = new EntityProfile.Builder();
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
                            entityValue,
                            profilesToCollect,
                            config,
                            job,
                            delegateListener
                        );
                    }

                    if (profilesToCollect.contains(EntityProfileName.ENTITY_INFO)) {
                        long enabledTimeMs = job.getEnabledTime().toEpochMilli();
                        SearchRequest lastSampleTimeRequest = createLastSampleTimeRequest(
                            detectorId,
                            enabledTimeMs,
                            entityValue,
                            config.getCustomResultIndexPattern()
                        );

                        EntityProfile.Builder builder = new EntityProfile.Builder();

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
                    logger.error(CommonMessages.FAIL_TO_GET_PROFILE_MSG, e);
                    listener.onFailure(e);
                }
            } else {
                sendUnknownState(profilesToCollect, entityValue, true, listener);
            }
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                logger.info(exception.getMessage());
                sendUnknownState(profilesToCollect, entityValue, true, listener);
            } else {
                logger.error(CommonMessages.FAIL_TO_GET_PROFILE_MSG + detectorId, exception);
                listener.onFailure(exception);
            }
        }));
    }

    private void profileStateRelated(
        long totalUpdates,
        String detectorId,
        Entity entityValue,
        Set<EntityProfileName> profilesToCollect,
        Config config,
        Job job,
        MultiResponsesDelegateActionListener<EntityProfile> delegateListener
    ) {
        if (totalUpdates == 0) {
            sendUnknownState(profilesToCollect, entityValue, false, delegateListener);
        } else if (false == job.isEnabled()) {
            sendUnknownState(profilesToCollect, entityValue, false, delegateListener);
        } else if (totalUpdates >= requiredSamples) {
            sendRunningState(profilesToCollect, entityValue, delegateListener);
        } else {
            sendInitState(profilesToCollect, entityValue, config, totalUpdates, delegateListener);
        }
    }

    /**
     * Send unknown state back
     * @param profilesToCollect Profiles to Collect
     * @param entityValue Entity value
     * @param immediate whether we should terminate workflow and respond immediately
     * @param delegateListener Delegate listener
     */
    private void sendUnknownState(
        Set<EntityProfileName> profilesToCollect,
        Entity entityValue,
        boolean immediate,
        ActionListener<EntityProfile> delegateListener
    ) {
        EntityProfile.Builder builder = new EntityProfile.Builder();
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
        Entity entityValue,
        MultiResponsesDelegateActionListener<EntityProfile> delegateListener
    ) {
        EntityProfile.Builder builder = new EntityProfile.Builder();
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
        Entity entityValue,
        Config config,
        long updates,
        MultiResponsesDelegateActionListener<EntityProfile> delegateListener
    ) {
        EntityProfile.Builder builder = new EntityProfile.Builder();
        if (profilesToCollect.contains(EntityProfileName.STATE)) {
            builder.state(EntityState.INIT);
        }
        if (profilesToCollect.contains(EntityProfileName.INIT_PROGRESS)) {
            long intervalMins = ((IntervalTimeConfiguration) config.getInterval()).toDuration().toMinutes();
            InitProgressProfile initProgress = computeInitProgressProfile(updates, intervalMins);
            builder.initProgress(initProgress);
        }
        delegateListener.onResponse(builder.build());
    }

    private SearchRequest createLastSampleTimeRequest(String configId, long enabledTime, Entity entity, String resultIndex) {
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        for (NestedQueryBuilder nestedNameQueryBuilder : entity.getTermQueryForResultIndex()) {
            boolQueryBuilder.filter(nestedNameQueryBuilder);
        }

        boolQueryBuilder.filter(QueryBuilders.termQuery(configIdField, configId));

        boolQueryBuilder.filter(QueryBuilders.rangeQuery(CommonName.EXECUTION_END_TIME_FIELD).gte(enabledTime));

        SearchSourceBuilder source = new SearchSourceBuilder()
            .query(boolQueryBuilder)
            .aggregation(AggregationBuilders.max(CommonName.AGG_NAME_MAX_TIME).field(CommonName.EXECUTION_END_TIME_FIELD))
            .trackTotalHits(false)
            .size(0);

        SearchRequest request = new SearchRequest(resultIndexAlias);
        request.source(source);
        if (resultIndex != null) {
            request.indices(resultIndex);
        }
        return request;
    }
}
