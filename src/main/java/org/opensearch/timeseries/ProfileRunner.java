/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.NOT_FOUND;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalCardinality;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.function.BiCheckedFunction;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ConfigProfile;
import org.opensearch.timeseries.model.ConfigState;
import org.opensearch.timeseries.model.InitProgressProfile;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.ModelProfileOnNode;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.transport.ProfileRequest;
import org.opensearch.timeseries.transport.ProfileResponse;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;

public abstract class ProfileRunner<TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskProfileType extends TaskProfile<TaskClass>, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, ConfigProfileType extends ConfigProfile<TaskClass, TaskProfileType>, ProfileActionType extends ActionType<ProfileResponse>, TaskProfileRunnerType extends TaskProfileRunner<TaskClass, TaskProfileType>>
    extends AbstractProfileRunner {

    private final Logger logger = LogManager.getLogger(ProfileRunner.class);
    protected Client client;
    protected SecurityClientUtil clientUtil;
    protected NamedXContentRegistry xContentRegistry;
    protected DiscoveryNodeFilterer nodeFilter;
    protected final TransportService transportService;
    protected final TaskManagerType taskManager;
    protected final int maxTotalEntitiesToTrack;
    protected final AnalysisType analysisType;
    protected final List<TaskTypeEnum> realTimeTaskTypes;
    protected final List<TaskTypeEnum> batchConfigTaskTypes;
    protected int maxCategoricalFields;
    protected ProfileName taskProfile;
    protected TaskProfileRunnerType taskProfileRunner;
    protected ProfileActionType profileAction;
    protected BiCheckedFunction<XContentParser, String, ? extends Config, IOException> configParser;
    protected String configIndexName;

    public ProfileRunner(
        Client client,
        SecurityClientUtil clientUtil,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        long requiredSamples,
        TransportService transportService,
        TaskManagerType taskManager,
        AnalysisType analysisType,
        List<TaskTypeEnum> realTimeTaskTypes,
        List<TaskTypeEnum> batchConfigTaskTypes,
        int maxCategoricalFields,
        ProfileName taskProfile,
        ProfileActionType profileAction,
        BiCheckedFunction<XContentParser, String, ? extends Config, IOException> configParser,
        TaskProfileRunnerType taskProfileRunner,
        String configIndexName
    ) {
        super(requiredSamples);
        this.client = client;
        this.clientUtil = clientUtil;
        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
        if (requiredSamples <= 0) {
            throw new IllegalArgumentException("required samples should be a positive number, but was " + requiredSamples);
        }
        this.transportService = transportService;
        this.taskManager = taskManager;
        this.maxTotalEntitiesToTrack = TimeSeriesSettings.MAX_TOTAL_ENTITIES_TO_TRACK;
        this.analysisType = analysisType;
        this.realTimeTaskTypes = realTimeTaskTypes;
        this.batchConfigTaskTypes = batchConfigTaskTypes;
        this.maxCategoricalFields = maxCategoricalFields;
        this.taskProfile = taskProfile;
        this.profileAction = profileAction;
        this.configParser = configParser;
        this.taskProfileRunner = taskProfileRunner;
        this.configIndexName = configIndexName;
    }

    public void profile(String configId, ActionListener<ConfigProfileType> listener, Set<ProfileName> profilesToCollect) {
        if (profilesToCollect.isEmpty()) {
            listener.onFailure(new IllegalArgumentException(CommonMessages.EMPTY_PROFILES_COLLECT));
            return;
        }
        calculateTotalResponsesToWait(configId, profilesToCollect, listener);
    }

    private void calculateTotalResponsesToWait(
        String configId,
        Set<ProfileName> profilesToCollect,
        ActionListener<ConfigProfileType> listener
    ) {
        GetRequest getConfigRequest = new GetRequest(configIndexName, configId);
        client.get(getConfigRequest, ActionListener.wrap(getConfigResponse -> {
            if (getConfigResponse != null && getConfigResponse.isExists()) {
                try (
                    XContentParser xContentParser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getConfigResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, xContentParser.nextToken(), xContentParser);
                    Config config = configParser.apply(xContentParser, configId);
                    prepareProfile(config, listener, profilesToCollect);
                } catch (Exception e) {
                    logger.error(CommonMessages.FAIL_TO_PARSE_CONFIG_MSG + configId, e);
                    listener.onFailure(new OpenSearchStatusException(CommonMessages.FAIL_TO_PARSE_CONFIG_MSG + configId, BAD_REQUEST));
                }
            } else {
                listener.onFailure(new OpenSearchStatusException(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + configId, NOT_FOUND));
            }
        }, exception -> {
            logger.error(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + configId, exception);
            listener.onFailure(new OpenSearchStatusException(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + configId, NOT_FOUND));
        }));
    }

    protected void prepareProfile(Config config, ActionListener<ConfigProfileType> listener, Set<ProfileName> profilesToCollect) {
        String configId = config.getId();
        GetRequest getRequest = new GetRequest(CommonName.JOB_INDEX, configId);
        client.get(getRequest, ActionListener.wrap(getResponse -> {
            if (getResponse != null && getResponse.isExists()) {
                try (
                    XContentParser parser = XContentType.JSON
                        .xContent()
                        .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, getResponse.getSourceAsString())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    Job job = Job.parse(parser);
                    long enabledTimeMs = job.getEnabledTime().toEpochMilli();

                    int totalResponsesToWait = 0;
                    if (profilesToCollect.contains(ProfileName.ERROR)) {
                        totalResponsesToWait++;
                    }

                    // total number of listeners we need to define. Needed by MultiResponsesDelegateActionListener to decide
                    // when to consolidate results and return to users
                    if (profilesToCollect.contains(ProfileName.TOTAL_ENTITIES)) {
                        totalResponsesToWait++;
                    }
                    if (profilesToCollect.contains(ProfileName.COORDINATING_NODE)
                        || profilesToCollect.contains(ProfileName.TOTAL_SIZE_IN_BYTES)
                        || profilesToCollect.contains(ProfileName.MODELS)
                        || profilesToCollect.contains(ProfileName.ACTIVE_ENTITIES)
                        || profilesToCollect.contains(ProfileName.INIT_PROGRESS)
                        || profilesToCollect.contains(ProfileName.STATE)) {
                        totalResponsesToWait++;
                    }
                    if (profilesToCollect.contains(taskProfile)) {
                        totalResponsesToWait++;
                    }

                    MultiResponsesDelegateActionListener<ConfigProfileType> delegateListener =
                        new MultiResponsesDelegateActionListener<ConfigProfileType>(
                            listener,
                            totalResponsesToWait,
                            CommonMessages.FAIL_FETCH_ERR_MSG + configId,
                            false
                        );
                    if (profilesToCollect.contains(ProfileName.ERROR)) {
                        taskManager.getAndExecuteOnLatestConfigLevelTask(configId, realTimeTaskTypes, task -> {
                            ConfigProfileType.Builder<TaskClass, TaskProfileType> profileBuilder = createProfileBuilder();
                            if (task.isPresent()) {
                                long lastUpdateTimeMs = task.get().getLastUpdateTime().toEpochMilli();

                                // if state index hasn't been updated, we should not use the error field
                                // For example, before a detector is enabled, if the error message contains
                                // the phrase "stopped due to blah", we should not show this when the detector
                                // is enabled.
                                if (lastUpdateTimeMs > enabledTimeMs && task.get().getError() != null) {
                                    profileBuilder.error(task.get().getError());
                                }
                                delegateListener.onResponse(profileBuilder.build());
                            } else {
                                // detector state for this detector does not exist
                                delegateListener.onResponse(profileBuilder.build());
                            }
                        }, transportService, false, delegateListener);
                    }

                    // total number of listeners we need to define. Needed by MultiResponsesDelegateActionListener to decide
                    // when to consolidate results and return to users
                    if (profilesToCollect.contains(ProfileName.TOTAL_ENTITIES)) {
                        profileEntityStats(delegateListener, config);
                    }
                    if (profilesToCollect.contains(ProfileName.COORDINATING_NODE)
                        || profilesToCollect.contains(ProfileName.TOTAL_SIZE_IN_BYTES)
                        || profilesToCollect.contains(ProfileName.MODELS)
                        || profilesToCollect.contains(ProfileName.ACTIVE_ENTITIES)
                        || profilesToCollect.contains(ProfileName.INIT_PROGRESS)
                        || profilesToCollect.contains(ProfileName.STATE)) {
                        profileModels(config, profilesToCollect, job, delegateListener);
                    }
                    if (profilesToCollect.contains(taskProfile)) {
                        getLatestHistoricalTaskProfile(configId, transportService, null, delegateListener);
                    }

                } catch (Exception e) {
                    logger.error(CommonMessages.FAIL_TO_GET_PROFILE_MSG, e);
                    listener.onFailure(e);
                }
            } else {
                onGetDetectorForPrepare(configId, listener, profilesToCollect);
            }
        }, exception -> {
            if (ExceptionUtil.isIndexNotAvailable(exception)) {
                logger.info(exception.getMessage());
                onGetDetectorForPrepare(configId, listener, profilesToCollect);
            } else {
                logger.error(CommonMessages.FAIL_TO_GET_PROFILE_MSG + configId);
                listener.onFailure(exception);
            }
        }));
    }

    private void profileEntityStats(MultiResponsesDelegateActionListener<ConfigProfileType> listener, Config config) {
        List<String> categoryField = config.getCategoryFields();
        if (!config.isHighCardinality() || categoryField.size() > maxCategoricalFields) {
            listener.onResponse(createProfileBuilder().build());
        } else {
            if (categoryField.size() == 1) {
                // Run a cardinality aggregation to count the cardinality of single category fields
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                CardinalityAggregationBuilder aggBuilder = new CardinalityAggregationBuilder(CommonName.TOTAL_ENTITIES);
                aggBuilder.field(categoryField.get(0));
                searchSourceBuilder.aggregation(aggBuilder);

                SearchRequest request = new SearchRequest(config.getIndices().toArray(new String[0]), searchSourceBuilder);
                final ActionListener<SearchResponse> searchResponseListener = ActionListener.wrap(searchResponse -> {
                    Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
                    InternalCardinality totalEntities = (InternalCardinality) aggMap.get(CommonName.TOTAL_ENTITIES);
                    long value = totalEntities.getValue();
                    ConfigProfileType.Builder<TaskClass, TaskProfileType> profileBuilder = createProfileBuilder();
                    ConfigProfileType profile = profileBuilder.totalEntities(value).build();
                    listener.onResponse(profile);
                }, searchException -> {
                    logger.warn(CommonMessages.FAIL_TO_GET_TOTAL_ENTITIES + config.getId());
                    listener.onFailure(searchException);
                });
                // using the original context in listener as user roles have no permissions for internal operations like fetching a
                // checkpoint
                clientUtil
                    .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                        request,
                        client::search,
                        config.getId(),
                        client,
                        analysisType,
                        searchResponseListener
                    );
            } else {
                // Run a composite query and count the number of buckets to decide cardinality of multiple category fields
                AggregationBuilder bucketAggs = AggregationBuilders
                    .composite(
                        CommonName.TOTAL_ENTITIES,
                        config.getCategoryFields().stream().map(f -> new TermsValuesSourceBuilder(f).field(f)).collect(Collectors.toList())
                    )
                    .size(maxTotalEntitiesToTrack);
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().aggregation(bucketAggs).trackTotalHits(false).size(0);
                SearchRequest searchRequest = new SearchRequest()
                    .indices(config.getIndices().toArray(new String[0]))
                    .source(searchSourceBuilder);
                final ActionListener<SearchResponse> searchResponseListener = ActionListener.wrap(searchResponse -> {
                    ConfigProfileType.Builder<TaskClass, TaskProfileType> profileBuilder = createProfileBuilder();
                    Aggregations aggs = searchResponse.getAggregations();
                    if (aggs == null) {
                        // This would indicate some bug or some opensearch core changes that we are not aware of (we don't keep up-to-date
                        // with
                        // the large amounts of changes there). For example, they may change to if there are results return it; otherwise
                        // return
                        // null instead of an empty Aggregations as they currently do.
                        logger.warn("Unexpected null aggregation.");
                        listener.onResponse(profileBuilder.totalEntities(0L).build());
                        return;
                    }

                    Aggregation aggrResult = aggs.get(CommonName.TOTAL_ENTITIES);
                    if (aggrResult == null) {
                        listener.onFailure(new IllegalArgumentException("Fail to find valid aggregation result"));
                        return;
                    }

                    CompositeAggregation compositeAgg = (CompositeAggregation) aggrResult;
                    ConfigProfileType profile = profileBuilder.totalEntities(Long.valueOf(compositeAgg.getBuckets().size())).build();
                    listener.onResponse(profile);
                }, searchException -> {
                    logger.warn(CommonMessages.FAIL_TO_GET_TOTAL_ENTITIES + config.getId());
                    listener.onFailure(searchException);
                });
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

        }
    }

    protected void onGetDetectorForPrepare(String configId, ActionListener<ConfigProfileType> listener, Set<ProfileName> profiles) {
        ConfigProfileType.Builder<TaskClass, TaskProfileType> profileBuilder = createProfileBuilder();
        if (profiles.contains(ProfileName.STATE)) {
            profileBuilder.state(ConfigState.DISABLED);
        }
        if (profiles.contains(taskProfile)) {
            getLatestHistoricalTaskProfile(configId, transportService, profileBuilder.build(), listener);
        } else {
            listener.onResponse(profileBuilder.build());
        }
    }

    /**
     * Profile models related
     *
     * @param config Config accessor
     * @param profiles profiles to collect
     * @param job Job accessor
     * @param listener returns collected profiles
     */
    protected void profileModels(
        Config config,
        Set<ProfileName> profiles,
        Job job,
        MultiResponsesDelegateActionListener<ConfigProfileType> listener
    ) {
        DiscoveryNode[] dataNodes = nodeFilter.getEligibleDataNodes();
        ProfileRequest profileRequest = new ProfileRequest(config.getId(), profiles, dataNodes);
        if (config.isLongInterval()) {
            ConfigProfileType.Builder<TaskClass, TaskProfileType> profile = createProfileBuilder();
            if (profiles.contains(ProfileName.COORDINATING_NODE)) {
                profile.coordinatingNode("");
            }
            if (profiles.contains(ProfileName.TOTAL_SIZE_IN_BYTES)) {
                profile.totalSizeInBytes(0L);
            }
            if (profiles.contains(ProfileName.MODELS)) {
                profile.modelProfile(new ModelProfileOnNode[0]);
                profile.modelCount(0);
            }
            if (config.isHighCardinality() && profiles.contains(ProfileName.ACTIVE_ENTITIES)) {
                profile.activeEntities(0L);
            }

            // only need to do it for models in priority cache. Single stream and HC are the same logic
            if (profiles.contains(ProfileName.INIT_PROGRESS) || profiles.contains(ProfileName.STATE)) {
                profileStateRelated(job, profiles, 0L, profile, config, listener);
            } else {
                listener.onResponse(profile.build());
            }
        } else {
            client.execute(profileAction, profileRequest, onModelResponse(config, profiles, job, listener));
        }
    }

    private ActionListener<ProfileResponse> onModelResponse(
        Config config,
        Set<ProfileName> profilesToCollect,
        Job job,
        MultiResponsesDelegateActionListener<ConfigProfileType> listener
    ) {
        boolean isMultientityDetector = config.isHighCardinality();
        return ActionListener.wrap(profileResponse -> {
            ConfigProfileType.Builder<TaskClass, TaskProfileType> profile = createProfileBuilder();
            if (profilesToCollect.contains(ProfileName.COORDINATING_NODE)) {
                profile.coordinatingNode(profileResponse.getCoordinatingNode());
            }
            if (profilesToCollect.contains(ProfileName.TOTAL_SIZE_IN_BYTES)) {
                profile.totalSizeInBytes(profileResponse.getTotalSizeInBytes());
            }
            if (profilesToCollect.contains(ProfileName.MODELS)) {
                profile.modelProfile(profileResponse.getModelProfile());
                profile.modelCount(profileResponse.getModelCount());
            }
            if (isMultientityDetector && profilesToCollect.contains(ProfileName.ACTIVE_ENTITIES)) {
                profile.activeEntities(profileResponse.getActiveEntities());
            }

            // only need to do it for models in priority cache. Single stream and HC are the same logic.
            if (profilesToCollect.contains(ProfileName.INIT_PROGRESS) || profilesToCollect.contains(ProfileName.STATE)) {
                profileStateRelated(job, profilesToCollect, profileResponse.getTotalUpdates(), profile, config, listener);
            } else {
                listener.onResponse(profile.build());
            }
        }, listener::onFailure);
    }

    private void profileStateRelated(
        Job job,
        Set<ProfileName> profilesToCollect,
        Long totalUpdates,
        ConfigProfileType.Builder<TaskClass, TaskProfileType> profileBuilder,
        Config config,
        MultiResponsesDelegateActionListener<ConfigProfileType> listener
    ) {
        if (job.isEnabled()) {
            if (totalUpdates < requiredSamples) {
                // need to double check for an HC analysis
                // since what ProfileResponse returns is the highest priority entity currently in memory, but
                // another entity might have already been initialized and sit somewhere else (in memory or on disk).
                long enabledTime = job.getEnabledTime().toEpochMilli();
                ProfileUtil
                    .confirmRealtimeResultStatus(
                        config,
                        enabledTime,
                        client,
                        analysisType,
                        onInittedEver(enabledTime, profileBuilder, profilesToCollect, config, totalUpdates, listener)
                    );
            } else {
                createRunningStateAndInitProgress(profilesToCollect, profileBuilder);
                listener.onResponse(profileBuilder.build());
            }
        } else {
            if (profilesToCollect.contains(ProfileName.STATE)) {
                profileBuilder.state(ConfigState.DISABLED);
            }
            listener.onResponse(profileBuilder.build());
        }
    }

    private ActionListener<SearchResponse> onInittedEver(
        long lastUpdateTimeMs,
        ConfigProfileType.Builder<TaskClass, TaskProfileType> profileBuilder,
        Set<ProfileName> profilesToCollect,
        Config config,
        long totalUpdates,
        MultiResponsesDelegateActionListener<ConfigProfileType> listener
    ) {
        return ActionListener.wrap(searchResponse -> {
            SearchHits hits = searchResponse.getHits();
            if (hits.getTotalHits().value == 0L) {
                processInitResponse(config, profilesToCollect, totalUpdates, false, profileBuilder, listener);
            } else {
                createRunningStateAndInitProgress(profilesToCollect, profileBuilder);
                listener.onResponse(profileBuilder.build());
            }
        }, exception -> {
            if (ExceptionUtil.isIndexNotAvailable(exception)) {
                // anomaly result index is not created yet
                processInitResponse(config, profilesToCollect, totalUpdates, false, profileBuilder, listener);
            } else {
                logger
                    .error(
                        "Fail to find any anomaly result with anomaly score larger than 0 after AD job enabled time for detector {}",
                        config.getId()
                    );
                listener.onFailure(exception);
            }
        });
    }

    protected void createRunningStateAndInitProgress(
        Set<ProfileName> profilesToCollect,
        ConfigProfileType.Builder<TaskClass, TaskProfileType> builder
    ) {
        if (profilesToCollect.contains(ProfileName.STATE)) {
            builder.state(ConfigState.RUNNING).build();
        }

        if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
            InitProgressProfile initProgress = new InitProgressProfile("100%", 0, 0);
            builder.initProgress(initProgress);
        }
    }

    protected void processInitResponse(
        Config config,
        Set<ProfileName> profilesToCollect,
        long totalUpdates,
        boolean hideMinutesLeft,
        ConfigProfileType.Builder<TaskClass, TaskProfileType> builder,
        MultiResponsesDelegateActionListener<ConfigProfileType> listener
    ) {
        if (profilesToCollect.contains(ProfileName.STATE)) {
            builder.state(ConfigState.INIT);
        }

        if (profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
            if (hideMinutesLeft) {
                InitProgressProfile initProgress = computeInitProgressProfile(totalUpdates, 0);
                builder.initProgress(initProgress);
            } else {
                long intervalMins = ((IntervalTimeConfiguration) config.getInterval()).toDuration().toMinutes();
                InitProgressProfile initProgress = computeInitProgressProfile(totalUpdates, intervalMins);
                builder.initProgress(initProgress);
            }
        }

        listener.onResponse(builder.build());
    }

    /**
     * Get latest historical config task profile.
     * Will not reset task state in this method.
     *
     * @param configId config id
     * @param transportService transport service
     * @param profile config profile
     * @param listener action listener
     */
    public void getLatestHistoricalTaskProfile(
        String configId,
        TransportService transportService,
        ConfigProfileType profile,
        ActionListener<ConfigProfileType> listener
    ) {
        taskManager.getAndExecuteOnLatestConfigTask(configId, null, null, batchConfigTaskTypes, task -> {
            if (task.isPresent()) {
                taskProfileRunner.getTaskProfile(task.get(), ActionListener.wrap(taskProfile -> {
                    ConfigProfileType.Builder<TaskClass, TaskProfileType> profileBuilder = createProfileBuilder();
                    profileBuilder.taskProfile(taskProfile);
                    ConfigProfileType configProfile = profileBuilder.build();
                    configProfile.merge(profile);
                    listener.onResponse(configProfile);
                }, e -> {
                    logger.error("Failed to get task profile for task " + task.get().getTaskId(), e);
                    listener.onFailure(e);
                }));
            } else {
                ConfigProfileType.Builder<TaskClass, TaskProfileType> profileBuilder = createProfileBuilder();
                listener.onResponse(profileBuilder.build());
            }
        }, transportService, false, listener);
    }

    protected abstract ConfigProfileType.Builder<TaskClass, TaskProfileType> createProfileBuilder();

}
