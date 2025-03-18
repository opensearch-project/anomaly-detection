/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.timeseries.constant.CommonMessages.FAIL_TO_GET_CONFIG_MSG;
import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.timeseries.util.ParseUtils.verifyResourceAccessAndProcessRequest;
import static org.opensearch.timeseries.util.RestHandlerUtils.PROFILE;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.constant.ADResourceScope;
import org.opensearch.ad.constant.ConfigConstants;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.EntityProfileRunner;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.ProfileRunner;
import org.opensearch.timeseries.TaskProfile;
import org.opensearch.timeseries.TaskProfileRunner;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ConfigProfile;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.EntityProfile;
import org.opensearch.timeseries.model.EntityProfileName;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.node.NodeClient;

import com.google.common.collect.Sets;

public abstract class BaseGetConfigTransportAction<GetConfigResponseType extends ActionResponse, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, ConfigType extends Config, EntityProfileActionType extends ActionType<EntityProfileResponse>, EntityProfileRunnerType extends EntityProfileRunner<EntityProfileActionType>, TaskProfileType extends TaskProfile<TaskClass>, ConfigProfileType extends ConfigProfile<TaskClass, TaskProfileType>, ProfileActionType extends ActionType<ProfileResponse>, TaskProfileRunnerType extends TaskProfileRunner<TaskClass, TaskProfileType>, ProfileRunnerType extends ProfileRunner<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType, TaskProfileType, TaskManagerType, ConfigProfileType, ProfileActionType, TaskProfileRunnerType>>
    extends HandledTransportAction<ActionRequest, GetConfigResponseType> {

    private static final Logger LOG = LogManager.getLogger(BaseGetConfigTransportAction.class);

    protected final ClusterService clusterService;
    protected final Client client;
    protected final SecurityClientUtil clientUtil;
    protected final Set<String> allProfileTypeStrs;
    protected final Set<ProfileName> allProfileTypes;
    protected final Set<ProfileName> defaultDetectorProfileTypes;
    protected final Set<String> allEntityProfileTypeStrs;
    protected final Set<EntityProfileName> allEntityProfileTypes;
    protected final Set<EntityProfileName> defaultEntityProfileTypes;
    protected final NamedXContentRegistry xContentRegistry;
    protected final DiscoveryNodeFilterer nodeFilter;
    protected final TransportService transportService;
    protected volatile Boolean filterByEnabled;
    protected final TaskManagerType taskManager;
    private final Class<ConfigType> configTypeClass;
    private final String configParseFieldName;
    private final List<TaskTypeEnum> allTaskTypes;
    private final String singleStreamRealTimeTaskName;
    private final String hcRealTImeTaskName;
    private final String singleStreamHistoricalTaskname;
    private final String hcHistoricalTaskName;
    private final TaskProfileRunnerType taskProfileRunner;
    private final boolean resourceSharingEnabled;
    private final Settings settings;
    private final NodeClient nodeClient;

    public BaseGetConfigTransportAction(
        TransportService transportService,
        DiscoveryNodeFilterer nodeFilter,
        ActionFilters actionFilters,
        ClusterService clusterService,
        Client client,
        SecurityClientUtil clientUtil,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        TaskManagerType forecastTaskManager,
        String getConfigAction,
        Class<ConfigType> configTypeClass,
        String configParseFieldName,
        List<TaskTypeEnum> allTaskTypes,
        String hcRealTImeTaskName,
        String singleStreamRealTimeTaskName,
        String hcHistoricalTaskName,
        String singleStreamHistoricalTaskname,
        Setting<Boolean> filterByBackendRoleEnableSetting,
        TaskProfileRunnerType taskProfileRunner,
        NodeClient nodeClient
    ) {
        super(getConfigAction, transportService, actionFilters, GetConfigRequest::new);
        this.clusterService = clusterService;
        this.client = client;
        this.clientUtil = clientUtil;

        List<ProfileName> allProfiles = Arrays.asList(ProfileName.values());
        this.allProfileTypes = EnumSet.copyOf(allProfiles);
        this.allProfileTypeStrs = Name.getListStrs(allProfiles);
        List<ProfileName> defaultProfiles = Arrays.asList(ProfileName.ERROR, ProfileName.STATE);
        this.defaultDetectorProfileTypes = new HashSet<>(defaultProfiles);

        List<EntityProfileName> allEntityProfiles = Arrays.asList(EntityProfileName.values());
        this.allEntityProfileTypes = EnumSet.copyOf(allEntityProfiles);
        this.allEntityProfileTypeStrs = Name.getListStrs(allEntityProfiles);
        List<EntityProfileName> defaultEntityProfiles = Arrays.asList(EntityProfileName.STATE);
        this.defaultEntityProfileTypes = new HashSet<>(defaultEntityProfiles);

        this.xContentRegistry = xContentRegistry;
        this.nodeFilter = nodeFilter;
        filterByEnabled = filterByBackendRoleEnableSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleEnableSetting, it -> filterByEnabled = it);
        this.transportService = transportService;
        this.taskManager = forecastTaskManager;
        this.configTypeClass = configTypeClass;
        this.configParseFieldName = configParseFieldName;
        this.allTaskTypes = allTaskTypes;
        this.hcRealTImeTaskName = hcRealTImeTaskName;
        this.singleStreamRealTimeTaskName = singleStreamRealTimeTaskName;
        this.hcHistoricalTaskName = hcHistoricalTaskName;
        this.singleStreamHistoricalTaskname = singleStreamHistoricalTaskname;
        this.taskProfileRunner = taskProfileRunner;
        this.resourceSharingEnabled = settings
            .getAsBoolean(ConfigConstants.OPENSEARCH_RESOURCE_SHARING_ENABLED, ConfigConstants.OPENSEARCH_RESOURCE_SHARING_ENABLED_DEFAULT);
        this.settings = settings;
        this.nodeClient = nodeClient;
    }

    @Override
    public void doExecute(Task task, ActionRequest request, ActionListener<GetConfigResponseType> actionListener) {
        GetConfigRequest getConfigRequest = GetConfigRequest.fromActionRequest(request);
        String configID = getConfigRequest.getConfigID();
        User user = ParseUtils.getUserContext(client);
        ActionListener<GetConfigResponseType> listener = wrapRestActionListener(actionListener, FAIL_TO_GET_CONFIG_MSG);

        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            if (resourceSharingEnabled) {
                // Call verifyResourceAccessAndProcessRequest before proceeding with get execution
                verifyResourceAccessAndProcessRequest(
                    user,
                    configID,
                    Set.of(ADResourceScope.AD_FULL_ACCESS.value()),
                    nodeClient,
                    settings,
                    listener,
                    args -> getExecute(getConfigRequest, listener) // Execute only if access is granted
                );
                return;
            }

            // Proceed with normal execution if resource sharing is not enabled
            resolveUserAndExecute(
                user,
                configID,
                filterByEnabled,
                listener,
                (config) -> getExecute(getConfigRequest, listener),
                client,
                clusterService,
                xContentRegistry,
                configTypeClass
            );
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    public void getConfigAndJob(
        String configID,
        boolean returnJob,
        boolean returnTask,
        Optional<TaskClass> realtimeConfigTask,
        Optional<TaskClass> historicalConfigTask,
        ActionListener<GetConfigResponseType> listener
    ) {
        MultiGetRequest.Item configItem = new MultiGetRequest.Item(CommonName.CONFIG_INDEX, configID);
        MultiGetRequest multiGetRequest = new MultiGetRequest().add(configItem);
        if (returnJob) {
            MultiGetRequest.Item adJobItem = new MultiGetRequest.Item(CommonName.JOB_INDEX, configID);
            multiGetRequest.add(adJobItem);
        }
        client
            .multiGet(
                multiGetRequest,
                onMultiGetResponse(listener, returnJob, returnTask, realtimeConfigTask, historicalConfigTask, configID)
            );
    }

    public void getExecute(GetConfigRequest request, ActionListener<GetConfigResponseType> listener) {
        String configID = request.getConfigID();
        String typesStr = request.getTypeStr();
        String rawPath = request.getRawPath();
        Entity entity = request.getEntity();
        boolean all = request.isAll();
        boolean returnJob = request.isReturnJob();
        boolean returnTask = request.isReturnTask();

        try {
            if (!Strings.isEmpty(typesStr) || rawPath.endsWith(PROFILE) || rawPath.endsWith(PROFILE + "/")) {
                getExecuteProfile(request, entity, typesStr, all, configID, listener);
            } else {
                if (returnTask) {
                    taskManager.getAndExecuteOnLatestTasks(configID, null, null, allTaskTypes, (taskList) -> {
                        Optional<TaskClass> realtimeTask = Optional.empty();
                        Optional<TaskClass> historicalTask = Optional.empty();
                        if (taskList != null && taskList.size() > 0) {
                            Map<String, TaskClass> tasks = new HashMap<>();
                            List<TaskClass> duplicateTasks = new ArrayList<>();
                            for (TaskClass task : taskList) {
                                if (tasks.containsKey(task.getTaskType())) {
                                    LOG
                                        .info(
                                            "Found duplicate latest task of config {}, task id: {}, task type: {}",
                                            configID,
                                            task.getTaskType(),
                                            task.getTaskId()
                                        );
                                    duplicateTasks.add(task);
                                    continue;
                                }
                                tasks.put(task.getTaskType(), task);
                            }
                            if (duplicateTasks.size() > 0) {
                                taskManager.resetLatestFlagAsFalse(duplicateTasks);
                            }

                            if (tasks.containsKey(hcRealTImeTaskName)) {
                                realtimeTask = Optional.ofNullable(tasks.get(hcRealTImeTaskName));
                            } else if (tasks.containsKey(singleStreamRealTimeTaskName)) {
                                realtimeTask = Optional.ofNullable(tasks.get(singleStreamRealTimeTaskName));
                            }
                            if (tasks.containsKey(hcHistoricalTaskName)) {
                                historicalTask = Optional.ofNullable(tasks.get(hcHistoricalTaskName));
                            } else if (tasks.containsKey(singleStreamHistoricalTaskname)) {
                                historicalTask = Optional.ofNullable(tasks.get(singleStreamHistoricalTaskname));
                            } else {
                                // AD needs to provides custom behavior for bwc, while forecasting can inherit
                                // the empty implementation
                                historicalTask = fillInHistoricalTaskforBwc(tasks);
                            }
                        }
                        getConfigAndJob(configID, returnJob, returnTask, realtimeTask, historicalTask, listener);
                    }, transportService, true, 2, listener);
                } else {
                    getConfigAndJob(configID, returnJob, returnTask, Optional.empty(), Optional.empty(), listener);
                }
            }
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

    private ActionListener<MultiGetResponse> onMultiGetResponse(
        ActionListener<GetConfigResponseType> listener,
        boolean returnJob,
        boolean returnTask,
        Optional<TaskClass> realtimeTask,
        Optional<TaskClass> historicalTask,
        String configId
    ) {
        return new ActionListener<MultiGetResponse>() {
            @Override
            public void onResponse(MultiGetResponse multiGetResponse) {
                MultiGetItemResponse[] responses = multiGetResponse.getResponses();
                ConfigType config = null;
                Job job = null;
                String id = null;
                long version = 0;
                long seqNo = 0;
                long primaryTerm = 0;

                for (MultiGetItemResponse response : responses) {
                    if (CommonName.CONFIG_INDEX.equals(response.getIndex())) {
                        if (response.getResponse() == null || !response.getResponse().isExists()) {
                            listener
                                .onFailure(
                                    new OpenSearchStatusException(CommonMessages.FAIL_TO_FIND_CONFIG_MSG + configId, RestStatus.NOT_FOUND)
                                );
                            return;
                        }
                        id = response.getId();
                        version = response.getResponse().getVersion();
                        primaryTerm = response.getResponse().getPrimaryTerm();
                        seqNo = response.getResponse().getSeqNo();
                        if (!response.getResponse().isSourceEmpty()) {
                            try (
                                XContentParser parser = RestHandlerUtils
                                    .createXContentParserFromRegistry(xContentRegistry, response.getResponse().getSourceAsBytesRef())
                            ) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                                config = parser.namedObject(configTypeClass, configParseFieldName, null);
                            } catch (Exception e) {
                                String message = "Failed to parse config " + configId;
                                listener.onFailure(buildInternalServerErrorResponse(e, message));
                                return;
                            }
                        }
                    } else if (CommonName.JOB_INDEX.equals(response.getIndex())) {
                        if (response.getResponse() != null
                            && response.getResponse().isExists()
                            && !response.getResponse().isSourceEmpty()) {
                            try (
                                XContentParser parser = RestHandlerUtils
                                    .createXContentParserFromRegistry(xContentRegistry, response.getResponse().getSourceAsBytesRef())
                            ) {
                                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                                job = Job.parse(parser);
                            } catch (Exception e) {
                                String message = "Failed to parse job " + configId;
                                listener.onFailure(buildInternalServerErrorResponse(e, message));
                                return;
                            }
                        }
                    }
                }
                listener
                    .onResponse(
                        createResponse(
                            version,
                            id,
                            primaryTerm,
                            seqNo,
                            config,
                            job,
                            returnJob,
                            realtimeTask,
                            historicalTask,
                            returnTask,
                            RestStatus.OK,
                            null,
                            null,
                            false
                        )
                    );
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        };
    }

    protected Optional<TaskClass> fillInHistoricalTaskforBwc(Map<String, TaskClass> tasks) {
        return Optional.empty();
    }

    protected void getExecuteProfile(
        GetConfigRequest request,
        Entity entity,
        String typesStr,
        boolean all,
        String configId,
        ActionListener<GetConfigResponseType> listener
    ) {
        if (entity != null) {
            Set<EntityProfileName> entityProfilesToCollect = getEntityProfilesToCollect(typesStr, all);
            EntityProfileRunnerType profileRunner = createEntityProfileRunner(
                client,
                clientUtil,
                xContentRegistry,
                TimeSeriesSettings.NUM_MIN_SAMPLES
            );
            profileRunner.profile(configId, entity, entityProfilesToCollect, ActionListener.wrap(profile -> {
                listener
                    .onResponse(
                        createResponse(
                            0,
                            null,
                            0,
                            0,
                            null,
                            null,
                            false,
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            null,
                            null,
                            profile,
                            true
                        )
                    );
            }, e -> listener.onFailure(e)));
        } else {
            Set<ProfileName> profilesToCollect = getProfilesToCollect(typesStr, all);
            ProfileRunnerType profileRunner = createProfileRunner(
                client,
                clientUtil,
                xContentRegistry,
                nodeFilter,
                TimeSeriesSettings.NUM_MIN_SAMPLES,
                transportService,
                taskManager,
                taskProfileRunner
            );
            profileRunner.profile(configId, getProfileActionListener(listener), profilesToCollect);
        }

    }

    protected abstract GetConfigResponseType createResponse(
        long version,
        String id,
        long primaryTerm,
        long seqNo,
        ConfigType config,
        Job job,
        boolean returnJob,
        Optional<TaskClass> realtimeTask,
        Optional<TaskClass> historicalTask,
        boolean returnTask,
        RestStatus restStatus,
        ConfigProfileType detectorProfile,
        EntityProfile entityProfile,
        boolean profileResponse
    );

    protected OpenSearchStatusException buildInternalServerErrorResponse(Exception e, String errorMsg) {
        LOG.error(errorMsg, e);
        return new OpenSearchStatusException(errorMsg, RestStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     *
     * @param typesStr a list of input profile types separated by comma
     * @param all whether we should return all profile in the response
     * @return profiles to collect for an entity
     */
    protected Set<EntityProfileName> getEntityProfilesToCollect(String typesStr, boolean all) {
        if (all) {
            return this.allEntityProfileTypes;
        } else if (Strings.isEmpty(typesStr)) {
            return this.defaultEntityProfileTypes;
        } else {
            // Filter out unsupported types
            Set<String> typesInRequest = new HashSet<>(Arrays.asList(typesStr.split(",")));
            return EntityProfileName.getNames(Sets.intersection(allEntityProfileTypeStrs, typesInRequest));
        }
    }

    /**
    *
    * @param typesStr a list of input profile types separated by comma
    * @param all whether we should return all profile in the response
    * @return profiles to collect for a detector
    */
    protected Set<ProfileName> getProfilesToCollect(String typesStr, boolean all) {
        if (all) {
            return this.allProfileTypes;
        } else if (Strings.isEmpty(typesStr)) {
            return this.defaultDetectorProfileTypes;
        } else {
            // Filter out unsupported types
            Set<String> typesInRequest = new HashSet<>(Arrays.asList(typesStr.split(",")));
            return ProfileName.getNames(Sets.intersection(allProfileTypeStrs, typesInRequest));
        }
    }

    protected ActionListener<ConfigProfileType> getProfileActionListener(ActionListener<GetConfigResponseType> listener) {
        return ActionListener.wrap(new CheckedConsumer<ConfigProfileType, Exception>() {
            @Override
            public void accept(ConfigProfileType profile) throws Exception {
                listener
                    .onResponse(
                        createResponse(
                            0,
                            null,
                            0,
                            0,
                            null,
                            null,
                            false,
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            null,
                            profile,
                            null,
                            true
                        )
                    );
            }
        }, exception -> { listener.onFailure(exception); });
    }

    protected abstract EntityProfileRunnerType createEntityProfileRunner(
        Client client,
        SecurityClientUtil clientUtil,
        NamedXContentRegistry xContentRegistry,
        long requiredSamples
    );

    protected abstract ProfileRunnerType createProfileRunner(
        Client client,
        SecurityClientUtil clientUtil,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        long requiredSamples,
        TransportService transportService,
        TaskManagerType taskManager,
        TaskProfileRunnerType taskProfileRunner
    );
}
