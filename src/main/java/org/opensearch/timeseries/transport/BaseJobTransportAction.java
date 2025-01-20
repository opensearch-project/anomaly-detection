/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.timeseries.util.ParseUtils.resolveUserAndExecute;
import static org.opensearch.timeseries.util.RestHandlerUtils.wrapRestActionListener;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.constant.ConfigConstants;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.ExecuteResultResponseRecorder;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.DateRange;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.model.TaskType;
import org.opensearch.timeseries.model.TimeSeriesTask;
import org.opensearch.timeseries.rest.handler.IndexJobActionHandler;
import org.opensearch.timeseries.task.TaskCacheManager;
import org.opensearch.timeseries.task.TaskManager;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.TransportService;

public abstract class BaseJobTransportAction<IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>, TaskCacheManagerType extends TaskCacheManager, TaskTypeEnum extends TaskType, TaskClass extends TimeSeriesTask, TaskManagerType extends TaskManager<TaskCacheManagerType, TaskTypeEnum, TaskClass, IndexType, IndexManagementType>, IndexableResultType extends IndexableResult, ProfileActionType extends ActionType<ProfileResponse>, ExecuteResultResponseRecorderType extends ExecuteResultResponseRecorder<IndexType, IndexManagementType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, IndexableResultType, ProfileActionType>, IndexJobActionHandlerType extends IndexJobActionHandler<IndexType, IndexManagementType, TaskCacheManagerType, TaskTypeEnum, TaskClass, TaskManagerType, IndexableResultType, ProfileActionType, ExecuteResultResponseRecorderType>>
    extends HandledTransportAction<JobRequest, JobResponse> {
    private final Logger logger = LogManager.getLogger(BaseJobTransportAction.class);

    private final Client client;
    private final ClusterService clusterService;
    private final Settings settings;
    private final NamedXContentRegistry xContentRegistry;
    private volatile Boolean filterByEnabled;
    private final TransportService transportService;
    private final Setting<TimeValue> requestTimeOutSetting;
    private final String failtoStartMsg;
    private final String failtoStopMsg;
    private final Class<? extends Config> configClass;
    private final IndexJobActionHandlerType indexJobActionHandlerType;
    private final boolean resourceSharingEnabled;

    public BaseJobTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        ClusterService clusterService,
        Settings settings,
        NamedXContentRegistry xContentRegistry,
        Setting<Boolean> filterByBackendRoleSettng,
        String jobActionName,
        Setting<TimeValue> requestTimeOutSetting,
        String failtoStartMsg,
        String failtoStopMsg,
        Class<? extends Config> configClass,
        IndexJobActionHandlerType indexJobActionHandlerType
    ) {
        super(jobActionName, transportService, actionFilters, JobRequest::new);
        this.transportService = transportService;
        this.client = client;
        this.clusterService = clusterService;
        this.settings = settings;
        this.xContentRegistry = xContentRegistry;
        filterByEnabled = filterByBackendRoleSettng.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleSettng, it -> filterByEnabled = it);
        this.requestTimeOutSetting = requestTimeOutSetting;
        this.failtoStartMsg = failtoStartMsg;
        this.failtoStopMsg = failtoStopMsg;
        this.configClass = configClass;
        this.indexJobActionHandlerType = indexJobActionHandlerType;
        this.resourceSharingEnabled = settings
            .getAsBoolean(ConfigConstants.OPENSEARCH_RESOURCE_SHARING_ENABLED, ConfigConstants.OPENSEARCH_RESOURCE_SHARING_ENABLED_DEFAULT);
    }

    @Override
    protected void doExecute(Task task, JobRequest request, ActionListener<JobResponse> actionListener) {
        String configId = request.getConfigID();
        DateRange dateRange = request.getDateRange();
        boolean historical = request.isHistorical();
        String rawPath = request.getRawPath();
        TimeValue requestTimeout = requestTimeOutSetting.get(settings);
        String errorMessage = rawPath.endsWith(RestHandlerUtils.START_JOB) ? failtoStartMsg : failtoStopMsg;
        ActionListener<JobResponse> listener = wrapRestActionListener(actionListener, errorMessage);

        // By the time request reaches here, the user permissions are validated by Security plugin.
        User user = ParseUtils.getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            resolveUserAndExecute(
                user,
                configId,
                filterByEnabled,
                listener,
                (config) -> executeConfig(listener, configId, dateRange, historical, rawPath, requestTimeout, user, context),
                client,
                clusterService,
                xContentRegistry,
                configClass,
                resourceSharingEnabled
            );
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    private void executeConfig(
        ActionListener<JobResponse> listener,
        String configId,
        DateRange dateRange,
        boolean historical,
        String rawPath,
        TimeValue requestTimeout,
        User user,
        ThreadContext.StoredContext context
    ) {
        if (rawPath.endsWith(RestHandlerUtils.START_JOB)) {
            indexJobActionHandlerType.startConfig(configId, dateRange, user, transportService, context, listener);
        } else if (rawPath.endsWith(RestHandlerUtils.STOP_JOB)) {
            indexJobActionHandlerType.stopConfig(configId, historical, user, transportService, listener);
        }
    }
}
