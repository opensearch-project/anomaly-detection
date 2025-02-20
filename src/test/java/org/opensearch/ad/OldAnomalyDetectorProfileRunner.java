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

package org.opensearch.ad;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.util.Throwables;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.get.GetRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndex;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.model.ADTask;
import org.opensearch.ad.model.ADTaskProfile;
import org.opensearch.ad.model.ADTaskType;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.DetectorProfile;
import org.opensearch.ad.settings.ADNumericSetting;
import org.opensearch.ad.task.ADTaskCacheManager;
import org.opensearch.ad.task.ADTaskManager;
import org.opensearch.ad.transport.ADProfileAction;
import org.opensearch.ad.transport.RCFPollingAction;
import org.opensearch.ad.transport.RCFPollingRequest;
import org.opensearch.ad.transport.RCFPollingResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.ProfileRunner;
import org.opensearch.timeseries.common.exception.NotSerializedExceptionName;
import org.opensearch.timeseries.common.exception.ResourceNotFoundException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.ConfigState;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.model.ProfileName;
import org.opensearch.timeseries.util.DiscoveryNodeFilterer;
import org.opensearch.timeseries.util.ExceptionUtil;
import org.opensearch.timeseries.util.MultiResponsesDelegateActionListener;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

/**
 * Profile runner that deals with single stream detectors have different workflow (e.g., require RCFPollingAction
 * to get model updates). Keep the old code here so that I can write tests for old logic. Need to keep old code (e.g.,
 * RCFPollingAction and ModelManager.getTotalUpdates to deal with requests from old node during B/G).
 *
 */
public class OldAnomalyDetectorProfileRunner extends
    ProfileRunner<ADTaskCacheManager, ADTaskType, ADTask, ADIndex, ADIndexManagement, ADTaskProfile, ADTaskManager, DetectorProfile, ADProfileAction, ADTaskProfileRunner> {

    private final Logger logger = LogManager.getLogger(AnomalyDetectorProfileRunner.class);

    public OldAnomalyDetectorProfileRunner(
        Client client,
        SecurityClientUtil clientUtil,
        NamedXContentRegistry xContentRegistry,
        DiscoveryNodeFilterer nodeFilter,
        long requiredSamples,
        TransportService transportService,
        ADTaskManager adTaskManager,
        ADTaskProfileRunner taskProfileRunner
    ) {
        super(
            client,
            clientUtil,
            xContentRegistry,
            nodeFilter,
            requiredSamples,
            transportService,
            adTaskManager,
            AnalysisType.AD,
            ADTaskType.REALTIME_TASK_TYPES,
            ADTaskType.HISTORICAL_DETECTOR_TASK_TYPES,
            ADNumericSetting.maxCategoricalFields(),
            ProfileName.AD_TASK,
            ADProfileAction.INSTANCE,
            AnomalyDetector::parse,
            taskProfileRunner
        );
    }

    @Override
    protected DetectorProfile.Builder createProfileBuilder() {
        return new DetectorProfile.Builder();
    }

    @Override
    protected void prepareProfile(Config config, ActionListener<DetectorProfile> listener, Set<ProfileName> profilesToCollect) {
        boolean isHC = config.isHighCardinality();
        if (isHC) {
            super.prepareProfile(config, listener, profilesToCollect);
        } else {
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

                        if (profilesToCollect.contains(ProfileName.STATE) || profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
                            totalResponsesToWait++;
                        }
                        if (profilesToCollect.contains(ProfileName.COORDINATING_NODE)
                            || profilesToCollect.contains(ProfileName.TOTAL_SIZE_IN_BYTES)
                            || profilesToCollect.contains(ProfileName.MODELS)) {
                            totalResponsesToWait++;
                        }
                        if (profilesToCollect.contains(ProfileName.AD_TASK)) {
                            totalResponsesToWait++;
                        }

                        MultiResponsesDelegateActionListener<DetectorProfile> delegateListener =
                            new MultiResponsesDelegateActionListener<DetectorProfile>(
                                listener,
                                totalResponsesToWait,
                                CommonMessages.FAIL_FETCH_ERR_MSG + configId,
                                false
                            );
                        if (profilesToCollect.contains(ProfileName.ERROR)) {
                            taskManager.getAndExecuteOnLatestConfigLevelTask(configId, realTimeTaskTypes, task -> {
                                DetectorProfile.Builder profileBuilder = createProfileBuilder();
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

                        if (profilesToCollect.contains(ProfileName.STATE) || profilesToCollect.contains(ProfileName.INIT_PROGRESS)) {
                            profileStateRelated(config, delegateListener, job.isEnabled(), profilesToCollect);
                        }
                        if (profilesToCollect.contains(ProfileName.COORDINATING_NODE)
                            || profilesToCollect.contains(ProfileName.TOTAL_SIZE_IN_BYTES)
                            || profilesToCollect.contains(ProfileName.MODELS)) {
                            profileModels(config, profilesToCollect, job, delegateListener);
                        }
                        if (profilesToCollect.contains(ProfileName.AD_TASK)) {
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
    }

    /**
     * We expect three kinds of states:
     *  -Disabled: if get ad job api says the job is disabled;
     *  -Init: if rcf model's total updates is less than required
     *  -Running: if neither of the above applies and no exceptions.
     * @param config config accessor
     * @param listener listener to process the returned state or exception
     * @param enabled whether the detector job is enabled or not
     * @param profilesToCollect target profiles to fetch
     */
    private void profileStateRelated(
        Config config,
        MultiResponsesDelegateActionListener<DetectorProfile> listener,
        boolean enabled,
        Set<ProfileName> profilesToCollect
    ) {
        if (enabled) {
            RCFPollingRequest request = new RCFPollingRequest(config.getId());
            client.execute(RCFPollingAction.INSTANCE, request, onPollRCFUpdates(config, profilesToCollect, listener));
        } else {
            DetectorProfile.Builder builder = new DetectorProfile.Builder();
            if (profilesToCollect.contains(ProfileName.STATE)) {
                builder.state(ConfigState.DISABLED);
            }
            listener.onResponse(builder.build());
        }
    }

    /**
     * Listener for polling rcf updates through transport messaging
     * @param detector anomaly detector
     * @param profilesToCollect profiles to collect like state
     * @param listener delegate listener
     * @return Listener for polling rcf updates through transport messaging
     */
    private ActionListener<RCFPollingResponse> onPollRCFUpdates(
        Config detector,
        Set<ProfileName> profilesToCollect,
        MultiResponsesDelegateActionListener<DetectorProfile> listener
    ) {
        return ActionListener.wrap(rcfPollResponse -> {
            long totalUpdates = rcfPollResponse.getTotalUpdates();
            if (totalUpdates < requiredSamples) {
                processInitResponse(detector, profilesToCollect, totalUpdates, false, new DetectorProfile.Builder(), listener);
            } else {
                DetectorProfile.Builder builder = createProfileBuilder();
                createRunningStateAndInitProgress(profilesToCollect, builder);
                listener.onResponse(builder.build());
            }
        }, exception -> {
            // we will get an AnomalyDetectionException wrapping the real exception inside
            Throwable cause = Throwables.getRootCause(exception);

            // exception can be a RemoteTransportException
            Exception causeException = (Exception) cause;
            if (ExceptionUtil
                .isException(
                    causeException,
                    ResourceNotFoundException.class,
                    NotSerializedExceptionName.RESOURCE_NOT_FOUND_EXCEPTION_NAME_UNDERSCORE.getName()
                )
                || (ExceptionUtil.isIndexNotAvailable(causeException)
                    && causeException.getMessage().contains(ADCommonName.CHECKPOINT_INDEX_NAME))) {
                // cannot find checkpoint
                // We don't want to show the estimated time remaining to initialize
                // a detector before cold start finishes, where the actual
                // initialization time may be much shorter if sufficient historical
                // data exists.
                processInitResponse(detector, profilesToCollect, 0L, true, createProfileBuilder(), listener);
            } else {
                logger.error(new ParameterizedMessage("Fail to get init progress through messaging for {}", detector.getId()), exception);
                listener.onFailure(exception);
            }
        });
    }

}
