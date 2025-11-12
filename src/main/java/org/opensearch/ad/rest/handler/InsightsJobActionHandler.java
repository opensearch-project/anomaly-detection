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

package org.opensearch.ad.rest.handler;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.indices.ADIndexManagement;
import org.opensearch.ad.transport.InsightsJobResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.Job;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.RestHandlerUtils;
import org.opensearch.transport.client.Client;

/**
 * Handler for Insights job operations.
 * Insights job is a global job that runs periodically to analyze
 * auto created detectors and generate insights.
 */
public class InsightsJobActionHandler {
    private static final Logger logger = LogManager.getLogger(InsightsJobActionHandler.class);

    // Default interval: 24 hours
    private static final int DEFAULT_INTERVAL_IN_HOURS = 24;

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final ADIndexManagement indexManagement;
    private final TimeValue requestTimeout;

    public InsightsJobActionHandler(
        Client client,
        NamedXContentRegistry xContentRegistry,
        ADIndexManagement indexManagement,
        TimeValue requestTimeout
    ) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.indexManagement = indexManagement;
        this.requestTimeout = requestTimeout;
    }

    /**
     * Start the insights job. Creates a new job or re-enables existing disabled job.
     * 
     * @param frequency Frequency string
     * @param listener Action listener for the response
     */
    public void startInsightsJob(String frequency, ActionListener<InsightsJobResponse> listener) {
        logger.info("Starting insights job with frequency: {}", frequency);

        // Get user context from current request (will be stored in job and used during execution)
        User user = ParseUtils.getUserContext(client);

        // init insights-results index
        indexManagement.initInsightsResultIndexIfAbsent(ActionListener.wrap(createIndexResponse -> {
            // create insights job
            ensureJobIndexAndCreateJob(frequency, user, listener);
        }, e -> {
            logger.error("Failed to initialize insights result index", e);
            listener.onFailure(e);
        }));
    }

    /**
     * Ensure job index exists, then create or enable the insights job.
     */
    private void ensureJobIndexAndCreateJob(String frequency, User user, ActionListener<InsightsJobResponse> listener) {
        if (!indexManagement.doesJobIndexExist()) {
            indexManagement.initJobIndex(ActionListener.wrap(response -> {
                if (response.isAcknowledged()) {
                    createOrEnableJob(frequency, user, listener);
                } else {
                    logger.warn("Created {} with mappings call not acknowledged", CommonName.JOB_INDEX);
                    listener
                        .onFailure(
                            new OpenSearchStatusException(
                                "Created " + CommonName.JOB_INDEX + " with mappings call not acknowledged",
                                RestStatus.INTERNAL_SERVER_ERROR
                            )
                        );
                }
            }, e -> {
                // If index already exists, proceed anyway
                if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                    createOrEnableJob(frequency, user, listener);
                } else {
                    logger.error("Failed to create job index", e);
                    listener.onFailure(e);
                }
            }));
        } else {
            createOrEnableJob(frequency, user, listener);
        }
    }

    /**
     * Get the status of the insights job
     * 
     * @param listener Action listener for the response containing job status
     */
    public void getInsightsJobStatus(ActionListener<InsightsJobResponse> listener) {
        GetRequest getRequest = new GetRequest(CommonName.JOB_INDEX).id(ADCommonName.INSIGHTS_JOB_NAME);

        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                // Job doesn't exist - return stopped status
                InsightsJobResponse statusResponse = new InsightsJobResponse(ADCommonName.INSIGHTS_JOB_NAME, false, null, null, null, null);
                listener.onResponse(statusResponse);
                return;
            }

            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                Job job = Job.parse(parser);

                // Return job status with all relevant fields
                InsightsJobResponse statusResponse = new InsightsJobResponse(
                    job.getName(),
                    job.isEnabled(),
                    job.getEnabledTime(),
                    job.getDisabledTime(),
                    job.getLastUpdateTime(),
                    job.getSchedule()
                );
                listener.onResponse(statusResponse);

            } catch (IOException e) {
                logger.error("Failed to parse insights job", e);
                listener.onFailure(new OpenSearchStatusException("Failed to parse insights job", RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, e -> {
            logger.error("Failed to get insights job status", e);
            listener.onFailure(e);
        }));
    }

    /**
     * Stop the insights job by disabling it
     * 
     * @param listener Action listener for the response
     */
    public void stopInsightsJob(ActionListener<InsightsJobResponse> listener) {
        GetRequest getRequest = new GetRequest(CommonName.JOB_INDEX).id(ADCommonName.INSIGHTS_JOB_NAME);

        client.get(getRequest, ActionListener.wrap(response -> {
            if (!response.isExists()) {
                listener.onResponse(new InsightsJobResponse("Insights job is not running"));
                return;
            }

            try (
                XContentParser parser = RestHandlerUtils.createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
            ) {
                ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                Job job = Job.parse(parser);

                if (!job.isEnabled()) {
                    listener.onResponse(new InsightsJobResponse("Insights job is already stopped"));
                    return;
                }

                Job disabledJob = new Job(
                    job.getName(),
                    job.getSchedule(),
                    job.getWindowDelay(),
                    false,
                    job.getEnabledTime(),
                    Instant.now(),
                    Instant.now(),
                    job.getLockDurationSeconds(),
                    job.getUser(),
                    job.getCustomResultIndexOrAlias(),
                    job.getAnalysisType()
                );

                indexJob(disabledJob, listener, "Insights job stopped successfully");

            } catch (IOException e) {
                logger.error("Failed to parse insights job", e);
                listener.onFailure(new OpenSearchStatusException("Failed to parse insights job", RestStatus.INTERNAL_SERVER_ERROR));
            }
        }, e -> {
            logger.error("Failed to get insights job", e);
            listener.onFailure(e);
        }));
    }

    /**
     * Create a new insights job or re-enable existing disabled job.
     */
    private void createOrEnableJob(String frequency, User user, ActionListener<InsightsJobResponse> listener) {
        GetRequest getRequest = new GetRequest(CommonName.JOB_INDEX).id(ADCommonName.INSIGHTS_JOB_NAME);

        client.get(getRequest, ActionListener.wrap(response -> {
            if (response.isExists()) {
                // Job exists, check if it's already enabled
                try (
                    XContentParser parser = RestHandlerUtils
                        .createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
                ) {
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    Job existingJob = Job.parse(parser);

                    if (existingJob.isEnabled()) {
                        logger.info("Insights job is already running");
                        listener.onResponse(new InsightsJobResponse("Insights job is already running"));
                        return;
                    }

                    // Use current user if provided, otherwise keep existing user (for BWC)
                    Job enabledJob = new Job(
                        existingJob.getName(),
                        createSchedule(frequency),
                        existingJob.getWindowDelay(),
                        true,
                        Instant.now(),
                        null,
                        Instant.now(),
                        existingJob.getLockDurationSeconds(),
                        user != null ? user : existingJob.getUser(),
                        existingJob.getCustomResultIndexOrAlias(),
                        existingJob.getAnalysisType()
                    );

                    indexJob(
                        enabledJob,
                        listener,
                        String.format(Locale.ROOT, "Insights job restarted successfully with frequency: %s", frequency)
                    );

                } catch (IOException e) {
                    logger.error("Failed to parse existing insights job", e);
                    listener
                        .onFailure(
                            new OpenSearchStatusException("Failed to parse existing insights job", RestStatus.INTERNAL_SERVER_ERROR)
                        );
                }
            } else {
                createNewJob(frequency, user, listener);
            }
        }, e -> {
            logger.error("Failed to check for existing insights job", e);
            listener.onFailure(e);
        }));
    }

    /**
     * Create a brand new insights job.
     */
    private void createNewJob(String frequency, User user, ActionListener<InsightsJobResponse> listener) {
        try {
            IntervalSchedule schedule = createSchedule(frequency);
            long lockDurationSeconds = java.time.Duration.of(schedule.getInterval(), schedule.getUnit()).getSeconds() * 2;

            IntervalTimeConfiguration windowDelay = new IntervalTimeConfiguration(0L, ChronoUnit.MINUTES);

            Job job = new Job(
                ADCommonName.INSIGHTS_JOB_NAME,
                schedule,
                windowDelay,
                true,
                Instant.now(),
                null,
                Instant.now(),
                lockDurationSeconds,
                user,
                ADCommonName.INSIGHTS_RESULT_INDEX_ALIAS,
                AnalysisType.AD
            );

            indexJob(job, listener, String.format(Locale.ROOT, "Insights job created successfully with frequency: %s", frequency));

        } catch (Exception e) {
            logger.error("Failed to create insights job", e);
            listener
                .onFailure(
                    new OpenSearchStatusException("Failed to create insights job: " + e.getMessage(), RestStatus.INTERNAL_SERVER_ERROR)
                );
        }
    }

    /**
     * Index the job document to the job index.
     */
    private void indexJob(Job job, ActionListener<InsightsJobResponse> listener, String successMessage) {
        try {
            IndexRequest indexRequest = new IndexRequest(CommonName.JOB_INDEX)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .source(job.toXContent(XContentFactory.jsonBuilder(), RestHandlerUtils.XCONTENT_WITH_TYPE))
                .timeout(requestTimeout)
                .id(job.getName());

            client
                .index(
                    indexRequest,
                    ActionListener.wrap(indexResponse -> { listener.onResponse(new InsightsJobResponse(successMessage)); }, e -> {
                        logger.error("Failed to index insights job", e);
                        listener.onFailure(e);
                    })
                );
        } catch (IOException e) {
            logger.error("Failed to create index request for insights job", e);
            listener.onFailure(new OpenSearchStatusException("Failed to create index request", RestStatus.INTERNAL_SERVER_ERROR));
        }
    }

    /**
     * Create an IntervalSchedule from frequency string 
     * e.g., "24h" -> 24 hours
     */
    private IntervalSchedule createSchedule(String frequency) {
        try {
            int interval = DEFAULT_INTERVAL_IN_HOURS;
            ChronoUnit unit = ChronoUnit.HOURS;

            if (frequency != null && !frequency.isEmpty()) {
                String lowerFreq = frequency.toLowerCase(Locale.ROOT).trim();

                if (lowerFreq.endsWith("h")) {
                    interval = Integer.parseInt(lowerFreq.substring(0, lowerFreq.length() - 1));
                    unit = ChronoUnit.HOURS;
                } else if (lowerFreq.endsWith("m")) {
                    interval = Integer.parseInt(lowerFreq.substring(0, lowerFreq.length() - 1));
                    unit = ChronoUnit.MINUTES;
                } else if (lowerFreq.endsWith("d")) {
                    interval = Integer.parseInt(lowerFreq.substring(0, lowerFreq.length() - 1));
                    unit = ChronoUnit.DAYS;
                } else {
                    interval = Integer.parseInt(lowerFreq);
                    unit = ChronoUnit.HOURS;
                }
            }

            return new IntervalSchedule(Instant.now(), interval, unit);

        } catch (NumberFormatException e) {
            logger.warn("Failed to parse frequency '{}', using default {}h", frequency, DEFAULT_INTERVAL_IN_HOURS);
            return new IntervalSchedule(Instant.now(), DEFAULT_INTERVAL_IN_HOURS, ChronoUnit.HOURS);
        }
    }

}
