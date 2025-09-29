/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.transport;

import static org.opensearch.timeseries.util.ParseUtils.checkFilterByBackendRoles;
import static org.opensearch.timeseries.util.ParseUtils.verifyResourceAccessAndProcessRequest;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.tasks.Task;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.Name;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.function.ExecutorFunction;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.rest.handler.HistorySuggest;
import org.opensearch.timeseries.rest.handler.IntervalCalculation;
import org.opensearch.timeseries.rest.handler.LatestTimeRetriever;
import org.opensearch.timeseries.util.ParseUtils;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

public abstract class BaseSuggestConfigParamTransportAction extends
    HandledTransportAction<SuggestConfigParamRequest, SuggestConfigParamResponse> {
    public static final Logger logger = LogManager.getLogger(BaseSuggestConfigParamTransportAction.class);

    protected final Client client;
    protected final SecurityClientUtil clientUtil;
    protected final SearchFeatureDao searchFeatureDao;
    protected volatile Boolean filterByEnabled;
    protected Clock clock;
    protected AnalysisType context;
    protected Set<String> allSuggestParamStrs;
    private final Settings settings;

    public BaseSuggestConfigParamTransportAction(
        String actionName,
        Client client,
        SecurityClientUtil clientUtil,
        ClusterService clusterService,
        Settings settings,
        ActionFilters actionFilters,
        TransportService transportService,
        Setting<Boolean> filterByBackendRoleSetting,
        AnalysisType context,
        SearchFeatureDao searchFeatureDao,
        Set<String> allSuggestParamStrs
    ) {
        super(actionName, transportService, actionFilters, SuggestConfigParamRequest::new);
        this.client = client;
        this.clientUtil = clientUtil;
        this.filterByEnabled = filterByBackendRoleSetting.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(filterByBackendRoleSetting, it -> filterByEnabled = it);
        this.clock = Clock.systemUTC();
        this.context = context;
        this.searchFeatureDao = searchFeatureDao;
        this.allSuggestParamStrs = allSuggestParamStrs;
        this.settings = settings;
    }

    @Override
    protected void doExecute(Task task, SuggestConfigParamRequest request, ActionListener<SuggestConfigParamResponse> listener) {
        User user = ParseUtils.getUserContext(client);
        try (ThreadContext.StoredContext context = client.threadPool().getThreadContext().stashContext()) {
            verifyResourceAccessAndProcessRequest(
                settings,
                args -> suggestExecute(request, user, context, listener),
                new Object[] {},
                (fallbackArgs) -> resolveUserAndExecute(user, listener, () -> suggestExecute(request, user, context, listener)),
                new Object[] {}
            );
        } catch (Exception e) {
            logger.error(e);
            listener.onFailure(e);
        }
    }

    public void resolveUserAndExecute(User requestedUser, ActionListener<SuggestConfigParamResponse> listener, ExecutorFunction function) {
        try {
            // Check if user has backend roles
            // When filter by is enabled, block users who do not have backend roles.
            if (filterByEnabled) {
                String error = checkFilterByBackendRoles(requestedUser);
                if (error != null) {
                    listener.onFailure(new TimeSeriesException(error));
                    return;
                }
            }
            // Validate analysis
            function.execute();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected void suggestInterval(
        Config config,
        User user,
        TimeValue timeout,
        ActionListener<Pair<IntervalTimeConfiguration, Map<String, Object>>> listener
    ) {
        LatestTimeRetriever latestTimeRetriever = new LatestTimeRetriever(
            config,
            timeout,
            clientUtil,
            client,
            user,
            context,
            searchFeatureDao,
            // simulate run once and real time scenario where we operate relative to now
            true
        );

        ActionListener<Pair<Optional<Long>, Map<String, Object>>> latestTimeListener = ActionListener.wrap(latestEntityAttributes -> {
            Optional<Long> latestTime = latestEntityAttributes.getLeft();
            if (latestTime.isPresent()) {
                IntervalCalculation intervalCalculation = new IntervalCalculation(
                    config,
                    timeout,
                    client,
                    clientUtil,
                    user,
                    context,
                    clock,
                    searchFeatureDao,
                    latestTime.get(),
                    latestEntityAttributes.getRight(),
                    false
                );
                intervalCalculation
                    .findInterval(
                        ActionListener
                            .wrap(
                                interval -> listener.onResponse(Pair.of(interval, latestEntityAttributes.getRight())),
                                listener::onFailure
                            )
                    );
            } else {
                listener.onFailure(new TimeSeriesException("Empty data. Cannot find a good interval."));
            }

        }, exception -> {
            listener.onFailure(exception);
            logger.error("Failed to create search request for last data point", exception);
        });

        latestTimeRetriever.checkIfHC(latestTimeListener);
    }

    protected void suggestHistory(
        Config config,
        User user,
        TimeValue timeout,
        boolean suggestInterval,
        ActionListener<SuggestConfigParamResponse> listener
    ) {
        // if interval provided
        // if suggesting Interval is required, first go to suggestInterval call.
        if (!suggestInterval && config.getInterval() != null) {
            IntervalTimeConfiguration interval = new IntervalTimeConfiguration(config.getIntervalInMinutes(), ChronoUnit.MINUTES);
            HistorySuggest historySuggest = new HistorySuggest(
                config,
                user,
                searchFeatureDao,
                interval,
                // No entity information is provided, so we need to return an empty map.
                // This may result in less accurate historical data due to the lack of context.
                // Additionally, it could lead to inefficiencies by requiring more processing cycles to analyze past data.
                new HashMap<String, Object>(),
                clock
            );
            historySuggest
                .suggestHistory(
                    ActionListener
                        .wrap(
                            historyResponse -> listener
                                .onResponse(
                                    new SuggestConfigParamResponse.Builder()
                                        .history(historyResponse.getHistory())
                                        .interval(interval)
                                        .build()
                                ),
                            listener::onFailure
                        )
                );
            return;
        }
        suggestInterval(config, user, timeout, ActionListener.wrap(intervalEntity -> {
            HistorySuggest historySuggest = new HistorySuggest(
                config,
                user,
                searchFeatureDao,
                intervalEntity.getLeft(),
                intervalEntity.getRight(),
                clock
            );
            historySuggest
                .suggestHistory(
                    ActionListener
                        .wrap(
                            historyResponse -> listener
                                .onResponse(
                                    new SuggestConfigParamResponse.Builder()
                                        .history(historyResponse.getHistory())
                                        .interval(intervalEntity.getLeft())
                                        .build()
                                ),
                            listener::onFailure
                        )
                );
        }, listener::onFailure));

    }

    protected void suggestWindowDelay(Config config, User user, TimeValue timeout, ActionListener<SuggestConfigParamResponse> listener) {
        LatestTimeRetriever latestTimeRetriever = new LatestTimeRetriever(
            config,
            timeout,
            clientUtil,
            client,
            user,
            context,
            searchFeatureDao,
            // if future date is found, just set window delay to 0
            false
        );
        ActionListener<Pair<Optional<Long>, Map<String, Object>>> latestTimeListener = ActionListener.wrap(latestEntityAttributes -> {
            Optional<Long> latestTime = latestEntityAttributes.getLeft();
            if (latestTime.isPresent()) {
                // default is 1 minute. If we use 0, validation might give window delay error as
                // it might have a slightly larger current time than latest time
                long windowDelayMillis = 0;
                // we may get future date (e.g., in testing)
                long currentMillis = clock.millis();

                // ---------------------------------------------------------------------------
                // Adaptive window-delay calculation
                // ---------------------------------------------------------------------------
                // Goal: pick a delay long enough that all data for the current query
                // window has been ingested, so the config never sees “future gaps”.
                //
                // Algorithm
                // 1. Compute the raw lag (`gapMs`) between now and the newest document.
                // 2. Convert that lag into whole config-interval “buckets” (ceil).
                // We use the integer-math identity
                // ceil(a / b) = (a + b − 1) / b for positive a, b
                // so we never under-estimate the number of missing buckets.
                // 3. Add one extra “safety” bucket to cover clock skew / network jitter.
                // 4. Transform the final bucket count back to milliseconds.
                //
                // ---------------------------------------------------------------------------

                if (currentMillis > latestTime.get()) {

                    // Milliseconds we are behind real time
                    long gapMs = currentMillis - latestTime.get();

                    // Length of one bucket (config interval)
                    long bucketMs = config.getIntervalInMilliseconds();

                    /* Missing buckets (ceiling division)
                        Example: gap = 15 000 ms, bucket = 10 000 ms
                        bucketsBehind = (15 000 + 10 000 − 1) / 10 000
                                      = 24 999 / 10 000
                                      = 2  ← correct (15 000 ms spans 2 full buckets)
                    */
                    long bucketsBehind = (gapMs + bucketMs - 1) / bucketMs;

                    // Always keep one extra bucket as a cushion
                    long safetyBuckets = 1;

                    // Convert back to milliseconds
                    windowDelayMillis = (bucketsBehind + safetyBuckets) * bucketMs;
                }

                // in case windowDelayMillis is small, we want at least 1 minute
                listener
                    .onResponse(
                        new SuggestConfigParamResponse.Builder()
                            .windowDelay(new IntervalTimeConfiguration((long) Math.ceil(windowDelayMillis / 60000.0), ChronoUnit.MINUTES))
                            .build()
                    );
            } else {
                listener.onFailure(new TimeSeriesException("Cannot find a good window delay."));
            }
        }, listener::onFailure);

        latestTimeRetriever.checkIfHC(latestTimeListener);
    }

    public abstract void suggestExecute(
        SuggestConfigParamRequest request,
        User user,
        ThreadContext.StoredContext storedContext,
        ActionListener<SuggestConfigParamResponse> listener
    );

    /**
    *
    * @param typesStr a list of input suggest types separated by comma
    * @return parameters to suggest for a forecaster or detector
    */
    protected abstract Set<? extends Name> getParametersToSuggest(String typesStr);
}
