/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.model.Config;
import org.opensearch.transport.client.Client;

/**
 * Authorizes a caller's read access to the indices a config reads.
 *
 * A config's searches run under the identity stored on the config. Actions a user triggers also check the caller's own
 * read access to the config's indices before running the config.
 */
public final class ConfigIndexAccess {
    private static final Logger logger = LogManager.getLogger(ConfigIndexAccess.class);
    private static final String INJECTION_ID = "config-index-access";
    private static final String REMOTE_INDICES_AGG = "remote_indices";
    // the most remote indices a config's remote expressions may read; more fails the call
    static final int MAX_REMOTE_INDICES = 10_000;

    private ConfigIndexAccess() {}

    /**
     * Loads the config and runs {@code onAuthorized} once the caller may read every index it reads.
     *
     * Must be called with the stashed context current. {@code onAuthorized} runs with that context restored. A
     * missing config is left to {@code onAuthorized} to report.
     *
     * @param client node client
     * @param clusterService cluster service, to resolve the config's index expressions
     * @param xContentRegistry registry to parse the config with
     * @param configClass type of the config
     * @param configId id of the config
     * @param caller the caller, or null when security is disabled or the request is internal
     * @param callerContext the caller's context, as returned by stashing it
     * @param listener answered with the denial or the failure when the caller is not authorized
     * @param onAuthorized what to run once the caller is authorized
     */
    public static void authorizeCaller(
        Client client,
        ClusterService clusterService,
        NamedXContentRegistry xContentRegistry,
        Class<? extends Config> configClass,
        String configId,
        User caller,
        ThreadContext.StoredContext callerContext,
        ActionListener<?> listener,
        Runnable onAuthorized
    ) {
        if (caller == null) {
            run(onAuthorized, listener);
            return;
        }
        String configIndex = configClass == AnomalyDetector.class ? ADCommonName.CONFIG_INDEX : ForecastCommonName.CONFIG_INDEX;
        // Not ActionListener.wrap: it would hand an exception thrown by onAuthorized to the failure branch, which can run it again.
        client.get(new GetRequest(configIndex).id(configId), new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse response) {
                if (!response.isExists()) {
                    run(onAuthorized, listener);
                    return;
                }
                Config config;
                try (
                    XContentParser parser = RestHandlerUtils
                        .createXContentParserFromRegistry(xContentRegistry, response.getSourceAsBytesRef())
                ) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
                    config = Config.parseConfig(configClass, parser);
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }
                authorizeCaller(client, clusterService, config, caller, callerContext, listener, onAuthorized);
            }

            @Override
            public void onFailure(Exception e) {
                // a missing config index is left to onAuthorized to report
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                    run(onAuthorized, listener);
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    /**
     * Runs {@code onAuthorized} once the caller may read every index {@code config}'s index expressions resolve to.
     *
     * Local expressions are resolved against the local cluster state. Remote expressions ({@code cluster:index}) are
     * probed as they are and for every remote index the run would read: the indices a search under the identity stored
     * on the config, the identity the run reads them with, finds documents in.
     *
     * Must be called with the stashed context current. The probes run under the caller's context and
     * {@code onAuthorized} runs with the stashed context restored.
     *
     * @param client node client
     * @param clusterService cluster service, to resolve the index expressions
     * @param config the config, or null when there is no caller
     * @param caller the caller, or null when security is disabled or the request is internal
     * @param callerContext the caller's context, as returned by stashing it
     * @param listener answered with the denial or the failure when the caller is not authorized
     * @param onAuthorized what to run once the caller is authorized
     */
    public static void authorizeCaller(
        Client client,
        ClusterService clusterService,
        Config config,
        User caller,
        ThreadContext.StoredContext callerContext,
        ActionListener<?> listener,
        Runnable onAuthorized
    ) {
        if (caller == null || config == null || config.getIndices() == null || config.getIndices().isEmpty()) {
            run(onAuthorized, listener);
            return;
        }
        List<String> indices = config.getIndices();
        ThreadContext threadContext = client.threadPool().getThreadContext();
        ThreadContext.StoredContext stashedContext = threadContext.newStoredContext(false);
        Set<String> probed = new LinkedHashSet<>();
        List<String> remote = new ArrayList<>();
        try {
            resolveLocal(clusterService, new IndexNameExpressionResolver(threadContext), indices, probed, remote);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        if (remote.isEmpty()) {
            probeAsCaller(client, probed, indices, caller, callerContext, stashedContext, listener, onAuthorized);
            return;
        }
        listRemoteIndices(client, clusterService.getSettings(), config, remote, new ActionListener<Set<String>>() {
            @Override
            public void onResponse(Set<String> remoteIndices) {
                stashedContext.restore();
                // The expressions themselves are probed too, so the caller is checked even when the listing is empty.
                probed.addAll(remote);
                probed.addAll(remoteIndices);
                probeAsCaller(client, probed, indices, caller, callerContext, stashedContext, listener, onAuthorized);
            }

            @Override
            public void onFailure(Exception e) {
                stashedContext.restore();
                // The failure is the config owner's, not the caller's; don't pass the owner's identity on to the caller.
                logger.warn("Failed to list the remote indices {} of config {}", remote, config.getId(), e);
                listener
                    .onFailure(
                        new OpenSearchStatusException(
                            String.format(Locale.ROOT, "Failed to list the remote indices %s of the config", remote),
                            ExceptionsHelper.status(e)
                        )
                    );
            }
        });
    }

    private static void probeAsCaller(
        Client client,
        Set<String> probed,
        List<String> indices,
        User caller,
        ThreadContext.StoredContext callerContext,
        ThreadContext.StoredContext stashedContext,
        ActionListener<?> listener,
        Runnable onAuthorized
    ) {
        if (probed.isEmpty()) {
            // nothing exists to read; the run reports the missing indices
            run(onAuthorized, listener);
            return;
        }
        ActionListener<Void> probeListener = new ActionListener<Void>() {
            @Override
            public void onResponse(Void response) {
                stashedContext.restore();
                run(onAuthorized, listener);
            }

            @Override
            public void onFailure(Exception e) {
                stashedContext.restore();
                if (isDenial(e)) {
                    logger.info("User {} may not read one or more of {}", caller.getName(), indices);
                    listener.onFailure(denial(indices));
                } else {
                    // Not authorized either way: report the failure instead of running the config.
                    logger.debug("Index access probe for user {} failed", caller.getName(), e);
                    listener.onFailure(e);
                }
            }
        };
        callerContext.restore();
        try {
            probeAccess(client, probed, probeListener);
        } finally {
            stashedContext.restore();
        }
    }

    /**
     * Resolves the local expressions in {@code indices} to the concrete indices they read, so that an alias or a
     * pattern is authorized index by index rather than passing as soon as the caller may read one of its indices.
     * Remote expressions ({@code cluster:index}) cannot be resolved here and are added to {@code remote}.
     */
    static void resolveLocal(
        ClusterService clusterService,
        IndexNameExpressionResolver resolver,
        List<String> indices,
        Set<String> resolved,
        List<String> remote
    ) {
        for (String index : indices) {
            if (!CrossClusterConfigUtils.parseClusterAndIndexName(index).getKey().isEmpty()) {
                remote.add(index);
                continue;
            }
            for (String concrete : resolver.concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpandOpen(), true, index)) {
                resolved.add(concrete);
            }
        }
    }

    /**
     * Lists the remote indices {@code remote} reads, each as {@code cluster:index}, with a search under the roles of the
     * identity stored on {@code config} that aggregates on {@code _index}. That is the identity and the permission the
     * run reads them with, so the list is what the run would read. The caller's identity cannot be used: with the
     * security plugin's do_not_fail_on_forbidden, its search would be reduced to the indices the caller may read. An
     * index without documents is not listed; the run reads nothing from it.
     *
     * A listing that may be incomplete, because of a shard failure, a skipped cluster or more than
     * {@link #MAX_REMOTE_INDICES} indices, fails.
     *
     * Must be called with the stashed context current.
     */
    static void listRemoteIndices(
        Client client,
        Settings settings,
        Config config,
        List<String> remote,
        ActionListener<Set<String>> listener
    ) {
        try {
            SearchRequest request = new SearchRequest(remote.toArray(new String[0]))
                .source(
                    new SearchSourceBuilder()
                        .size(0)
                        .trackTotalHits(false)
                        .aggregation(AggregationBuilders.terms(REMOTE_INDICES_AGG).field("_index").size(MAX_REMOTE_INDICES))
                );
            AnalysisType analysisType = config instanceof AnomalyDetector ? AnalysisType.AD : AnalysisType.FORECAST;
            try (
                TimeSeriesSafeSecurityInjector injector = new TimeSeriesSafeSecurityInjector(
                    INJECTION_ID,
                    settings,
                    client.threadPool().getThreadContext(),
                    null,
                    analysisType
                )
            ) {
                injector.injectUserRoles(SecurityUtil.getUserFromConfig(config, settings));
                client.search(request, ActionListener.runBefore(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse response) {
                        Set<String> indices;
                        try {
                            indices = remoteIndices(response);
                        } catch (Exception e) {
                            listener.onFailure(e);
                            return;
                        }
                        listener.onResponse(indices);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                            // nothing exists to read; the expressions are still probed
                            listener.onResponse(new LinkedHashSet<>());
                        } else {
                            listener.onFailure(e);
                        }
                    }
                }, injector::close));
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * The remote indices of a listing search, each named {@code cluster:index}.
     */
    static Set<String> remoteIndices(SearchResponse response) {
        if (response.getFailedShards() > 0 || (response.getClusters() != null && response.getClusters().getSkipped() > 0)) {
            throw new IllegalStateException("the listing of the remote indices is incomplete");
        }
        Set<String> indices = new LinkedHashSet<>();
        Terms terms = response.getAggregations() == null ? null : response.getAggregations().get(REMOTE_INDICES_AGG);
        if (terms == null) {
            return indices;
        }
        if (terms.getSumOfOtherDocCounts() > 0) {
            throw new IllegalStateException("the config reads more than " + MAX_REMOTE_INDICES + " remote indices");
        }
        for (Terms.Bucket bucket : terms.getBuckets()) {
            String index = bucket.getKeyAsString();
            // a remote index without its cluster would be probed on the local cluster instead
            if (CrossClusterConfigUtils.parseClusterAndIndexName(index).getKey().isEmpty()) {
                throw new IllegalStateException("remote index " + index + " has no cluster");
            }
            indices.add(index);
        }
        return indices;
    }

    /**
     * Probes read access to {@code indices} under the current context and answers {@code listener} once every probe
     * has answered: with a denial if any index is denied, otherwise with any other probe failure, otherwise with
     * success. An index deleted since it was resolved is not a failure; the run reports it.
     *
     * One index per search is required. With the security plugin's do_not_fail_on_forbidden, a search over several
     * indices is reduced to the subset the caller may read and succeeds, so an index the caller cannot read would be
     * dropped from the search instead of being denied.
     */
    static void probeAccess(Client client, Set<String> indices, ActionListener<Void> listener) {
        AtomicInteger pending = new AtomicInteger(indices.size());
        AtomicReference<Exception> denial = new AtomicReference<>();
        AtomicReference<Exception> otherFailure = new AtomicReference<>();
        Runnable answer = () -> {
            if (pending.decrementAndGet() == 0) {
                Exception failure = denial.get() != null ? denial.get() : otherFailure.get();
                if (failure == null) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(failure);
                }
            }
        };
        for (String index : indices) {
            SearchRequest probe = new SearchRequest(index)
                .source(new SearchSourceBuilder().size(0).query(QueryBuilders.matchAllQuery()).trackTotalHits(false));
            client.search(probe, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    answer.run();
                }

                @Override
                public void onFailure(Exception e) {
                    if (isDenial(e)) {
                        denial.compareAndSet(null, e);
                    } else if (!(ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException)) {
                        otherFailure.compareAndSet(null, e);
                    }
                    answer.run();
                }
            });
        }
    }

    static boolean isDenial(Exception e) {
        return ExceptionsHelper.unwrapCause(e) instanceof OpenSearchSecurityException;
    }

    static OpenSearchStatusException denial(List<String> indices) {
        return new OpenSearchStatusException(
            String.format(Locale.ROOT, "User doesn't have read permissions for one or more configured index %s", indices),
            RestStatus.FORBIDDEN
        );
    }

    private static void run(Runnable onAuthorized, ActionListener<?> listener) {
        try {
            onAuthorized.run();
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
