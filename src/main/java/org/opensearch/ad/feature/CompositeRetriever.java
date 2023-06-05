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

package org.opensearch.ad.feature;

import java.io.IOException;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.util.SecurityClientUtil;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.util.ParseUtils;

/**
 *
 * Use pagination to fetch entities.  If there are more than pageSize entities,
 * we will fetch them in the next page. We implement pagination with composite query.
 * Results are decomposed into pages. Each page encapsulates aggregated values for
 * each entity and is sent to model nodes according to the hash ring mapping from
 * entity model Id to a data node.
 *
 */
public class CompositeRetriever extends AbstractRetriever {
    public static final String AGG_NAME_COMP = "comp_agg";
    private static final Logger LOG = LogManager.getLogger(CompositeRetriever.class);

    private final long dataStartEpoch;
    private final long dataEndEpoch;
    private final AnomalyDetector anomalyDetector;
    private final NamedXContentRegistry xContent;
    private final Client client;
    private final SecurityClientUtil clientUtil;
    private int totalResults;
    // we can process at most maxEntities entities
    private int maxEntities;
    private final int pageSize;
    private long expirationEpochMs;
    private Clock clock;
    private IndexNameExpressionResolver indexNameExpressionResolver;
    private ClusterService clusterService;

    public CompositeRetriever(
        long dataStartEpoch,
        long dataEndEpoch,
        AnomalyDetector anomalyDetector,
        NamedXContentRegistry xContent,
        Client client,
        SecurityClientUtil clientUtil,
        long expirationEpochMs,
        Clock clock,
        Settings settings,
        int maxEntitiesPerInterval,
        int pageSize,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService
    ) {
        this.dataStartEpoch = dataStartEpoch;
        this.dataEndEpoch = dataEndEpoch;
        this.anomalyDetector = anomalyDetector;
        this.xContent = xContent;
        this.client = client;
        this.clientUtil = clientUtil;
        this.totalResults = 0;
        this.maxEntities = maxEntitiesPerInterval;
        this.pageSize = pageSize;
        this.expirationEpochMs = expirationEpochMs;
        this.clock = clock;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.clusterService = clusterService;
    }

    // a constructor that provide default value of clock
    public CompositeRetriever(
        long dataStartEpoch,
        long dataEndEpoch,
        AnomalyDetector anomalyDetector,
        NamedXContentRegistry xContent,
        Client client,
        SecurityClientUtil clientUtil,
        long expirationEpochMs,
        Settings settings,
        int maxEntitiesPerInterval,
        int pageSize,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService
    ) {
        this(
            dataStartEpoch,
            dataEndEpoch,
            anomalyDetector,
            xContent,
            client,
            clientUtil,
            expirationEpochMs,
            Clock.systemUTC(),
            settings,
            maxEntitiesPerInterval,
            pageSize,
            indexNameExpressionResolver,
            clusterService
        );
    }

    /**
     * @return an iterator over pages
     * @throws IOException - if we cannot construct valid queries according to
     *  detector definition
     */
    public PageIterator iterator() throws IOException {
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(anomalyDetector.getTimeField())
            .gte(dataStartEpoch)
            .lt(dataEndEpoch)
            .format("epoch_millis");

        BoolQueryBuilder internalFilterQuery = new BoolQueryBuilder().filter(anomalyDetector.getFilterQuery()).filter(rangeQuery);

        // multiple categorical fields are supported
        CompositeAggregationBuilder composite = AggregationBuilders
            .composite(
                AGG_NAME_COMP,
                anomalyDetector.getCategoryFields().stream().map(f -> new TermsValuesSourceBuilder(f).field(f)).collect(Collectors.toList())
            )
            .size(pageSize);
        for (Feature feature : anomalyDetector.getFeatureAttributes()) {
            AggregatorFactories.Builder internalAgg = ParseUtils
                .parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
            composite.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
        }

        // In order to optimize the early termination it is advised to set track_total_hits in the request to false.
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(internalFilterQuery)
            .size(0)
            .aggregation(composite)
            .trackTotalHits(false);

        return new PageIterator(searchSourceBuilder);
    }

    public class PageIterator {
        private SearchSourceBuilder source;
        // a map from categorical field name to values (type: java.lang.Comparable)
        private Map<String, Object> afterKey;
        // number of iterations so far
        private int iterations;
        private long startMs;

        public PageIterator(SearchSourceBuilder source) {
            this.source = source;
            this.afterKey = null;
            this.iterations = 0;
            this.startMs = clock.millis();
        }

        /**
         * Results are returned using listener
         * @param listener Listener to return results
         */
        public void next(ActionListener<Page> listener) {
            iterations++;

            // inject user role while searching.

            SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().toArray(new String[0]), source);
            final ActionListener<SearchResponse> searchResponseListener = new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    processResponse(response, () -> client.search(searchRequest, this), listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            };
            // using the original context in listener as user roles have no permissions for internal operations like fetching a
            // checkpoint
            clientUtil
                .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                    searchRequest,
                    client::search,
                    anomalyDetector.getId(),
                    client,
                    searchResponseListener
                );
        }

        private void processResponse(SearchResponse response, Runnable retry, ActionListener<Page> listener) {
            try {
                if (shouldRetryDueToEmptyPage(response)) {
                    updateCompositeAfterKey(response, source);
                    retry.run();
                    return;
                }

                Page page = analyzePage(response);
                if (afterKey != null) {
                    updateCompositeAfterKey(response, source);
                }
                listener.onResponse(page);
            } catch (Exception ex) {
                listener.onFailure(ex);
            }
        }

        /**
         *
         * @param response current response
         * @return A page containing
         *  ** the after key
         *  ** query source builder to next page if any
         *  ** a map of composite keys to its values.  The values are arranged
         *    according to the order of anomalyDetector.getEnabledFeatureIds().
         */
        private Page analyzePage(SearchResponse response) {
            Optional<CompositeAggregation> compositeOptional = getComposite(response);

            if (false == compositeOptional.isPresent()) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Empty resposne: %s", response));
            }

            CompositeAggregation composite = compositeOptional.get();
            Map<Entity, double[]> results = new HashMap<>();
            /*
             *
             * Example composite aggregation:
             *
             "aggregations": {
                "my_buckets": {
                    "after_key": {
                        "service": "app_6",
                        "host": "server_3"
                    },
                    "buckets": [
                        {
                            "key": {
                                "service": "app_6",
                                "host": "server_3"
                            },
                            "doc_count": 1,
                            "the_max": {
                                "value": -38.0
                            },
                            "the_min": {
                                "value": -38.0
                            }
                        }
                    ]
               }
             }
             */
            for (Bucket bucket : composite.getBuckets()) {
                Optional<double[]> featureValues = parseBucket(bucket, anomalyDetector.getEnabledFeatureIds());
                // bucket.getKey() returns a map of categorical field like "host" and its value like "server_1"
                if (featureValues.isPresent() && bucket.getKey() != null) {
                    results.put(Entity.createEntityByReordering(bucket.getKey()), featureValues.get());
                }
            }

            totalResults += results.size();

            afterKey = composite.afterKey();
            return new Page(results);
        }

        private void updateCompositeAfterKey(SearchResponse r, SearchSourceBuilder search) {
            Optional<CompositeAggregation> composite = getComposite(r);

            if (false == composite.isPresent()) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Empty resposne: %s", r));
            }

            updateSourceAfterKey(composite.get().afterKey(), search);
        }

        private boolean shouldRetryDueToEmptyPage(SearchResponse response) {
            Optional<CompositeAggregation> composite = getComposite(response);
            // if there are no buckets but a next page, go fetch it instead of sending an empty response to the client
            if (false == composite.isPresent()) {
                return false;
            }
            CompositeAggregation aggr = composite.get();
            return aggr.getBuckets().isEmpty() && aggr.afterKey() != null && !aggr.afterKey().isEmpty();
        }

        Optional<CompositeAggregation> getComposite(SearchResponse response) {
            // When the source index is a regex like blah*, we will get empty response like
            // the following even if no index starting with blah exists.
            // {"took":0,"timed_out":false,"_shards":{"total":0,"successful":0,"skipped":0,"failed":0},"hits":{"max_score":0.0,"hits":[]}}
            // Without regex, we will get IndexNotFoundException instead.
            // {"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such
            // index
            // [blah]","index":"blah","resource.id":"blah","resource.type":"index_or_alias","index_uuid":"_na_"}],"type":"index_not_found_exception","reason":"no
            // such index
            // [blah]","index":"blah","resource.id":"blah","resource.type":"index_or_alias","index_uuid":"_na_"},"status":404}%
            if (response == null || response.getAggregations() == null) {
                List<String> sourceIndices = anomalyDetector.getIndices();
                String[] concreteIndices = indexNameExpressionResolver
                    .concreteIndexNames(clusterService.state(), IndicesOptions.lenientExpandOpen(), sourceIndices.toArray(new String[0]));
                if (concreteIndices.length == 0) {
                    throw new IndexNotFoundException(String.join(",", sourceIndices));
                } else {
                    return Optional.empty();
                }
            }
            Aggregation agg = response.getAggregations().get(AGG_NAME_COMP);
            if (agg == null) {
                // when current interval has no data
                return Optional.empty();
            }

            if (agg instanceof CompositeAggregation) {
                return Optional.of((CompositeAggregation) agg);
            }

            throw new IllegalArgumentException(String.format(Locale.ROOT, "Not a composite response; {}", agg.getClass()));
        }

        /**
         * Whether next page exists.  Conditions are:
         * 1) this is the first time we query (iterations == 0) or afterKey is not null
         * 2) next detection interval has not started
         * @return true if the iteration has more pages.
         */
        public boolean hasNext() {
            long now = clock.millis();
            if (expirationEpochMs <= now) {
                LOG
                    .debug(
                        new ParameterizedMessage(
                            "Time is up, afterKey: [{}], expirationEpochMs: [{}], now [{}]",
                            afterKey,
                            expirationEpochMs,
                            now
                        )
                    );
            }
            if ((iterations > 0 && afterKey == null) || totalResults > maxEntities) {
                LOG.debug(new ParameterizedMessage("Finished in [{}] msecs. ", (now - startMs)));
            }
            return (iterations == 0 || (totalResults > 0 && afterKey != null)) && expirationEpochMs > now && totalResults <= maxEntities;
        }

        @Override
        public String toString() {
            ToStringBuilder toStringBuilder = new ToStringBuilder(this);

            if (afterKey != null) {
                toStringBuilder.append("afterKey", afterKey);
            }
            if (source != null) {
                toStringBuilder.append("source", source);
            }

            return toStringBuilder.toString();
        }
    }

    public class Page {

        Map<Entity, double[]> results;

        public Page(Map<Entity, double[]> results) {
            this.results = results;
        }

        public boolean isEmpty() {
            return results == null || results.isEmpty();
        }

        public Map<Entity, double[]> getResults() {
            return results;
        }

        @Override
        public String toString() {
            ToStringBuilder toStringBuilder = new ToStringBuilder(this);

            if (results != null) {
                toStringBuilder.append("results", results);
            }

            return toStringBuilder.toString();
        }
    }
}
