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
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Entity;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.util.ParseUtils;
import org.opensearch.client.Client;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation.Bucket;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

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
    private int totalResults;
    private int maxEntities;
    private final int pageSize;
    private long expirationEpochMs;
    private Clock clock;

    public CompositeRetriever(
        long dataStartEpoch,
        long dataEndEpoch,
        AnomalyDetector anomalyDetector,
        NamedXContentRegistry xContent,
        Client client,
        long expirationEpochMs,
        Clock clock,
        Settings settings,
        int maxEntitiesPerInterval,
        int pageSize
    ) {
        this.dataStartEpoch = dataStartEpoch;
        this.dataEndEpoch = dataEndEpoch;
        this.anomalyDetector = anomalyDetector;
        this.xContent = xContent;
        this.client = client;
        this.totalResults = 0;
        this.maxEntities = maxEntitiesPerInterval;
        this.pageSize = pageSize;
        this.expirationEpochMs = expirationEpochMs;
        this.clock = clock;
    }

    // a constructor that provide default value of clock
    public CompositeRetriever(
        long dataStartEpoch,
        long dataEndEpoch,
        AnomalyDetector anomalyDetector,
        NamedXContentRegistry xContent,
        Client client,
        long expirationEpochMs,
        Settings settings,
        int maxEntitiesPerInterval,
        int pageSize
    ) {
        this(
            dataStartEpoch,
            dataEndEpoch,
            anomalyDetector,
            xContent,
            client,
            expirationEpochMs,
            Clock.systemUTC(),
            settings,
            maxEntitiesPerInterval,
            pageSize
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
                anomalyDetector.getCategoryField().stream().map(f -> new TermsValuesSourceBuilder(f).field(f)).collect(Collectors.toList())
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
        Map<String, Object> afterKey;

        public PageIterator(SearchSourceBuilder source) {
            this.source = source;
            this.afterKey = null;
        }

        /**
         * Results are returned using listener
         * @param listener Listener to return results
         */
        public void next(ActionListener<Page> listener) {
            SearchRequest searchRequest = new SearchRequest(anomalyDetector.getIndices().toArray(new String[0]), source);
            client.search(searchRequest, new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse response) {
                    processResponse(response, () -> client.search(searchRequest, this), listener);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        }

        private void processResponse(SearchResponse response, Runnable retry, ActionListener<Page> listener) {
            if (shouldRetryDueToEmptyPage(response)) {
                updateCompositeAfterKey(response, source);
                retry.run();
                return;
            }

            try {
                Page page = analyzePage(response);
                if (totalResults < maxEntities && afterKey != null) {
                    updateCompositeAfterKey(response, source);
                    listener.onResponse(page);
                } else {
                    listener.onResponse(null);
                }
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
                    results.put(Entity.createEntityByReordering(anomalyDetector.getDetectorId(), bucket.getKey()), featureValues.get());
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

        private void updateSourceAfterKey(Map<String, Object> afterKey, SearchSourceBuilder search) {
            AggregationBuilder aggBuilder = search.aggregations().getAggregatorFactories().iterator().next();
            // update after-key with the new value
            if (aggBuilder instanceof CompositeAggregationBuilder) {
                CompositeAggregationBuilder comp = (CompositeAggregationBuilder) aggBuilder;
                comp.aggregateAfter(afterKey);
            } else {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "Invalid client request; expected a composite builder but instead got {}", aggBuilder)
                );
            }
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
            if (response == null || response.getAggregations() == null) {
                return Optional.empty();
            }
            Aggregation agg = response.getAggregations().get(AGG_NAME_COMP);
            if (agg == null) {
                return Optional.empty();
            }

            if (agg instanceof CompositeAggregation) {
                return Optional.of((CompositeAggregation) agg);
            }

            throw new IllegalArgumentException(String.format(Locale.ROOT, "Not a composite response; {}", agg.getClass()));
        }

        /**
         * Whether next page exists.  Conditions are:
         * 1) we haven't fetched any page yet (totalResults == 0) or afterKey is not null
         * 2) next detection interval has not started
         * @return true if the iteration has more pages.
         */
        public boolean hasNext() {
            return (totalResults == 0 || (totalResults > 0 && afterKey != null)) && expirationEpochMs > clock.millis();
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
