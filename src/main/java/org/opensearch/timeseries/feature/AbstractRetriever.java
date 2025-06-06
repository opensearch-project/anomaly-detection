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

package org.opensearch.timeseries.feature;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.opensearch.search.aggregations.metrics.Percentile;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.common.exception.EndRunException;

public abstract class AbstractRetriever {
    protected double parseAggregation(Aggregation aggregationToParse) {
        Double result = null;
        /* example InternalSingleBucketAggregation: filter aggregation like
           "t_shirts": {
                "filter": {
                    "bool": {
                        "should": [
                            {
                                "term": {
                                    "issueType": "foo"
                                }
                            }
                            ...
                            ],
                            "minimum_should_match": "1",
                            "boost": 1
                    }
                },
                "aggs": {
                    "impactUniqueAccounts": {
                        "aggregation": {
                            "field": "account"
                        }
                    }
                }
            }
        
            would produce an InternalFilter (a subtype of InternalSingleBucketAggregation) with a sub-aggregation
            InternalCardinality that is also a SingleValue
        */

        if (aggregationToParse instanceof InternalSingleBucketAggregation) {
            InternalAggregations bucket = ((InternalSingleBucketAggregation) aggregationToParse).getAggregations();
            if (bucket != null) {
                List<Aggregation> aggrs = bucket.asList();
                if (aggrs.size() == 1) {
                    // we only accept a single value as feature
                    aggregationToParse = aggrs.get(0);
                }
            }
        }

        final Aggregation aggregation = aggregationToParse;
        if (aggregation instanceof SingleValue) {
            result = ((SingleValue) aggregation).value();
        } else if (aggregation instanceof InternalTDigestPercentiles) {
            Iterator<Percentile> percentile = ((InternalTDigestPercentiles) aggregation).iterator();
            if (percentile.hasNext()) {
                result = percentile.next().getValue();
            }
        }
        return Optional
            .ofNullable(result)
            .orElseThrow(() -> new EndRunException("Failed to parse aggregation " + aggregation, true).countedInStats(false));
    }

    /**
     * If it is feature with a filter, return the doc count for that feature; otherwise, return default count.
     * @param aggregationToParse
     * @param defaultCount
     * @return
     */
    protected long parseAggregationDocCount(Aggregation aggregationToParse, long defaultCount) {
        /* example InternalSingleBucketAggregation: filter aggregation like
           "t_shirts": {
                "filter": {
                    "bool": {
                        "should": [
                            {
                                "term": {
                                    "issueType": "foo"
                                }
                            }
                            ...
                            ],
                            "minimum_should_match": "1",
                            "boost": 1
                    }
                },
                "aggs": {
                    "impactUniqueAccounts": {
                        "aggregation": {
                            "field": "account"
                        }
                    }
                }
            }
        
            would produce an InternalFilter (a subtype of InternalSingleBucketAggregation) with a sub-aggregation
            InternalCardinality that is also a SingleValue
        */

        if (aggregationToParse instanceof InternalSingleBucketAggregation) {
            InternalSingleBucketAggregation bucket = (InternalSingleBucketAggregation) aggregationToParse;
            if (bucket != null) {
                return bucket.getDocCount();
            }
        }
        return defaultCount;
    }

    protected Optional<double[]> parseBucket(MultiBucketsAggregation.Bucket bucket, List<String> featureIds, boolean keepMissingValue) {
        return parseAggregations(Optional.ofNullable(bucket).map(b -> b.getAggregations()), featureIds, keepMissingValue);
    }

    protected Optional<double[]> parseAggregations(Optional<Aggregations> aggregations, List<String> featureIds, boolean keepMissingValue) {
        return aggregations
            .map(aggs -> aggs.asMap())
            .map(
                map -> featureIds
                    .stream()
                    .mapToDouble(id -> Optional.ofNullable(map.get(id)).map(this::parseAggregation).orElse(Double.NaN))
                    .toArray()
            )
            .flatMap(result -> {
                if (keepMissingValue) {
                    // Convert Double.isInfinite values to Double.NaN
                    return Optional.of(Arrays.stream(result).map(d -> Double.isInfinite(d) ? Double.NaN : d).toArray());
                } else {
                    // Return the array only if it contains no Double.NaN or Double.isInfinite
                    boolean noneNaNOrInfinite = Arrays.stream(result).noneMatch(d -> Double.isNaN(d) || Double.isInfinite(d));
                    return noneNaNOrInfinite ? Optional.of(result) : Optional.empty();
                }
            });
    }

    /**
     * Attempt to extract the per-feature document counts that live inside a
     * histogram / date-histogram **bucket**.
     *
     * <p>The method returns:</p>
     * <ul>
     *   <li><strong>{@code Optional.empty()}</strong> when
     *       <ul>
     *         <li>the bucket itself is {@code null},</li>
     *         <li>{@code bucket.getDocCount() == 0}, or</li>
     *         <li>any feature-specific sub-aggregation (e.g. a
     *             {@code filter} agg you created for that feature) reports
     *             {@code doc_count == 0}.</li>
     *       </ul>
     *   </li>
     *   <li><strong>{@code Optional<long[]>}</strong> (one element per
     *       {@code featureId}) when <em>all</em> counts are non-zero.</li>
     * </ul>
     */
    protected Optional<long[]> extractFeatureDocCounts(MultiBucketsAggregation.Bucket bucket, List<String> featureIds) {
        return Optional.ofNullable(bucket).flatMap(b -> {

            long parentCount = b.getDocCount();
            if (parentCount == 0) {
                return Optional.empty();                  // short-circuit
            }

            Map<String, Aggregation> subAggs = b.getAggregations().asMap();
            long[] counts = new long[featureIds.size()];

            for (int i = 0; i < featureIds.size(); i++) {
                String id = featureIds.get(i);
                Aggregation agg = subAggs.get(id);

                long count = parseAggregationDocCount(agg, parentCount);
                if (count == 0) {                         // any zero breaks continuity
                    return Optional.empty();
                }
                counts[i] = count;
            }

            return Optional.of(counts);
        });
    }

    protected void updateSourceAfterKey(Map<String, Object> afterKey, SearchSourceBuilder search) {
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
}
