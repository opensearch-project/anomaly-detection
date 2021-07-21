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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.opensearch.search.aggregations.metrics.Percentile;
import org.opensearch.search.builder.SearchSourceBuilder;

public abstract class AbstractRetriever {
    protected double parseAggregation(Aggregation aggregation) {
        Double result = null;
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

    protected Optional<double[]> parseBucket(MultiBucketsAggregation.Bucket bucket, List<String> featureIds) {
        return parseAggregations(Optional.ofNullable(bucket).map(b -> b.getAggregations()), featureIds);
    }

    protected Optional<double[]> parseAggregations(Optional<Aggregations> aggregations, List<String> featureIds) {
        return aggregations
            .map(aggs -> aggs.asMap())
            .map(
                map -> featureIds
                    .stream()
                    .mapToDouble(id -> Optional.ofNullable(map.get(id)).map(this::parseAggregation).orElse(Double.NaN))
                    .toArray()
            )
            .filter(result -> Arrays.stream(result).noneMatch(d -> Double.isNaN(d) || Double.isInfinite(d)));
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
