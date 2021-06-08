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

/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.feature;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.opensearch.ad.common.exception.EndRunException;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.opensearch.search.aggregations.metrics.Percentile;

public class AbstractRetriever {
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
}
