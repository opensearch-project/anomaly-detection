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
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistroforelasticsearch.ad.feature;

import static com.amazon.opendistroforelasticsearch.ad.constant.CommonName.DATE_HISTOGRAM;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_ENTITIES_FOR_PREVIEW;
import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MAX_ENTITIES_PER_QUERY;
import static com.amazon.opendistroforelasticsearch.ad.util.ParseUtils.batchFeatureQuery;
import static org.apache.commons.math3.linear.MatrixUtils.createRealMatrix;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.aggregations.bucket.range.InternalDateRange;
import org.opensearch.search.aggregations.bucket.range.InternalDateRange.Bucket;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation.SingleValue;
import org.opensearch.search.aggregations.metrics.Percentile;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.AnomalyDetectorPlugin;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.dataprocessor.Interpolator;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.model.Entity;
import com.amazon.opendistroforelasticsearch.ad.model.Feature;
import com.amazon.opendistroforelasticsearch.ad.model.IntervalTimeConfiguration;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.ParseUtils;

/**
 * DAO for features from search.
 */
public class SearchFeatureDao {

    protected static final String AGG_NAME_MIN = "min_timefield";
    protected static final String AGG_NAME_TERM = "term_agg";

    private static final Logger logger = LogManager.getLogger(SearchFeatureDao.class);

    // Dependencies
    private final Client client;
    private final NamedXContentRegistry xContent;
    private final Interpolator interpolator;
    private final ClientUtil clientUtil;
    private ThreadPool threadPool;
    private int maxEntitiesPerQuery;
    private int maxEntitiesForPreview;

    /**
     * Constructor injection.
     *
     * @param client ES client for queries
     * @param xContent ES XContentRegistry
     * @param interpolator interpolator for missing values
     * @param clientUtil utility for ES client
     * @param threadPool accessor to different threadpools
     * @param settings ES settings
     * @param clusterService ES ClusterService
     */
    public SearchFeatureDao(
        Client client,
        NamedXContentRegistry xContent,
        Interpolator interpolator,
        ClientUtil clientUtil,
        ThreadPool threadPool,
        Settings settings,
        ClusterService clusterService
    ) {
        this.client = client;
        this.xContent = xContent;
        this.interpolator = interpolator;
        this.clientUtil = clientUtil;
        this.threadPool = threadPool;
        this.maxEntitiesPerQuery = MAX_ENTITIES_PER_QUERY.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ENTITIES_PER_QUERY, it -> maxEntitiesPerQuery = it);
        this.maxEntitiesForPreview = MAX_ENTITIES_FOR_PREVIEW.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_ENTITIES_FOR_PREVIEW, it -> maxEntitiesForPreview = it);
    }

    /**
     * Returns epoch time of the latest data under the detector.
     *
     * @deprecated use getLatestDataTime with listener instead.
     *
     * @param detector info about the indices and documents
     * @return epoch time of the latest data in milliseconds
     */
    @Deprecated
    public Optional<Long> getLatestDataTime(AnomalyDetector detector) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.max(CommonName.AGG_NAME_MAX_TIME).field(detector.getTimeField()))
            .size(0);
        SearchRequest searchRequest = new SearchRequest().indices(detector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        return clientUtil
            .<SearchRequest, SearchResponse>timedRequest(searchRequest, logger, client::search)
            .map(SearchResponse::getAggregations)
            .map(aggs -> aggs.asMap())
            .map(map -> (Max) map.get(CommonName.AGG_NAME_MAX_TIME))
            .map(agg -> (long) agg.getValue());
    }

    /**
     * Returns to listener the epoch time of the latset data under the detector.
     *
     * @param detector info about the data
     * @param listener onResponse is called with the epoch time of the latset data under the detector
     */
    public void getLatestDataTime(AnomalyDetector detector, ActionListener<Optional<Long>> listener) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .aggregation(AggregationBuilders.max(CommonName.AGG_NAME_MAX_TIME).field(detector.getTimeField()))
            .size(0);
        SearchRequest searchRequest = new SearchRequest().indices(detector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        client
            .search(
                searchRequest,
                ActionListener.wrap(response -> listener.onResponse(ParseUtils.getLatestDataTime(response)), listener::onFailure)
            );
    }

    /**
     * Get list of entities with high count in descending order within specified time range
     * @param detector detector config
     * @param startTime start time of time range
     * @param endTime end time of time range
     * @param listener listener to return back the entities
     */
    public void getHighestCountEntities(AnomalyDetector detector, long startTime, long endTime, ActionListener<List<Entity>> listener) {
        RangeQueryBuilder rangeQuery = new RangeQueryBuilder(detector.getTimeField())
            .from(startTime)
            .to(endTime)
            .format("epoch_millis")
            .includeLower(true)
            .includeUpper(false);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().filter(rangeQuery).filter(detector.getFilterQuery());
        TermsAggregationBuilder termsAgg = AggregationBuilders
            .terms(AGG_NAME_TERM)
            .field(detector.getCategoryField().get(0))
            .size(maxEntitiesForPreview);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(boolQueryBuilder)
            .aggregation(termsAgg)
            .trackTotalHits(false)
            .size(0);
        SearchRequest searchRequest = new SearchRequest().indices(detector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        ActionListener<SearchResponse> termsListener = ActionListener.wrap(response -> {
            Aggregations aggs = response.getAggregations();
            if (aggs == null) {
                listener.onResponse(Collections.emptyList());
                return;
            }

            List<Entity> results = aggs
                .asList()
                .stream()
                .filter(agg -> AGG_NAME_TERM.equals(agg.getName()))
                .flatMap(agg -> ((Terms) agg).getBuckets().stream())
                .map(bucket -> bucket.getKeyAsString())
                .collect(Collectors.toList())
                .stream()
                .map(entityValue -> new Entity(detector.getCategoryField().get(0), entityValue))
                .collect(Collectors.toList());
            listener.onResponse(results);
        }, listener::onFailure);
        client.search(searchRequest, termsListener);
    }

    /**
     * Get the entity's earliest and latest timestamps
     * @param detector detector config
     * @param entityName entity's name
     * @param listener listener to return back the requested timestamps
     */
    public void getEntityMinMaxDataTime(
        AnomalyDetector detector,
        String entityName,
        ActionListener<Entry<Optional<Long>, Optional<Long>>> listener
    ) {
        TermQueryBuilder term = new TermQueryBuilder(detector.getCategoryField().get(0), entityName);
        BoolQueryBuilder internalFilterQuery = QueryBuilders.boolQuery().filter(term);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
            .query(internalFilterQuery)
            .aggregation(AggregationBuilders.max(CommonName.AGG_NAME_MAX_TIME).field(detector.getTimeField()))
            .aggregation(AggregationBuilders.min(AGG_NAME_MIN).field(detector.getTimeField()))
            .trackTotalHits(false)
            .size(0);
        SearchRequest searchRequest = new SearchRequest().indices(detector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        client
            .search(
                searchRequest,
                ActionListener.wrap(response -> { listener.onResponse(parseMinMaxDataTime(response)); }, listener::onFailure)
            );
    }

    private Entry<Optional<Long>, Optional<Long>> parseMinMaxDataTime(SearchResponse searchResponse) {
        Optional<Map<String, Aggregation>> mapOptional = Optional
            .ofNullable(searchResponse)
            .map(SearchResponse::getAggregations)
            .map(aggs -> aggs.asMap());

        Optional<Long> latest = mapOptional.map(map -> (Max) map.get(CommonName.AGG_NAME_MAX_TIME)).map(agg -> (long) agg.getValue());

        Optional<Long> earliest = mapOptional.map(map -> (Min) map.get(AGG_NAME_MIN)).map(agg -> (long) agg.getValue());

        return new SimpleImmutableEntry<>(earliest, latest);
    }

    /**
     * Gets features for the given time period.
     * This function also adds given detector to negative cache before sending es request.
     * Once response/exception is received within timeout, this request will be treated as complete
     * and cleared from the negative cache.
     * Otherwise this detector entry remain in the negative to reject further request.
     *
     * @deprecated use getFeaturesForPeriod with listener instead.
     *
     * @param detector info about indices, documents, feature query
     * @param startTime epoch milliseconds at the beginning of the period
     * @param endTime epoch milliseconds at the end of the period
     * @throws IllegalStateException when unexpected failures happen
     * @return features from search results, empty when no data found
     */
    @Deprecated
    public Optional<double[]> getFeaturesForPeriod(AnomalyDetector detector, long startTime, long endTime) {
        SearchRequest searchRequest = createFeatureSearchRequest(detector, startTime, endTime, Optional.empty());

        // send throttled request: this request will clear the negative cache if the request finished within timeout
        return clientUtil
            .<SearchRequest, SearchResponse>throttledTimedRequest(searchRequest, logger, client::search, detector)
            .flatMap(resp -> parseResponse(resp, detector.getEnabledFeatureIds()));
    }

    /**
     * Returns to listener features for the given time period.
     *
     * @param detector info about indices, feature query
     * @param startTime epoch milliseconds at the beginning of the period
     * @param endTime epoch milliseconds at the end of the period
     * @param listener onResponse is called with features for the given time period.
     */
    public void getFeaturesForPeriod(AnomalyDetector detector, long startTime, long endTime, ActionListener<Optional<double[]>> listener) {
        SearchRequest searchRequest = createFeatureSearchRequest(detector, startTime, endTime, Optional.empty());
        client
            .search(
                searchRequest,
                ActionListener
                    .wrap(response -> listener.onResponse(parseResponse(response, detector.getEnabledFeatureIds())), listener::onFailure)
            );
    }

    public void getFeaturesForPeriodByBatch(
        AnomalyDetector detector,
        long startTime,
        long endTime,
        ActionListener<Map<Long, Optional<double[]>>> listener
    ) throws IOException {
        SearchSourceBuilder searchSourceBuilder = batchFeatureQuery(detector, startTime, endTime, xContent);
        logger.debug("Batch query for detector {}: {} ", detector.getDetectorId(), searchSourceBuilder);

        SearchRequest searchRequest = new SearchRequest(detector.getIndices().toArray(new String[0])).source(searchSourceBuilder);
        client
            .search(
                searchRequest,
                ActionListener
                    .wrap(
                        response -> { listener.onResponse(parseBucketAggregationResponse(response, detector.getEnabledFeatureIds())); },
                        listener::onFailure
                    )
            );
    }

    private Map<Long, Optional<double[]>> parseBucketAggregationResponse(SearchResponse response, List<String> featureIds) {
        Map<Long, Optional<double[]>> dataPoints = new HashMap<>();
        List<Aggregation> aggregations = response.getAggregations().asList();
        logger.debug("Feature aggregation result size {}", aggregations.size());
        for (Aggregation agg : aggregations) {
            List<InternalComposite.InternalBucket> buckets = ((InternalComposite) agg).getBuckets();
            buckets.forEach(bucket -> {
                Optional<double[]> featureData = parseAggregations(Optional.ofNullable(bucket.getAggregations()), featureIds);
                dataPoints.put((Long) bucket.getKey().get(DATE_HISTOGRAM), featureData);
            });
        }
        return dataPoints;
    }

    public Optional<double[]> parseResponse(SearchResponse response, List<String> featureIds) {
        return parseAggregations(Optional.ofNullable(response).map(resp -> resp.getAggregations()), featureIds);
    }

    private double parseAggregation(Aggregation aggregation) {
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

    /**
     * Gets samples of features for the time ranges.
     *
     * Sampled features are not true features. They are intended to be approximate results produced at low costs.
     *
     * @param detector info about the indices, documents, feature query
     * @param ranges list of time ranges
     * @param listener handle approximate features for the time ranges
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    public void getFeatureSamplesForPeriods(
        AnomalyDetector detector,
        List<Entry<Long, Long>> ranges,
        ActionListener<List<Optional<double[]>>> listener
    ) throws IOException {
        SearchRequest request = createPreviewSearchRequest(detector, ranges);

        client.search(request, ActionListener.wrap(response -> {
            Aggregations aggs = response.getAggregations();
            if (aggs == null) {
                listener.onResponse(Collections.emptyList());
                return;
            }

            listener
                .onResponse(
                    aggs
                        .asList()
                        .stream()
                        .filter(InternalDateRange.class::isInstance)
                        .flatMap(agg -> ((InternalDateRange) agg).getBuckets().stream())
                        .map(bucket -> parseBucket(bucket, detector.getEnabledFeatureIds()))
                        .collect(Collectors.toList())
                );
        }, listener::onFailure));
    }

    /**
     * Gets features for sampled periods.
     *
     * @deprecated use getFeaturesForSampledPeriods with listener instead.
     *
     * Sampling starts with the latest period and goes backwards in time until there are up to {@code maxSamples} samples.
     * If the initial stride {@code maxStride} results into a low count of samples, the implementation
     * may attempt with (exponentially) reduced strides and interpolate missing points.
     *
     * @param detector info about indices, documents, feature query
     * @param maxSamples the maximum number of samples to return
     * @param maxStride the maximum number of periods between samples
     * @param endTime the end time of the latest period
     * @return sampled features and stride, empty when no data found
     */
    @Deprecated
    public Optional<Entry<double[][], Integer>> getFeaturesForSampledPeriods(
        AnomalyDetector detector,
        int maxSamples,
        int maxStride,
        long endTime
    ) {
        Map<Long, double[]> cache = new HashMap<>();
        int currentStride = maxStride;
        Optional<double[][]> features = Optional.empty();
        logger.info(String.format(Locale.ROOT, "Getting features for detector %s starting %d", detector.getDetectorId(), endTime));
        while (currentStride >= 1) {
            boolean isInterpolatable = currentStride < maxStride;
            features = getFeaturesForSampledPeriods(detector, maxSamples, currentStride, endTime, cache, isInterpolatable);

            if (!features.isPresent() || features.get().length > maxSamples / 2 || currentStride == 1) {
                logger
                    .info(
                        String
                            .format(
                                Locale.ROOT,
                                "Get features for detector %s finishes with features present %b, current stride %d",
                                detector.getDetectorId(),
                                features.isPresent(),
                                currentStride
                            )
                    );
                break;
            } else {
                currentStride = currentStride / 2;
            }
        }
        if (features.isPresent()) {
            return Optional.of(new SimpleEntry<>(features.get(), currentStride));
        } else {
            return Optional.empty();
        }
    }

    private Optional<double[][]> getFeaturesForSampledPeriods(
        AnomalyDetector detector,
        int maxSamples,
        int stride,
        long endTime,
        Map<Long, double[]> cache,
        boolean isInterpolatable
    ) {
        ArrayDeque<double[]> sampledFeatures = new ArrayDeque<>(maxSamples);
        for (int i = 0; i < maxSamples; i++) {
            long span = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMillis();
            long end = endTime - span * stride * i;
            if (cache.containsKey(end)) {
                sampledFeatures.addFirst(cache.get(end));
            } else {
                Optional<double[]> features = getFeaturesForPeriod(detector, end - span, end);
                if (features.isPresent()) {
                    cache.put(end, features.get());
                    sampledFeatures.addFirst(features.get());
                } else if (isInterpolatable) {
                    Optional<double[]> previous = Optional.ofNullable(cache.get(end - span * stride));
                    Optional<double[]> next = Optional.ofNullable(cache.get(end + span * stride));
                    if (previous.isPresent() && next.isPresent()) {
                        double[] interpolants = getInterpolants(previous.get(), next.get());
                        cache.put(end, interpolants);
                        sampledFeatures.addFirst(interpolants);
                    } else {
                        break;
                    }
                } else {
                    break;
                }

            }
        }
        Optional<double[][]> samples;
        if (sampledFeatures.isEmpty()) {
            samples = Optional.empty();
        } else {
            samples = Optional.of(sampledFeatures.toArray(new double[0][0]));
        }
        return samples;
    }

    /**
     * Returns to listener features for sampled periods.
     *
     * Sampling starts with the latest period and goes backwards in time until there are up to {@code maxSamples} samples.
     * If the initial stride {@code maxStride} results into a low count of samples, the implementation
     * may attempt with (exponentially) reduced strides and interpolate missing points.
     *
     * @param detector info about indices, documents, feature query
     * @param maxSamples the maximum number of samples to return
     * @param maxStride the maximum number of periods between samples
     * @param endTime the end time of the latest period
     * @param listener onResponse is called with sampled features and stride between points, or empty for no data
     */
    public void getFeaturesForSampledPeriods(
        AnomalyDetector detector,
        int maxSamples,
        int maxStride,
        long endTime,
        ActionListener<Optional<Entry<double[][], Integer>>> listener
    ) {
        Map<Long, double[]> cache = new HashMap<>();
        logger.info(String.format(Locale.ROOT, "Getting features for detector %s ending at %d", detector.getDetectorId(), endTime));
        getFeatureSamplesWithCache(detector, maxSamples, maxStride, endTime, cache, maxStride, listener);
    }

    private void getFeatureSamplesWithCache(
        AnomalyDetector detector,
        int maxSamples,
        int maxStride,
        long endTime,
        Map<Long, double[]> cache,
        int currentStride,
        ActionListener<Optional<Entry<double[][], Integer>>> listener
    ) {
        getFeatureSamplesForStride(
            detector,
            maxSamples,
            maxStride,
            currentStride,
            endTime,
            cache,
            ActionListener
                .wrap(
                    features -> processFeatureSamplesForStride(
                        features,
                        detector,
                        maxSamples,
                        maxStride,
                        currentStride,
                        endTime,
                        cache,
                        listener
                    ),
                    listener::onFailure
                )
        );
    }

    private void processFeatureSamplesForStride(
        Optional<double[][]> features,
        AnomalyDetector detector,
        int maxSamples,
        int maxStride,
        int currentStride,
        long endTime,
        Map<Long, double[]> cache,
        ActionListener<Optional<Entry<double[][], Integer>>> listener
    ) {
        if (!features.isPresent()) {
            logger
                .info(
                    String
                        .format(
                            Locale.ROOT,
                            "Get features for detector %s finishes without any features present, current stride %d",
                            detector.getDetectorId(),
                            currentStride
                        )
                );
            listener.onResponse(Optional.empty());
        } else if (features.get().length > maxSamples / 2 || currentStride == 1) {
            logger
                .info(
                    String
                        .format(
                            Locale.ROOT,
                            "Get features for detector %s finishes with %d samples, current stride %d",
                            detector.getDetectorId(),
                            features.get().length,
                            currentStride
                        )
                );
            listener.onResponse(Optional.of(new SimpleEntry<>(features.get(), currentStride)));
        } else {
            getFeatureSamplesWithCache(detector, maxSamples, maxStride, endTime, cache, currentStride / 2, listener);
        }
    }

    private void getFeatureSamplesForStride(
        AnomalyDetector detector,
        int maxSamples,
        int maxStride,
        int currentStride,
        long endTime,
        Map<Long, double[]> cache,
        ActionListener<Optional<double[][]>> listener
    ) {
        ArrayDeque<double[]> sampledFeatures = new ArrayDeque<>(maxSamples);
        boolean isInterpolatable = currentStride < maxStride;
        long span = ((IntervalTimeConfiguration) detector.getDetectionInterval()).toDuration().toMillis();
        sampleForIteration(detector, cache, maxSamples, endTime, span, currentStride, sampledFeatures, isInterpolatable, 0, listener);
    }

    private void sampleForIteration(
        AnomalyDetector detector,
        Map<Long, double[]> cache,
        int maxSamples,
        long endTime,
        long span,
        int stride,
        ArrayDeque<double[]> sampledFeatures,
        boolean isInterpolatable,
        int iteration,
        ActionListener<Optional<double[][]>> listener
    ) {
        if (iteration < maxSamples) {
            long end = endTime - span * stride * iteration;
            if (cache.containsKey(end)) {
                sampledFeatures.addFirst(cache.get(end));
                sampleForIteration(
                    detector,
                    cache,
                    maxSamples,
                    endTime,
                    span,
                    stride,
                    sampledFeatures,
                    isInterpolatable,
                    iteration + 1,
                    listener
                );
            } else {
                getFeaturesForPeriod(detector, end - span, end, ActionListener.wrap(features -> {
                    if (features.isPresent()) {
                        cache.put(end, features.get());
                        sampledFeatures.addFirst(features.get());
                        sampleForIteration(
                            detector,
                            cache,
                            maxSamples,
                            endTime,
                            span,
                            stride,
                            sampledFeatures,
                            isInterpolatable,
                            iteration + 1,
                            listener
                        );
                    } else if (isInterpolatable) {
                        Optional<double[]> previous = Optional.ofNullable(cache.get(end - span * stride));
                        Optional<double[]> next = Optional.ofNullable(cache.get(end + span * stride));
                        if (previous.isPresent() && next.isPresent()) {
                            double[] interpolants = getInterpolants(previous.get(), next.get());
                            cache.put(end, interpolants);
                            sampledFeatures.addFirst(interpolants);
                            sampleForIteration(
                                detector,
                                cache,
                                maxSamples,
                                endTime,
                                span,
                                stride,
                                sampledFeatures,
                                isInterpolatable,
                                iteration + 1,
                                listener
                            );
                        } else {
                            listener.onResponse(toMatrix(sampledFeatures));
                        }
                    } else {
                        listener.onResponse(toMatrix(sampledFeatures));
                    }
                }, listener::onFailure));
            }
        } else {
            listener.onResponse(toMatrix(sampledFeatures));
        }
    }

    private Optional<double[][]> toMatrix(ArrayDeque<double[]> sampledFeatures) {
        Optional<double[][]> samples;
        if (sampledFeatures.isEmpty()) {
            samples = Optional.empty();
        } else {
            samples = Optional.of(sampledFeatures.toArray(new double[0][0]));
        }
        return samples;
    }

    private double[] getInterpolants(double[] previous, double[] next) {
        return transpose(interpolator.interpolate(transpose(new double[][] { previous, next }), 3))[1];
    }

    private double[][] transpose(double[][] matrix) {
        return createRealMatrix(matrix).transpose().getData();
    }

    private SearchRequest createFeatureSearchRequest(AnomalyDetector detector, long startTime, long endTime, Optional<String> preference) {
        // TODO: FeatureQuery field is planned to be removed and search request creation will migrate to new api.
        try {
            SearchSourceBuilder searchSourceBuilder = ParseUtils.generateInternalFeatureQuery(detector, startTime, endTime, xContent);
            return new SearchRequest(detector.getIndices().toArray(new String[0]), searchSourceBuilder).preference(preference.orElse(null));
        } catch (IOException e) {
            logger
                .warn(
                    "Failed to create feature search request for " + detector.getDetectorId() + " from " + startTime + " to " + endTime,
                    e
                );
            throw new IllegalStateException(e);
        }
    }

    private SearchRequest createPreviewSearchRequest(AnomalyDetector detector, List<Entry<Long, Long>> ranges) throws IOException {
        try {
            SearchSourceBuilder searchSourceBuilder = ParseUtils.generatePreviewQuery(detector, ranges, xContent);
            return new SearchRequest(detector.getIndices().toArray(new String[0]), searchSourceBuilder);
        } catch (IOException e) {
            logger.warn("Failed to create feature search request for " + detector.getDetectorId() + " for preview", e);
            throw e;
        }
    }

    private Optional<double[]> parseBucket(InternalDateRange.Bucket bucket, List<String> featureIds) {
        return parseAggregations(Optional.ofNullable(bucket).map(b -> b.getAggregations()), featureIds);
    }

    private Optional<double[]> parseAggregations(Optional<Aggregations> aggregations, List<String> featureIds) {
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

    public void getColdStartSamplesForPeriods(
        AnomalyDetector detector,
        List<Entry<Long, Long>> ranges,
        String entityName,
        boolean includesEmptyBucket,
        ActionListener<List<Optional<double[]>>> listener
    ) throws IOException {
        SearchRequest request = createColdStartFeatureSearchRequest(detector, ranges, entityName);

        client.search(request, ActionListener.wrap(response -> {
            Aggregations aggs = response.getAggregations();
            if (aggs == null) {
                listener.onResponse(Collections.emptyList());
                return;
            }

            long docCountThreshold = includesEmptyBucket ? -1 : 0;

            // Extract buckets and order by from_as_string. Currently by default it is ascending. Better not to assume it.
            // Example responses from date range bucket aggregation:
            // "aggregations":{"date_range":{"buckets":[{"key":"1598865166000-1598865226000","from":1.598865166E12,"
            // from_as_string":"1598865166000","to":1.598865226E12,"to_as_string":"1598865226000","doc_count":3,
            // "deny_max":{"value":154.0}},{"key":"1598869006000-1598869066000","from":1.598869006E12,
            // "from_as_string":"1598869006000","to":1.598869066E12,"to_as_string":"1598869066000","doc_count":3,
            // "deny_max":{"value":141.0}},
            // We don't want to use default 0 for sum/count aggregation as it might cause false positives during scoring.
            // Terms aggregation only returns non-zero count values. If we use a lot of 0s during cold start,
            // we will see alarming very easily.
            listener
                .onResponse(
                    aggs
                        .asList()
                        .stream()
                        .filter(InternalDateRange.class::isInstance)
                        .flatMap(agg -> ((InternalDateRange) agg).getBuckets().stream())
                        .filter(bucket -> bucket.getFrom() != null)
                        .filter(bucket -> bucket.getDocCount() > docCountThreshold)
                        .sorted(Comparator.comparing((Bucket bucket) -> Long.valueOf(bucket.getFromAsString())))
                        .map(bucket -> parseBucket(bucket, detector.getEnabledFeatureIds()))
                        .collect(Collectors.toList())
                );
        }, listener::onFailure));
    }

    /**
     * Get features by entities.  An entity is one combination of particular
     * categorical fields’ value. A categorical field in this setting refers to
     * an OpenSearch field of type keyword or ip.  Specifically, an entity
     * can be the IP address 182.3.4.5.
     * @param detector Accessor to the detector object
     * @param startMilli Start of time range to query
     * @param endMilli End of time range to query
     * @param listener Listener to return entities and their data points
     */
    public void getFeaturesByEntities(
        AnomalyDetector detector,
        long startMilli,
        long endMilli,
        ActionListener<Map<String, double[]>> listener
    ) {
        try {
            RangeQueryBuilder rangeQuery = new RangeQueryBuilder(detector.getTimeField())
                .gte(startMilli)
                .lt(endMilli)
                .format("epoch_millis");

            BoolQueryBuilder internalFilterQuery = new BoolQueryBuilder().filter(detector.getFilterQuery()).filter(rangeQuery);

            /* Terms aggregation implementation.*/
            // Support one category field
            TermsAggregationBuilder termsAgg = AggregationBuilders
                .terms(AGG_NAME_TERM)
                .field(detector.getCategoryField().get(0))
                .size(maxEntitiesPerQuery);
            for (Feature feature : detector.getFeatureAttributes()) {
                AggregatorFactories.Builder internalAgg = ParseUtils
                    .parseAggregators(feature.getAggregation().toString(), xContent, feature.getId());
                termsAgg.subAggregation(internalAgg.getAggregatorFactories().iterator().next());
            }

            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(internalFilterQuery)
                .size(0)
                .aggregation(termsAgg)
                .trackTotalHits(false);
            SearchRequest searchRequest = new SearchRequest(detector.getIndices().toArray(new String[0]), searchSourceBuilder);

            ActionListener<SearchResponse> termsListener = ActionListener.wrap(response -> {
                Aggregations aggs = response.getAggregations();
                if (aggs == null) {
                    listener.onResponse(Collections.emptyMap());
                    return;
                }

                Map<String, double[]> results = aggs
                    .asList()
                    .stream()
                    .filter(agg -> AGG_NAME_TERM.equals(agg.getName()))
                    .flatMap(agg -> ((Terms) agg).getBuckets().stream())
                    .collect(Collectors.toMap(Terms.Bucket::getKeyAsString, bucket -> parseBucket(bucket, detector.getEnabledFeatureIds())))
                    .entrySet()
                    .stream()
                    .filter(entry -> entry.getValue().isPresent())
                    .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().get()));

                listener.onResponse(results);
            }, listener::onFailure);

            client
                .search(
                    searchRequest,
                    new ThreadedActionListener<>(logger, threadPool, AnomalyDetectorPlugin.AD_THREAD_POOL_NAME, termsListener, false)
                );

        } catch (Exception e) {
            // TODO: catch concrete exception and check if they should be counted in stats or not
            throw new EndRunException(detector.getDetectorId(), CommonErrorMessages.INVALID_SEARCH_QUERY_MSG, e, false);
        }
    }

    private SearchRequest createColdStartFeatureSearchRequest(AnomalyDetector detector, List<Entry<Long, Long>> ranges, String entityName) {
        try {
            SearchSourceBuilder searchSourceBuilder = ParseUtils.generateEntityColdStartQuery(detector, ranges, entityName, xContent);
            return new SearchRequest(detector.getIndices().toArray(new String[0]), searchSourceBuilder);
        } catch (IOException e) {
            logger
                .warn(
                    "Failed to create cold start feature search request for "
                        + detector.getDetectorId()
                        + " from "
                        + ranges.get(0).getKey()
                        + " to "
                        + ranges.get(ranges.size() - 1).getKey(),
                    e
                );
            throw new IllegalStateException(e);
        }
    }

    private Optional<double[]> parseBucket(MultiBucketsAggregation.Bucket bucket, List<String> featureIds) {
        return parseAggregations(Optional.ofNullable(bucket).map(b -> b.getAggregations()), featureIds);
    }
}
