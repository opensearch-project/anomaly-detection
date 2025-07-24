/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import java.io.IOException;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.opensearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.opensearch.search.aggregations.pipeline.BucketHelpers;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.common.exception.ValidationException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.ValidationAspect;
import org.opensearch.timeseries.model.ValidationIssueType;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.util.SecurityClientUtil;
import org.opensearch.transport.client.Client;

public class IntervalCalculation {
    private final Logger logger = LogManager.getLogger(IntervalCalculation.class);
    // keep ≤256 buckets when searching for minimum interval
    private static final int BUCKET_CAP = 256;
    private static final int MAX_SPLIT_DEPTH = 10;

    private final AggregationPrep aggregationPrep;
    private final Client client;
    private final SecurityClientUtil clientUtil;
    private final User user;
    private final AnalysisType context;
    private final Clock clock;
    private final Map<String, Object> topEntity;
    private final long endMillis;
    private final Config config;
    // how many intervals to look back when exploring intervals
    private final int lookBackWindows;

    public IntervalCalculation(
        Config config,
        TimeValue requestTimeout,
        Client client,
        SecurityClientUtil clientUtil,
        User user,
        AnalysisType context,
        Clock clock,
        SearchFeatureDao searchFeatureDao,
        long latestTime,
        Map<String, Object> topEntity,
        boolean validate
    ) {
        this.aggregationPrep = new AggregationPrep(searchFeatureDao, requestTimeout, config);
        this.client = client;
        this.clientUtil = clientUtil;
        this.user = user;
        this.context = context;
        this.clock = clock;
        this.topEntity = topEntity;
        this.endMillis = latestTime;
        this.config = config;
        if (validate) {
            lookBackWindows = config.getHistoryIntervals();
        } else {
            // look back more when history is not finalized yet yet
            lookBackWindows = config.getDefaultHistory() * 2;
        }
    }

    public void findInterval(ActionListener<IntervalTimeConfiguration> listener) {
        ActionListener<IntervalTimeConfiguration> minimumIntervalListener = ActionListener.wrap(minInterval -> {
            logger.debug("minimum interval found: {}", minInterval);
            if (minInterval == null) {
                logger.debug("Fail to find minimum interval");
                listener.onResponse(null);
            } else {
                // starting exploring whether minimum or larger interval satisfy density requirement
                getBucketAggregates(minInterval, listener);
            }
        }, listener::onFailure);
        findMedianIntervalAdaptive(minimumIntervalListener);
    }

    private void getBucketAggregates(IntervalTimeConfiguration minimumInterval, ActionListener<IntervalTimeConfiguration> listener)
        throws IOException {

        try {
            LongBounds timeStampBounds = aggregationPrep.getTimeRangeBounds(minimumInterval, endMillis, lookBackWindows);
            SearchRequest searchRequest = aggregationPrep.createSearchRequest(minimumInterval, timeStampBounds, topEntity, 0);
            ActionListener<IntervalTimeConfiguration> intervalListener = ActionListener
                .wrap(interval -> listener.onResponse(interval), exception -> {
                    listener.onFailure(exception);
                    logger.error("Failed to get interval recommendation", exception);
                });
            final ActionListener<SearchResponse> searchResponseListener = new IntervalRecommendationListener(
                intervalListener,
                minimumInterval,
                clock.millis() + TimeSeriesSettings.TOP_VALIDATE_TIMEOUT_IN_MILLIS,
                timeStampBounds
            );
            // using the original context in listener as user roles have no permissions for internal operations like fetching a
            // checkpoint
            logger.debug("Interval explore search request: {}", searchRequest);
            clientUtil
                .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                    searchRequest,
                    client::search,
                    user,
                    client,
                    context,
                    searchResponseListener
                );
        } catch (ValidationException ex) {
            listener.onFailure(ex);
        }
    }

    /**
     * ActionListener class to handle execution of multiple bucket aggregations one after the other
     * Bucket aggregation with different interval lengths are executed one by one to check if the data is dense enough
     * We only need to execute the next query if the previous one led to data that is too sparse.
     */
    public class IntervalRecommendationListener implements ActionListener<SearchResponse> {
        private final ActionListener<IntervalTimeConfiguration> intervalListener;
        IntervalTimeConfiguration currentIntervalToTry;
        private final long expirationEpochMs;
        private LongBounds currentTimeStampBounds;
        private int attempts = 1;                       // the first query has fired

        public IntervalRecommendationListener(
            ActionListener<IntervalTimeConfiguration> intervalListener,
            IntervalTimeConfiguration currentIntervalToTry,
            long expirationEpochMs,
            LongBounds timeStampBounds
        ) {
            this.intervalListener = intervalListener;
            this.currentIntervalToTry = currentIntervalToTry;
            this.expirationEpochMs = expirationEpochMs;
            this.currentTimeStampBounds = timeStampBounds;
        }

        @Override
        public void onResponse(SearchResponse resp) {
            logger.debug("interval explorer response: {}", resp);
            try {
                long shingles = aggregationPrep.getShingleCount(resp);
                logger.debug("number of shingles: {}", shingles);

                if (shingles >= TimeSeriesSettings.NUM_MIN_SAMPLES) {     // dense enough
                    intervalListener.onResponse(currentIntervalToTry);
                    return;
                }

                if (++attempts > 10) {                                    // retry budget exhausted
                    logger.debug("number of attempts: {}", attempts);
                    intervalListener.onResponse(null);
                    return;
                }

                long nowMillis = clock.millis();
                if (nowMillis > expirationEpochMs) {                          // timeout
                    logger.debug("Timed out: now={}, expires={}", nowMillis, expirationEpochMs);
                    intervalListener
                        .onFailure(
                            new ValidationException(
                                CommonMessages.TIMEOUT_ON_INTERVAL_REC,
                                ValidationIssueType.TIMEOUT,
                                ValidationAspect.MODEL
                            )
                        );
                    return;
                }

                int nextMin = nextNiceInterval((int) currentIntervalToTry.getInterval());
                if (nextMin <= currentIntervalToTry.getInterval()) {           // cannot grow further
                    logger.debug("Cannot grow interval further: next={}, current={}", nextMin, currentIntervalToTry.getInterval());
                    intervalListener.onResponse(null);
                    return;
                }
                searchWithDifferentInterval(nextMin);

            } catch (Exception e) {
                onFailure(e);
            }
        }

        private void searchWithDifferentInterval(int newIntervalMinuteValue) {
            this.currentIntervalToTry = new IntervalTimeConfiguration(newIntervalMinuteValue, ChronoUnit.MINUTES);
            this.currentTimeStampBounds = aggregationPrep.getTimeRangeBounds(currentIntervalToTry, endMillis, lookBackWindows);
            // using the original context in listener as user roles have no permissions for internal operations like fetching a
            // checkpoint
            SearchRequest searchRequest = aggregationPrep.createSearchRequest(currentIntervalToTry, currentTimeStampBounds, topEntity, 0);
            logger.debug("next search request: {}", searchRequest);
            clientUtil
                .<SearchRequest, SearchResponse>asyncRequestWithInjectedSecurity(
                    searchRequest,
                    client::search,
                    user,
                    client,
                    context,
                    this
                );
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Failed to recommend new interval", e);
            intervalListener
                .onFailure(
                    new ValidationException(
                        CommonMessages.MODEL_VALIDATION_FAILED_UNEXPECTEDLY,
                        ValidationIssueType.AGGREGATION,
                        ValidationAspect.MODEL
                    )
                );
        }
    }

    /**
     * Adaptive Median Interval Discovery
     *
     * This method discovers a suitable data interval by first making a coarse
     * estimate and then refining it with a recursive search. The goal is to
     * find an interval that reflects the median time gap between data points.
     *
     * Phase-A (Coarse Sweep):
     * -----------------------
     * 1. A single search query is executed to find the MIN and MAX timestamps
     *    and the total number of documents in the time range.
     * 2. From these values, a naive "average" interval is calculated as:
     *    (MAX_timestamp - MIN_timestamp) / total_docs
     * 3. This serves as the initial guess (`initBucketMins`) for the refinement phase.
     *
     * Phase-B (Refinement):
     * ---------------------
     * The initial guess is passed to the `refineGap` method, which performs
     * a recursive, bidirectional search. It "zooms in" (halving the interval)
     * and "zooms out" (doubling the interval) to find a bucket size that is a
     * good approximation of the data's median interval. See the `refineGap`
     * method for implementation details.
     */
    public void findMedianIntervalAdaptive(ActionListener<IntervalTimeConfiguration> listener) {

        final long MIN_BUCKET_WIDTH_MINS = 1;
        final String TS_FIELD = config.getTimeField();

        /*
         * --------------------------------------------------------------
         * the common filter (time-range + caller predicates) *
         * --------------------------------------------------------------
         */
        BoolQueryBuilder baseFilter = QueryBuilders.boolQuery().filter(config.getFilterQuery());

        if (topEntity != null && !topEntity.isEmpty()) {
            Entity e = Entity.createEntityByReordering(topEntity);
            for (TermQueryBuilder tq : e.getTermQueryForCustomerIndex()) {
                baseFilter.filter(tq);
            }
        }

        /*
         * --------------------------------------------------------------
         * request: get min / max timestamp + total hits *
         *
         * "bounds" query built by boundsSrc
         *
         * (Everything under `"filter": […]` comes from     baseFilter.
         *  It already includes customer predicates and,            if
         *  present, the topEntity terms. No date-range yet — we are
         *  discovering min/max here.)
         *
         *  --------------------------------------------------------------------
         {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "match_all": {
                                "boost": 1
                            }
                        }
                    ],
                    "adjust_pure_negative": true,
                    "boost": 1
                }
            },
            "aggregations": {
                "min_ts": {
                    "min": {
                        "field": "timestamp"
                    }
                },
                "max_ts": {
                    "max": {
                        "field": "timestamp"
                    }
                }
            }
        }
         * --------------------------------------------------------------
         */
        SearchSourceBuilder boundsSrc = new SearchSourceBuilder()
            .size(0)
            .trackTotalHits(true)
            .query(baseFilter)
            .aggregation(AggregationBuilders.min("min_ts").field(TS_FIELD))
            .aggregation(AggregationBuilders.max("max_ts").field(TS_FIELD));

        SearchRequest boundsReq = new SearchRequest(config.getIndices().toArray(new String[0])).source(boundsSrc);

        client.search(boundsReq, ActionListener.wrap(r -> {

            Min minAgg = r.getAggregations().get("min_ts");
            Max maxAgg = r.getAggregations().get("max_ts");

            if (minAgg == null
                || maxAgg == null
                || Double.isInfinite(minAgg.getValue())
                || Double.isInfinite(maxAgg.getValue())
                || Double.isNaN(minAgg.getValue())
                || Double.isNaN(maxAgg.getValue())
                || minAgg.getValue() == maxAgg.getValue()) {
                listener.onResponse(null); // < 2 docs
                return;
            }

            long totalDocs = r.getHits().getTotalHits() == null ? 0L : r.getHits().getTotalHits().value();

            if (totalDocs < 2) {
                logger.debug("Exit early due to few docs");
                listener.onResponse(null);
                return;
            }

            long minMs = (long) minAgg.getValue();
            long maxMs = (long) maxAgg.getValue();
            long rangeMs = maxMs - minMs;

            // naive average: range / totalDocs
            // never create more than SOFT buckets on the first pass
            long naiveIntervalMs = rangeMs / totalDocs; // honour cadence
            long initBucketMins = Math.max(MIN_BUCKET_WIDTH_MINS, toCeilMinutes(naiveIntervalMs));

            final long MAX_RANGE_MS = 5L * 365 * 24 * 60 * 60 * 1000; // 5 years
            if (rangeMs > MAX_RANGE_MS) {
                logger.warn("Range ({}) exceeds max allowed ({}); clamping.", rangeMs, MAX_RANGE_MS);
                rangeMs = MAX_RANGE_MS;
            }

            refineGap(initBucketMins, -1, baseFilter, listener, MIN_BUCKET_WIDTH_MINS, ChronoUnit.MINUTES, TS_FIELD, 0, minMs, maxMs);

        }, listener::onFailure));
    }

    /* ----------------------------------------------------------------------
     * Recursive refinement with bidirectional zoom and client-side median calculation.
     *
     * This method recursively adjusts a date_histogram's interval (`bucketMins`) to find the
     * optimal interval for the data's cadence. It uses a bidirectional "zoom" strategy.
     *
     * zoomDir:
     *   -1: Zoom-IN  (halve the bucket size)
     *   +1: Zoom-OUT (double the bucket size)
     *
     * Algorithm:
     * 1. A `date_histogram` query is executed with the current `bucketMins`. It fetches
     *    the document count and the earliest timestamp for each bucket.
     *
     * 2. Client-Side Processing:
     *    - The response buckets are processed in the client.
     *    - Gaps (time differences) between all consecutive non-empty buckets are calculated.
     *    - The MEDIAN of these gaps is computed to find the data's typical interval.
     *
     * 3. Zooming Logic:
     *    - The algorithm begins by zooming IN.
     *    - It switches direction to zoom OUT if it's currently zooming in AND either:
     *        a) It detects empty buckets between buckets with data (meaning the current
     *           interval is too fine-grained).
     *        b) The bucket size reaches the configured minimum (`minBucketMins`).
     *
     * 4. Stopping Condition:
     *    - The recursion terminates when the current `bucketMins` is a "close enough"
     *      approximation of the calculated median gap. We consider it a fit if the
     *      median is within a factor of two of the current bucket size.
     *
     * Example Query (for bucketMins = 10):
     * --------------------------------------------------------------------
     * {
        "size": 0,
        "query": {
                "bool": {
                    "must": [
                        {
            "bool": {
                "filter": [
                    {
                        "match_all": {
                            "boost": 1
                                        }
                                    }
                                ],
                                "adjust_pure_negative": true,
                                "boost": 1
                            }
                        }
                    ],
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "from": 1615436160000,
                                    "to": null,
                                    "include_lower": true,
                                    "include_upper": true,
                                    "format": "epoch_millis",
                                    "boost": 1
                                }
                        }
                    }
                ],
                "adjust_pure_negative": true,
                "boost": 1
            }
        },
        "aggregations": {
            "dyn": {
                "date_histogram": {
                    "field": "timestamp",
                        "fixed_interval": "359m",
                    "offset": 0,
                    "order": {
                        "_key": "asc"
                    },
                    "keyed": false,
                    "min_doc_count": 0
                },
                "aggregations": {
                    "first_ts": {
                        "min": {
                            "field": "timestamp"
                        }
                        }
                    }
                }
            }
        }
     */
    public void refineGap(
        long bucketMins,
        int zoomDir,
        BoolQueryBuilder baseFilter,
        ActionListener<IntervalTimeConfiguration> listener,
        long minBucketMins,
        ChronoUnit returnUnit,
        String tsField,
        int depth,
        long sliceMinMs,
        long sliceMaxMs
    ) {
        if (depth > MAX_SPLIT_DEPTH) {          // fallback
            runAutoDate(baseFilter, listener, returnUnit, tsField);
            return;
        }

        BoolQueryBuilder filter = new BoolQueryBuilder();
        filter.must(baseFilter);              // shallow copy of all clauses
        long sliceRange = sliceMaxMs - sliceMinMs;
        /* -------- histogram request ---------------------------------- */
        long bucketMs = TimeUnit.MINUTES.toMillis(bucketMins);
        if (bucketMs > 0 && sliceRange / bucketMs > BUCKET_CAP) {                         // keep ≤256 buckets
            long windowMs = bucketMs * BUCKET_CAP;
            long sliceStart = sliceMaxMs - windowMs;
            filter.filter(QueryBuilders.rangeQuery(tsField).gte(sliceStart).format("epoch_millis"));  // ← add filter
        }

        SearchSourceBuilder src = new SearchSourceBuilder().size(0).query(filter);

        DateHistogramAggregationBuilder hist = AggregationBuilders
            .dateHistogram("dyn")
            .field(tsField)
            .fixedInterval(new DateHistogramInterval(bucketMins + "m"))
            // min_doc_count is set to 0 to ensure that all buckets in the time range are returned,
            // even if they are empty. This allows the client-side logic to detect empty buckets,
            // which is the signal to switch from zooming-in to zooming-out.
            .minDocCount(0);

        hist.subAggregation(AggregationBuilders.min("first_ts").field(tsField));

        src.aggregation(hist);

        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(src);
        logger.debug("Minimum interval search request: {}", searchRequest);
        client.search(searchRequest, ActionListener.wrap(resp -> {
            logger.debug("Minimum interval search response: {}", resp);
            double gap = Double.NaN;
            boolean hasEmptyBuckets = false;
            Histogram histogram = resp.getAggregations().get("dyn");
            if (histogram != null) {
                List<? extends Histogram.Bucket> buckets = histogram.getBuckets();
                int firstNonEmpty = -1;
                int lastNonEmpty = -1;
                for (int i = 0; i < buckets.size(); i++) {
                    if (buckets.get(i).getDocCount() > 0) {
                        if (firstNonEmpty == -1) {
                            firstNonEmpty = i;
                        }
                        lastNonEmpty = i;
                    }
                }

                if (firstNonEmpty != -1) {
                    for (int i = firstNonEmpty + 1; i < lastNonEmpty; i++) {
                        if (buckets.get(i).getDocCount() == 0) {
                            hasEmptyBuckets = true;
                            break;
                        }
                    }
                }

                List<Long> timestamps = buckets
                    .stream()
                    .filter(bucket -> bucket.getDocCount() > 0)
                    .map(bucket -> (Min) bucket.getAggregations().get("first_ts"))
                    .filter(min -> min != null && Double.isFinite(min.getValue()))
                    .map(min -> (long) min.getValue())
                    .collect(Collectors.toList());

                if (timestamps.size() >= 2) {
                    List<Long> gaps = new ArrayList<>();
                    for (int i = 1; i < timestamps.size(); i++) {
                        long currentGap = timestamps.get(i) - timestamps.get(i - 1);
                        if (currentGap > 0) {
                            gaps.add(currentGap);
                        }
                    }

                    if (!gaps.isEmpty()) {
                        gaps.sort(null);
                        int size = gaps.size();
                        int middle = size / 2;
                        if (size % 2 == 1) {
                            gap = gaps.get(middle);
                        } else if (size > 0) {
                            gap = (gaps.get(middle - 1) + gaps.get(middle)) / 2.0;
                        }
                    }
                }
            }

            long gapMins = toCeilMinutes(gap);

            /*
             * Stop when the bucket size is a reasonable estimate of the median gap.
             * Since we are now looking for the median, we should stop when our bucket
             * interval is in the same ballpark as the calculated median.
             * We consider it "close enough" if the median is within a factor of 2 of
             * the bucket size.
             */
            if (!Double.isNaN(gap) && gap > 0) {
                if (gapMins > bucketMins / 2.0 && gapMins < bucketMins * 2.0) {
                    listener.onResponse(new IntervalTimeConfiguration(Math.max(1, gapMins), returnUnit));
                    return;
                }
            }

            long nextBucketMins;
            int nextDir = zoomDir;

            if (zoomDir < 0) { // zooming in
                // If we see empty buckets between data points, we have zoomed in too far.
                // It's time to turn around and start zooming out.
                if (hasEmptyBuckets || bucketMins <= minBucketMins) {
                    nextDir = 1;
                    nextBucketMins = bucketMins * 2;
                } else {
                    nextBucketMins = bucketMins / 2;
                    if (nextBucketMins < minBucketMins) {
                        nextBucketMins = minBucketMins;
                    }
                }
            } else { // zooming out
                nextBucketMins = bucketMins * 2;
            }

            refineGap(nextBucketMins, nextDir, baseFilter, listener, minBucketMins, returnUnit, tsField, depth + 1, sliceMinMs, sliceMaxMs);

        }, listener::onFailure));
    }

    /**
     * Finds an approximate data interval using a single {@code auto_date_histogram} query.
     *
     * <p>This method serves as a fallback for the more precise {@link #refineGap}
     * algorithm. It lets OpenSearch automatically select a bucket interval that
     * keeps the total number of buckets under a fixed limit ({@code BUCKET_CAP}).
     * It then uses a pipeline aggregation (serial_diff) to find the shortest time
     * gap between those buckets.
     *
     * <p><b>Trade-offs:</b> While this approach is fast (requiring only a single
     * round-trip), it is less accurate than the adaptive search. The chosen interval
     * may be an upper bound of the true median gap, as multiple data points can
     * fall into the same aggregation bucket, masking their real interval. The
     * {@code refineGap} method is preferred for its higher precision.
     *
     * @param filter The base query filter.
     * @param listener The listener to be notified with the result.
     * @param unit The time unit for the resulting interval.
     * @param tsField The timestamp field.
     */
    public void runAutoDate(BoolQueryBuilder filter, ActionListener<IntervalTimeConfiguration> listener, ChronoUnit unit, String tsField) {

        AutoDateHistogramAggregationBuilder adh = new AutoDateHistogramAggregationBuilder("auto").field(tsField).setNumBuckets(BUCKET_CAP);

        adh.subAggregation(AggregationBuilders.min("first").field(tsField));
        adh.subAggregation(PipelineAggregatorBuilders.diff("gap", "first").lag(1).gapPolicy(BucketHelpers.GapPolicy.SKIP));

        SearchSourceBuilder src = new SearchSourceBuilder()
            .size(0)
            .query(filter)
            .aggregation(adh)
            .aggregation(PipelineAggregatorBuilders.minBucket("shortest", "auto>gap"));

        SearchRequest searchRequest = new SearchRequest(config.getIndices().toArray(new String[0])).source(src);
        client.search(searchRequest, ActionListener.wrap(r -> {
            NumericMetricsAggregation.SingleValue v = r.getAggregations().get("shortest");
            if (v == null || Double.isNaN(v.value())) {
                listener.onResponse(null);
                return;
            }
            long mins = toCeilMinutes(v.value());
            if (mins > 0) {
                listener.onResponse(new IntervalTimeConfiguration(mins, unit));
            } else {
                listener.onResponse(null);
            }
        }, listener::onFailure));
    }

    /**
     * Converts milliseconds to minutes, rounding up to the nearest minute.
     * @param milliseconds value in milliseconds.
     * @return value in minutes, rounded up.
     */
    private long toCeilMinutes(double milliseconds) {
        if (milliseconds <= 0) {
            return 0;
        }
        double minutes = milliseconds / TimeUnit.MINUTES.toMillis(1);
        return (long) Math.ceil(minutes);
    }

    /**
     * Computes the smallest power of&nbsp;two that is <em>greater than or equal to</em> {@code n}.
     *
     * There isn't a single public JDK routine that says "give me the ceiling-power-of-two for
     * this long"
     *
     * @param n any positive number. For {@code n <= 1} the method returns 1.
     *          If {@code n >= 2<sup>63</sup>} the result will overflow to a
     *          negative value—callers should guard against that.
     * @return the least power of two ≥ {@code n}
     */
    private static long nextPowerOfTwo(long n) {
        // If n is already a power of two, n–1 turns the single 1-bit into a
        // sequence of lower 1-bits (e.g. 1000₂ → 0111₂). For non-powers of two
        // it simply lowers the highest set bit so the OR-cascade works.
        n--;

        // Propagate the highest set bit to all lower positions.
        // After these operations all bits below the original MSB are 1.
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        n |= n >>> 32;  // spreads across the full 64-bit word

        // Adding 1 flips the string of 1-bits to 0-bits and carries into the
        // next position, yielding a pure power of two.
        return n + 1;
    }

    /*  ------------------------------------------------------------------
     *  "Nice" step ladder used when we probe successively larger
     *  date-histogram intervals.            (all values are **minutes**)
     *
     *  Why we call them *nice* intervals
     *  ---------------------------------
     *  • **Human-friendly** numbers** –  1 min, 5 min, 1 h, 1 day… are the
     *    same break-points you see in CloudWatch, Grafana, Kibana, etc.
     *  • **Calendar-aligned buckets** – Each value divides evenly into an
     *    hour (60) or a day (1 440).  Histogram buckets therefore start on
     *    :00, :05, :10 … not :07 or :13.
     *  • **Log-style growth** – The ladder roughly doubles every few
     *    steps, so we cover the whole spectrum from 1 min ➜ 1 day in
     *    ≤ 10 tries (our hard retry budget), instead of crawling upward in
     *    tiny increments.
     *
     *  Why we DON'T just multiply the current interval by a factor (e.g. 1.2)
     *  ---------------------------------------------------------------------
     *  Even if you round every multiplication result up to the next
     *  integer minute, the sequence quickly drifts into awkward,
     *  non-divisors of 60:
     *
     *      1  →  1.2 ⌈⌉= 2  →  2.4 ⌈⌉= 3  →  3.6 ⌈⌉= 4  →  4.8 ⌈⌉= 5 …
     *
     *  • **Messy bucket starts** – 3-minute or 7-minute buckets break the
     *    nice "every five minutes" cadence.
     *  • **Too many steps** – Reaching a day (1 440 min) would require
     *    ~28 multiplies by 1.2, far beyond our 10-attempt cap, hurting
     *    latency.
     *
     *  TL;DR – the ladder gives us predictable, pretty intervals and lets
     *  the algorithm converge in a handful of requests; geometric growth
     *  does not.
     *
     *  Keep this array **sorted** and **deduplicated** – the helper that
     *  picks "the next larger nice value" relies on that.
     *  ------------------------------------------------------------------ */
    private static final int[] INTERVAL_LADDER = {
        /* sub‑hour granularity  */ 1,
        2,
        5,
        10,
        15,
        30,
        /* exact hours          */ 60,
        120,
        240,
        /* half‑day / day       */ 480,
        720,
        1440,
        /* multi‑day            */ 2880,          // 2 days
        10080,         // 1 week
        43200          // 30 days
    };

    /* ------------------------------------------------------------------ */
    /*  Return the next "nice" interval after currentMin.                 */
    /*  – If currentMin is inside the ladder → next ladder element.       */
    /*  – If currentMin ≥ ladder max        → nextPowerOfTwo(currentMin). */
    /* ------------------------------------------------------------------ */
    private static int nextNiceInterval(int currentMin) {
        for (int step : INTERVAL_LADDER) {
            if (step > currentMin) {
                return step;
            }
        }
        // already at / beyond ladder top → jump by power-of-two
        return (int) nextPowerOfTwo(currentMin);
    }

}
