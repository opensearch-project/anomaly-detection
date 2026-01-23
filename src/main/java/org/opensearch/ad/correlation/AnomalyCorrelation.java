/*
 * SPDX-License-Identifier: Apache-2.0
 * 
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 * 
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 * 
 */
package org.opensearch.ad.correlation;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.search.aggregations.metrics.PercentilesAggregationBuilder;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.model.TimeConfiguration;

/**
 * Utilities for clustering anomalies into correlated incident windows.
 *
 * <p>
 * Tutorial:
 * <ol>
 * <li>Collect all anomaly points with grade &gt; 0 and build a
 * {@code List<Anomaly>}.</li>
 * <li>Call
 * {@code AnomalyCorrelation.clusterWithEventWindows(anomalies, detectors)} to
 * obtain a
 * {@code List<AnomalyCorrelation.Cluster>}. Each cluster contains the grouped
 * anomalies
 * and an event window whose start and end span the earliest start and latest
 * end in that
 * cluster. To omit single-element (uncorrelated) clusters, use
 * {@code AnomalyCorrelation.clusterWithEventWindows(anomalies, detectors, false)}
 * when
 * rendering
 * Dashboard views that should not show uncorrelated clusters.</li>
 * </ol>
 */
public final class AnomalyCorrelation {
    private AnomalyCorrelation() {}

    public enum TemporalMode {
        IOU,
        OVL,
        HYBRID
    }

    public static final class EventWindow {
        private final Instant start;
        private final Instant end;

        public EventWindow(Instant start, Instant end) {
            this.start = Objects.requireNonNull(start, "start");
            this.end = Objects.requireNonNull(end, "end");
            if (end.isBefore(start)) {
                throw new IllegalArgumentException("end must be on or after start");
            }
        }

        public Instant getStart() {
            return start;
        }

        public Instant getEnd() {
            return end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            EventWindow that = (EventWindow) o;
            return start.equals(that.start) && end.equals(that.end);
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end);
        }

        @Override
        public String toString() {
            return "EventWindow{" + "start=" + start + ", end=" + end + '}';
        }
    }

    public static final class Cluster {
        private final EventWindow eventWindow;
        private final List<Anomaly> anomalies;

        public Cluster(EventWindow eventWindow, List<Anomaly> anomalies) {
            this.eventWindow = Objects.requireNonNull(eventWindow, "eventWindow");
            this.anomalies = Objects.requireNonNull(anomalies, "anomalies");
            if (anomalies.isEmpty()) {
                throw new IllegalArgumentException("anomalies must not be empty");
            }
        }

        public EventWindow getEventWindow() {
            return eventWindow;
        }

        public List<Anomaly> getAnomalies() {
            return anomalies;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            Cluster cluster = (Cluster) o;
            return eventWindow.equals(cluster.eventWindow) && anomalies.equals(cluster.anomalies);
        }

        @Override
        public int hashCode() {
            return Objects.hash(eventWindow, anomalies);
        }

        @Override
        public String toString() {
            return "Cluster{" + "eventWindow=" + eventWindow + ", anomalies=" + anomalies + '}';
        }
    }

    private static final class DilatedAnomaly {
        final int idx; // original index in anomalies list
        final Anomaly anomaly;
        final Instant start; // dilated start
        final Instant end; // dilated end

        DilatedAnomaly(
            int idx,
            Anomaly anomaly,
            Duration delta,
            Map<String, Duration> detectorIntervals,
            Map<String, Boolean> detectorUsesCommunityAggregation
        ) {
            this.idx = idx;
            this.anomaly = anomaly;

            Duration deltaStart = backwardDilation(anomaly, delta, detectorIntervals, detectorUsesCommunityAggregation);
            this.start = anomaly.getDataStartTime().minus(deltaStart);
            this.end = anomaly.getDataEndTime().plus(delta);
        }
    }

    private static final Comparator<DilatedAnomaly> BY_START_THEN_END_THEN_IDX = Comparator
        .comparing((DilatedAnomaly d) -> d.start)
        .thenComparing(d -> d.end)
        .thenComparingInt(d -> d.idx);

    private static final Comparator<DilatedAnomaly> BY_END_THEN_START_THEN_IDX = Comparator
        .comparing((DilatedAnomaly d) -> d.end)
        .thenComparing(d -> d.start)
        .thenComparingInt(d -> d.idx);

    private static final Duration COARSE_INTERVAL_THRESHOLD = Duration.ofMinutes(30);

    private static Map<String, Duration> detectorIntervalsById(List<AnomalyDetector> detectors) {
        Objects.requireNonNull(detectors, "detectors");
        if (detectors.isEmpty()) {
            throw new IllegalArgumentException("detectors must not be empty");
        }
        Map<String, Duration> intervals = new HashMap<>(detectors.size());
        for (AnomalyDetector detector : detectors) {
            if (detector == null) {
                continue;
            }
            String detectorId = detector.getId();
            if (detectorId == null) {
                continue;
            }
            TimeConfiguration intervalConfig = detector.getInterval();
            if (intervalConfig instanceof IntervalTimeConfiguration) {
                intervals.put(detectorId, ((IntervalTimeConfiguration) intervalConfig).toDuration());
            }
        }
        if (intervals.isEmpty()) {
            throw new IllegalArgumentException("detectors must include interval configurations");
        }
        return intervals;
    }

    private static Duration detectorIntervalForAnomaly(Anomaly anomaly, Map<String, Duration> detectorIntervals) {
        if (detectorIntervals == null || detectorIntervals.isEmpty()) {
            return null;
        }
        return detectorIntervals.get(anomaly.getConfigId());
    }

    private static Map<String, Boolean> detectorUsesCommunityAggregationById(List<AnomalyDetector> detectors) {
        Objects.requireNonNull(detectors, "detectors");
        if (detectors.isEmpty()) {
            throw new IllegalArgumentException("detectors must not be empty");
        }
        Map<String, Boolean> usesCommunityAggregation = new HashMap<>(detectors.size());
        for (AnomalyDetector detector : detectors) {
            if (detector == null) {
                continue;
            }
            String detectorId = detector.getId();
            if (detectorId == null) {
                continue;
            }
            usesCommunityAggregation.put(detectorId, hasCommunityAggregation(detector.getFeatureAttributes()));
        }
        return usesCommunityAggregation;
    }

    /**
     * Returns true if any enabled feature uses an aggregation other than min,
     * max, or percentiles.
     *
     * We call these "community" aggregations because they depend on most/all
     * values in the bucket (e.g., sum/avg/count) rather than an extreme value.
     * Community aggregations are more sensitive to missing early data in a
     * bucket and can make anomaly start times lag the real onset, so we detect
     * them to decide whether to apply backward dilation.
     * 
     * @param features The features to check.
     * @return True if any enabled feature uses an aggregation other than min,
     *         max, or percentiles.
     */
    private static boolean hasCommunityAggregation(List<Feature> features) {
        if (features == null || features.isEmpty()) {
            return false;
        }
        for (Feature feature : features) {
            if (feature == null) {
                return true;
            }
            if (Boolean.FALSE.equals(feature.getEnabled())) {
                continue;
            }
            AggregationBuilder aggregation = feature.getAggregation();
            if (aggregation == null) {
                return true;
            }
            AggregationBuilder metricAggregation = unwrapSingleMetricAggregation(aggregation);
            if (metricAggregation == null || !isMaxMinPercentileAggregation(metricAggregation)) {
                return true;
            }
        }
        return false;
    }

    private static AggregationBuilder unwrapSingleMetricAggregation(AggregationBuilder aggregation) {
        AggregationBuilder current = aggregation;
        while (current != null) {
            Collection<AggregationBuilder> subAggregations = current.getSubAggregations();
            if (subAggregations == null || subAggregations.isEmpty()) {
                return current;
            }
            if (subAggregations.size() != 1) {
                return null;
            }
            current = subAggregations.iterator().next();
        }
        return null;
    }

    private static boolean isMaxMinPercentileAggregation(AggregationBuilder aggregation) {
        String type = aggregation.getType();
        return MaxAggregationBuilder.NAME.equals(type)
            || MinAggregationBuilder.NAME.equals(type)
            || PercentilesAggregationBuilder.NAME.equals(type);
    }

    /**
     * Returns the dilation to apply to the start of an anomaly.
     * 
     * We only extend the start for coarse detectors (e.g., >= 30m) with
     * community aggregations because their start times can lag the true onset.
     * Community aggregations need most/all data in a bucket (sum/avg/count), so
     * if the real incident happens near the end of a bucket, the score may not
     * cross the threshold until the next bucket. Bucket boundaries are an
     * artifact of the interval schedule rather than the incident timing, so the
     * reported start can be late by up to one bucket. Extending the end by a
     * large amount can create "bridge" overlaps to unrelated later incidents,
     * so we keep the end dilation at deltaTol to limit false merges.
     * 
     * Example lag with coarse buckets:
     * 1) Time quantization: we only "see" time at bucket boundaries.
     * - 60-minute buckets: 10:00-11:00, 11:00-12:00, 12:00-13:00.
     * - The detector cannot pinpoint "incident began at 10:17"; it only
     * knows "something was off during 11:00-12:00".
     * 2) Detection latency: the first bucket that crosses the threshold can be
     * later than the true onset.
     * - True onset: 10:20.
     * - 10:00-11:00 contains 40m normal + 40m incident (may look normal).
     * - 11:00-12:00 contains 60m incident (stronger signal).
     * - Result: anomaly starts at 11:00 even though the incident began at
     * 10:20 (lags by up to one bucket).
     * 
     * @param a        the anomaly
     * @param deltaTol the dilation to apply to the end of an anomaly
     * @return the dilation to apply to the start of an anomaly
     */
    private static Duration backwardDilation(
        Anomaly a,
        Duration deltaTol,
        Map<String, Duration> detectorIntervals,
        Map<String, Boolean> detectorUsesCommunityAggregation
    ) {
        if (detectorUsesCommunityAggregation == null
            || detectorUsesCommunityAggregation.isEmpty()
            || !Boolean.TRUE.equals(detectorUsesCommunityAggregation.get(a.getConfigId()))) {
            return deltaTol;
        }
        Duration interval = detectorIntervalForAnomaly(a, detectorIntervals);
        if (interval == null || interval.isZero() || interval.isNegative()) {
            return deltaTol;
        }
        return interval.compareTo(COARSE_INTERVAL_THRESHOLD) >= 0 ? interval : deltaTol;
    }

    private static final Duration DELTA_TOL = Duration.ofMinutes(5);
    private static final double ALPHA = 0.30;
    private static final Duration KAPPA = Duration.ofMinutes(30);
    private static final Duration MIN_OVERLAP = Duration.ofMinutes(3);

    private static final AnomalyCorrelation.TemporalMode MODE = AnomalyCorrelation.TemporalMode.HYBRID;
    private static final double LAM = 0.6;
    private static final double TAU_CONTAIN = 0.8;
    private static final double RHO_MAX = 0.25;

    /**
     * CONTAINMENT_RELAX controls how much we "relax" the duration-penalty when two
     * anomalies are in strong containment (i.e., overlap coefficient >= TAU_CONTAIN
     * and the duration ratio passes RHO_MAX).
     *
     * Intuition:
     * - Containment cases often happen when one detector emits a long anomaly
     * (coarse window) while another emits shorter anomalies (fine window) that
     * sit inside it. In these cases, penalizing duration differences too
     * aggressively can prevent otherwise clearly-related events from being linked.
     *
     * Behavior:
     * - If strongContainment == false:
     * similarity = temporalOverlap * durationPenalty
     * (duration penalty applies normally)
     *
     * - If strongContainment == true:
     * - CONTAINMENT_RELAX == 0.0:
     * duration penalty is fully disabled under containment (pen = 1.0).
     * This makes containment edges depend only on temporal overlap (IoU/OVL blend).
     * - CONTAINMENT_RELAX > 0.0:
     * duration penalty is partially relaxed by exponentiating it:
     * pen = pow(durationPenalty, CONTAINMENT_RELAX)
     * Since durationPenalty is in (0, 1], raising it to a smaller exponent moves it
     * closer to 1.
     * Example: basePen=0.2
     * relax=1.0 -> 0.2 (no relaxation)
     * relax=0.5 -> sqrt(0.2) ≈ 0.447 (less penalty)
     * relax=0.25 -> 0.2^0.25 ≈ 0.758 (much less penalty)
     *
     * Practical guidance:
     * - 0.0 : strongest containment linking (duration mismatch ignored under
     * containment)
     * - 0.2~0.6 : keep some duration penalty but soften it for containment
     * - 1.0 : no special treatment; containment still affects temporal mode but
     * penalty is unchanged
     */
    private static final double CONTAINMENT_RELAX = 0.45;

    private static final Comparator<Anomaly> CLUSTER_MEMBER_ORDER = Comparator.comparing(Anomaly::getModelId);

    private static final class Interval {
        final Instant start;
        final Instant end;

        Interval(Instant start, Instant end) {
            this.start = Objects.requireNonNull(start, "start");
            this.end = Objects.requireNonNull(end, "end");
        }
    }

    /**
     * Dilate an interval [s, e] by +/- delta.
     * 
     * Two detectors may flag the same incident but disagree by a couple minutes on
     * start/end (e.g., due to different bucket alignment).
     * This function dilates the interval by +/- delta for fault tolerance.
     * 
     * @param s          The start time of the interval.
     * @param e          The end time of the interval.
     * @param deltaStart The time window for dilation at the start.
     * @param deltaEnd   The time window for dilation at the end.
     * @return The dilated interval.
     */
    private static Interval dilate(Instant s, Instant e, Duration deltaStart, Duration deltaEnd) {
        return new Interval(s.minus(deltaStart), e.plus(deltaEnd));
    }

    /**
     * Overlap length between two intervals.
     */
    private static Duration overlapLength(Interval a, Interval b) {
        Instant s = a.start.isAfter(b.start) ? a.start : b.start;
        Instant e = a.end.isBefore(b.end) ? a.end : b.end;
        if (!e.isAfter(s)) {
            // if the end time is before the start time, return 0
            return Duration.ZERO;
        }
        return Duration.between(s, e);
    }

    /** Temporal IoU (Jaccard over time) on dilated intervals. */
    private static double temporalIou(
        Anomaly a,
        Anomaly b,
        Duration delta,
        Map<String, Duration> detectorIntervals,
        Map<String, Boolean> detectorUsesCommunityAggregation
    ) {
        Duration aStart = backwardDilation(a, delta, detectorIntervals, detectorUsesCommunityAggregation);
        Duration bStart = backwardDilation(b, delta, detectorIntervals, detectorUsesCommunityAggregation);

        Interval ad = dilate(a.getDataStartTime(), a.getDataEndTime(), aStart, delta);
        Interval bd = dilate(b.getDataStartTime(), b.getDataEndTime(), bStart, delta);

        long overlapNanos = overlapLength(ad, bd).toNanos();
        if (overlapNanos <= 0L)
            return 0.0;

        long lenANanos = Duration.between(ad.start, ad.end).toNanos();
        long lenBNanos = Duration.between(bd.start, bd.end).toNanos();
        long unionNanos = lenANanos + lenBNanos - overlapNanos;
        if (unionNanos <= 0L)
            return 0.0;

        return (double) overlapNanos / (double) unionNanos;
    }

    /**
     * Overlap coefficient https://en.wikipedia.org/wiki/Overlap_coefficient:
     * overlap / min(lenA, lenB).
     * 
     * Motivation:
     * detector A has a single, long anomaly interval, while detector B has several
     * short anomaly intervals contained
     * within detector A's interval. These should probably all be grouped together,
     * but the pairwise IoU / duration
     * similarity scores will be very low. The overlap coefficient is a more robust
     * measure of similarity in this case.
     * 
     * @param a                                The first anomaly.
     * @param b                                The second anomaly.
     * @param delta                            The time window for temporal overlap.
     * @param detectorIntervals                The detector intervals.
     * @param detectorUsesCommunityAggregation The detector uses community
     *                                         aggregation.
     * @return The overlap coefficient.
     */
    private static double overlapCoefficient(
        Anomaly a,
        Anomaly b,
        Duration delta,
        Map<String, Duration> detectorIntervals,
        Map<String, Boolean> detectorUsesCommunityAggregation
    ) {
        Duration aStart = backwardDilation(a, delta, detectorIntervals, detectorUsesCommunityAggregation);
        Duration bStart = backwardDilation(b, delta, detectorIntervals, detectorUsesCommunityAggregation);
        Interval ad = dilate(a.getDataStartTime(), a.getDataEndTime(), aStart, delta);
        Interval bd = dilate(b.getDataStartTime(), b.getDataEndTime(), bStart, delta);

        long overlapNanos = overlapLength(ad, bd).toNanos();
        if (overlapNanos <= 0L)
            return 0.0;

        long lenANanos = Duration.between(ad.start, ad.end).toNanos();
        long lenBNanos = Duration.between(bd.start, bd.end).toNanos();
        long denom = Math.min(lenANanos, lenBNanos);
        if (denom <= 0L)
            return 0.0;

        return (double) overlapNanos / (double) denom;
    }

    /**
     * Duration penalty: discourage linking anomalies with very different lengths.
     * Even if these two anomalies overlap in time, treat them as less related when
     * one lasts much longer than the other. This penalty is basically acting like
     * a Laplacian kernel on durations: similarity decays exponentially with
     * absolute distance.
     *
     *
     * <p>
     * Formula: exp(-|durA - durB| / kappa).
     *
     * <p>
     * Because the penalty is exp(-diff / kappa):
     * <ul>
     * <li>diff = 0 -> exp(0) = 1.0 (no penalty)</li>
     * <li>diff = kappa -> exp(-1) ~= 0.3679</li>
     * <li>diff = 2*kappa -> exp(-2) ~= 0.1353</li>
     * <li>diff = 3*kappa -> exp(-3) ~= 0.0498</li>
     * </ul>
     *
     * <p>
     * With kappa = 30 minutes:
     * <ul>
     * <li>10 min mismatch -> exp(-10/30) ~= 0.7165</li>
     * <li>15 min mismatch -> exp(-15/30) ~= 0.6065</li>
     * <li>30 min mismatch -> ~= 0.3679</li>
     * <li>60 min mismatch -> exp(-60/30) ~= 0.1353</li>
     * </ul>
     *
     * <p>
     * exp(-|diff|/kappa) is proportional to a Laplace likelihood in diff with
     * scale kappa. This treats duration differences as Laplace noise with typical
     * scale about kappa. A 30-minute mismatch cuts the duration trust to about
     * 37%. With kappa = 30 minutes, a 30-minute mismatch is plausible; much larger
     * mismatch becomes exponentially unlikely.
     *
     * @param a     The first anomaly.
     * @param b     The second anomaly.
     * @param kappa the time-scale for how tolerant you want to be to duration
     *              mismatch between two anomaly intervals. In other words,
     *              kappa controls how quickly should we stop trusting overlap when
     *              the anomaly durations disagree.
     * @return The duration penalty.
     */
    public static double durationPenalty(Anomaly a, Anomaly b, Duration kappa) {
        if (kappa == null || kappa.isZero() || kappa.isNegative())
            return 1.0;

        long durANanos = a.getDuration().toNanos();
        long durBNanos = b.getDuration().toNanos();
        long diffNanos = Math.abs(durANanos - durBNanos);
        long kappaNanos = kappa.toNanos();
        if (kappaNanos <= 0L)
            return 1.0;

        return Math.exp(-((double) diffNanos / (double) kappaNanos));
    }

    /**
     * similarity is a product of temporal overlap and duration penalty.
     * Temporal overlap rewards overlapping anomalies, while duration penalty
     * penalizes anomalies with different durations.
     *
     * @param a                                The first anomaly.
     * @param b                                The second anomaly.
     * @param delta                            The time window for temporal overlap.
     * @param kappa                            The duration penalty factor.
     * @param temporalMode                     The temporal mode.
     * @param lam                              The hybrid containment factor.
     * @param tauContain                       The containment threshold.
     * @param rhoMax                           The maximum ratio of lengths.
     * @param containmentRelax                 The containment relaxation factor.
     * @param detectorIntervals                The detector intervals.
     * @param detectorUsesCommunityAggregation The detector uses community
     *                                         aggregation.
     * @return The similarity score.
     */
    private static double similarity(
        Anomaly a,
        Anomaly b,
        Duration delta,
        Duration kappa,
        TemporalMode temporalMode,
        double lam,
        double tauContain,
        double rhoMax,
        double containmentRelax,
        Map<String, Duration> detectorIntervals,
        Map<String, Boolean> detectorUsesCommunityAggregation
    ) {

        double iou = temporalIou(a, b, delta, detectorIntervals, detectorUsesCommunityAggregation);
        double ovl = overlapCoefficient(a, b, delta, detectorIntervals, detectorUsesCommunityAggregation);

        Duration aStart = backwardDilation(a, delta, detectorIntervals, detectorUsesCommunityAggregation);
        Duration bStart = backwardDilation(b, delta, detectorIntervals, detectorUsesCommunityAggregation);
        Interval ad = dilate(a.getDataStartTime(), a.getDataEndTime(), aStart, delta);
        Interval bd = dilate(b.getDataStartTime(), b.getDataEndTime(), bStart, delta);

        double lenA = (double) Duration.between(ad.start, ad.end).toNanos();
        double lenB = (double) Duration.between(bd.start, bd.end).toNanos();

        // with rhoMax=0.25, ratioOk means durations differ by at least 4×.
        boolean durationVeryDifferent = (lenA > 0.0 && lenB > 0.0) && (Math.min(lenA, lenB) / Math.max(lenA, lenB) <= rhoMax);
        // One interval (after dilation) covers most/all of the other
        boolean covered = (ovl >= tauContain);
        // A coarse detector emits a long window, and a fine detector emits shorter
        // windows inside it
        boolean strongContainment = covered && durationVeryDifferent;

        double t;
        switch (temporalMode) {
            case IOU:
                t = iou;
                break;
            case OVL:
                t = ovl;
                break;
            case HYBRID:
                t = strongContainment ? ((1.0 - lam) * iou + lam * ovl) : iou;
                break;
            default:
                throw new IllegalArgumentException("temporalMode must be IOU|OVL|HYBRID");
        }

        if (t <= 0.0)
            return 0.0;

        Duration kappaEff = kappa;

        if (strongContainment) {
            // duration mismatch is expected, using the same fixed 30m KAPPA (default kappa
            // value) can be overly punitive. Make duration tolerance scale with the
            // coarsest detector interval. That is, if it’s containment + large mismatch,
            // treat duration mismatch as less suspicious.
            // But we don't want to weaken the duration penalty in general cases where we’re
            // worried about bridge effects.
            Duration ia = detectorIntervalForAnomaly(a, detectorIntervals);
            Duration ib = detectorIntervalForAnomaly(b, detectorIntervals);

            if (ia != null && ib != null && !ia.isZero() && !ia.isNegative() && !ib.isZero() && !ib.isNegative()) {
                Duration maxInt = ia.compareTo(ib) >= 0 ? ia : ib;
                if (maxInt.compareTo(kappaEff) > 0) {
                    kappaEff = maxInt;
                }
            }
        }

        double basePen = durationPenalty(a, b, kappaEff);

        double pen;
        if (strongContainment) {
            pen = (containmentRelax == 0.0) ? 1.0 : Math.pow(basePen, containmentRelax);
        } else {
            pen = basePen;
        }

        return t * pen;
    }

    /**
     * Build an undirected threshold graph (adjacency list) based on similarity
     * scores.
     * Connect i and j if:
     * - similarity(i,j) >= min_similarity, AND
     * - dilated-overlap >= min_overlap,
     *
     * @param anomalies        The list of anomalies.
     * @param detectors        The detector configs used to resolve detector
     *                         intervals.
     * @param delta            The time window for temporal overlap.
     * @param kappa            The duration penalty factor.
     * @param minSimilarity    The minimum similarity score.
     * @param minOverlap       The minimum overlap duration.
     * @param temporalMode     The temporal mode.
     * @param lam              The hybrid containment factor.
     * @param tauContain       The containment threshold.
     * @param rhoMax           The maximum ratio of lengths.
     * @param containmentRelax The containment relaxation factor.
     * @return The adjacency list.
     */
    public static List<List<Integer>> buildThresholdGraph(
        List<Anomaly> anomalies,
        List<AnomalyDetector> detectors,
        Duration delta,
        Duration kappa,
        double minSimilarity,
        Duration minOverlap,
        TemporalMode temporalMode,
        double lam,
        double tauContain,
        double rhoMax,
        double containmentRelax
    ) {
        Map<String, Duration> detectorIntervals = detectorIntervalsById(detectors);
        Map<String, Boolean> detectorUsesCommunityAggregation = detectorUsesCommunityAggregationById(detectors);
        return buildThresholdGraph(
            anomalies,
            delta,
            kappa,
            minSimilarity,
            minOverlap,
            temporalMode,
            lam,
            tauContain,
            rhoMax,
            containmentRelax,
            detectorIntervals,
            detectorUsesCommunityAggregation
        );
    }

    private static List<List<Integer>> buildThresholdGraph(
        List<Anomaly> anomalies,
        Duration delta,
        Duration kappa,
        double minSimilarity,
        Duration minOverlap,
        TemporalMode temporalMode,
        double lam,
        double tauContain,
        double rhoMax,
        double containmentRelax,
        Map<String, Duration> detectorIntervals,
        Map<String, Boolean> detectorUsesCommunityAggregation
    ) {
        Objects.requireNonNull(anomalies, "anomalies");
        Objects.requireNonNull(delta, "delta");
        Objects.requireNonNull(minOverlap, "minOverlap");

        int n = anomalies.size();

        List<List<Integer>> adj = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            adj.add(new ArrayList<>());
        }
        if (n <= 1) {
            return adj;
        }

        // 1) Precompute dilated intervals once
        List<DilatedAnomaly> nodes = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Anomaly a = anomalies.get(i);
            nodes.add(new DilatedAnomaly(i, a, delta, detectorIntervals, detectorUsesCommunityAggregation));
        }

        // 2) Sweep in (dilated) start-time order
        nodes.sort(BY_START_THEN_END_THEN_IDX);

        // Active set ordered by (dilated) end time, so we can expire quickly
        TreeSet<DilatedAnomaly> active = new TreeSet<>(BY_END_THEN_START_THEN_IDX);

        for (DilatedAnomaly cur : nodes) {
            // Any interval whose end is before this cannot overlap cur by minOverlap
            Instant requiredEnd = cur.start.plus(minOverlap);

            // Expire intervals that can't overlap cur (or any future interval) by
            // minOverlap
            while (!active.isEmpty() && active.first().end.isBefore(requiredEnd)) {
                active.pollFirst();
            }

            // If cur itself is too short (after dilation) to ever have minOverlap with
            // anyone, skip comparisons.
            // (It also can't help future ones, because future starts are >= cur.start,
            // making requiredEnd even later.)
            if (cur.end.isBefore(requiredEnd)) {
                continue;
            }

            // Now, for every prev in active:
            // - prev.start <= cur.start (since it was added earlier in sweep)
            // - prev.end >= requiredEnd (due to expiration)
            // - cur.end >= requiredEnd (checked above)
            // => dilated overlap length >= minOverlap automatically
            for (DilatedAnomaly prev : active) {
                double s = similarity(
                    prev.anomaly,
                    cur.anomaly,
                    delta,
                    kappa,
                    temporalMode,
                    lam,
                    tauContain,
                    rhoMax,
                    containmentRelax,
                    detectorIntervals,
                    detectorUsesCommunityAggregation
                );

                if (s >= minSimilarity) {
                    int i = prev.idx;
                    int j = cur.idx;
                    adj.get(i).add(j);
                    adj.get(j).add(i);
                }
            }

            active.add(cur);
        }

        // Optional: restore the "sorted neighbor lists" property of the original nested
        // loop version
        for (List<Integer> neigh : adj) {
            Collections.sort(neigh);
        }

        return adj;
    }

    /**
     * Connected components of an adjacency list. Deterministic order:
     * iterate vertices 0..n-1 and sort each component’s indices.
     * Note: vertices with no neighbors (i.e., adj.get(x) is empty) still form
     * singleton components and are kept.
     * 
     * @param adj The adjacency list.
     * @return The list of connected components.
     */
    public static List<List<Integer>> connectedComponents(List<List<Integer>> adj) {
        int n = adj.size();
        boolean[] seen = new boolean[n];
        List<List<Integer>> comps = new ArrayList<>();

        for (int v = 0; v < n; v++) {
            if (seen[v])
                continue;

            List<Integer> comp = new ArrayList<>();
            Deque<Integer> stack = new ArrayDeque<>();
            stack.push(v);
            seen[v] = true;

            while (!stack.isEmpty()) {
                int x = stack.pop();
                comp.add(x);
                for (int y : adj.get(x)) {
                    if (!seen[y]) {
                        seen[y] = true;
                        stack.push(y);
                    }
                }
            }

            Collections.sort(comp);
            comps.add(comp);
        }
        return comps;
    }

    /**
     * run threshold-graph clustering and return clusters as lists of anomalies.
     * 
     * @param anomalies         The list of anomalies.
     * @param detectors         The detector configs used to resolve detector
     *                          intervals.
     * @param delta             The time window for temporal overlap.
     * @param kappa             The duration penalty factor.
     * @param minSimilarity     The minimum similarity score.
     * @param minOverlap        The minimum overlap duration.
     * @param temporalMode      The temporal mode.
     * @param lam               The hybrid containment factor.
     * @param tauContain        The containment threshold.
     * @param rhoMax            The maximum ratio of lengths.
     * @param containmentRelax  The containment relaxation factor.
     * @param includeSingletons Whether to include single-element clusters. Set to
     *                          false to avoid showing
     *                          uncorrelated clusters in the Dashboard.
     * @return The list of clusters.
     */
    public static List<List<Anomaly>> cluster(
        List<Anomaly> anomalies,
        List<AnomalyDetector> detectors,
        Duration delta,
        Duration kappa,
        double minSimilarity,
        Duration minOverlap,
        TemporalMode temporalMode,
        double lam,
        double tauContain,
        double rhoMax,
        double containmentRelax,
        boolean includeSingletons
    ) {

        List<Anomaly> dedupedAnomalies = dedupe(anomalies);
        Map<String, Duration> detectorIntervals = detectorIntervalsById(detectors);
        Map<String, Boolean> detectorUsesCommunityAggregation = detectorUsesCommunityAggregationById(detectors);
        List<List<Integer>> adj = buildThresholdGraph(
            dedupedAnomalies,
            delta,
            kappa,
            minSimilarity,
            minOverlap,
            temporalMode,
            lam,
            tauContain,
            rhoMax,
            containmentRelax,
            detectorIntervals,
            detectorUsesCommunityAggregation
        );

        List<List<Integer>> comps = connectedComponents(adj);
        List<List<Anomaly>> out = new ArrayList<>(comps.size());

        for (List<Integer> comp : comps) {
            if (!includeSingletons && comp.size() == 1) {
                continue;
            }
            List<Anomaly> members = new ArrayList<>(comp.size());
            for (int idx : comp) {
                members.add(dedupedAnomalies.get(idx));
            }
            // output in a deterministic order
            members.sort(CLUSTER_MEMBER_ORDER);
            out.add(members);
        }
        return out;
    }

    public static List<List<Anomaly>> cluster(
        List<Anomaly> anomalies,
        List<AnomalyDetector> detectors,
        Duration delta,
        Duration kappa,
        double minSimilarity,
        Duration minOverlap,
        TemporalMode temporalMode,
        double lam,
        double tauContain,
        double rhoMax,
        double containmentRelax
    ) {
        return cluster(
            anomalies,
            detectors,
            delta,
            kappa,
            minSimilarity,
            minOverlap,
            temporalMode,
            lam,
            tauContain,
            rhoMax,
            containmentRelax,
            true
        );
    }

    private static List<Anomaly> dedupe(List<Anomaly> anomalies) {
        Objects.requireNonNull(anomalies, "anomalies");
        // Use LinkedHashSet to preserve insertion order and deduplicate based on
        // Anomaly.equals()
        return new ArrayList<>(new java.util.LinkedHashSet<>(anomalies));
    }

    private static EventWindow eventWindowForCluster(List<Anomaly> cluster) {
        Objects.requireNonNull(cluster, "cluster");
        if (cluster.isEmpty()) {
            throw new IllegalArgumentException("cluster must not be empty");
        }

        Instant minStart = null;
        Instant maxEnd = null;
        for (Anomaly anomaly : cluster) {
            Objects.requireNonNull(anomaly, "anomaly");
            Instant start = anomaly.getDataStartTime();
            Instant end = anomaly.getDataEndTime();
            if (minStart == null || start.isBefore(minStart)) {
                minStart = start;
            }
            if (maxEnd == null || end.isAfter(maxEnd)) {
                maxEnd = end;
            }
        }
        return new EventWindow(minStart, maxEnd);
    }

    public static List<EventWindow> clusterEventWindows(List<List<Anomaly>> clusters) {
        Objects.requireNonNull(clusters, "clusters");
        List<EventWindow> windows = new ArrayList<>(clusters.size());
        for (List<Anomaly> cluster : clusters) {
            windows.add(eventWindowForCluster(cluster));
        }
        return windows;
    }

    /**
     * Run clustering with defaults and attach event windows to each cluster.
     * Singleton clusters are included by default.
     * 
     * @param anomalies The list of anomalies.
     * @param detectors The detector configs used to resolve detector intervals.
     * @return The list of clusters with event windows.
     */
    public static List<Cluster> clusterWithEventWindows(List<Anomaly> anomalies, List<AnomalyDetector> detectors) {
        return clusterWithEventWindows(anomalies, detectors, true);
    }

    /**
     * Run clustering with defaults and attach event windows to each cluster.
     * 
     * @param anomalies         The list of anomalies.
     * @param detectors         The detector configs used to resolve detector
     *                          intervals.
     * @param includeSingletons Whether to include single-element clusters. Set to
     *                          false to avoid showing
     *                          uncorrelated clusters in the Dashboard.
     * @return The list of clusters with event windows.
     */
    public static List<Cluster> clusterWithEventWindows(
        List<Anomaly> anomalies,
        List<AnomalyDetector> detectors,
        boolean includeSingletons
    ) {
        List<List<Anomaly>> clusters = cluster(anomalies, detectors, includeSingletons);
        List<Cluster> out = new ArrayList<>(clusters.size());
        for (List<Anomaly> cluster : clusters) {
            out.add(new Cluster(eventWindowForCluster(cluster), cluster));
        }
        return out;
    }

    public static List<List<Anomaly>> cluster(List<Anomaly> anomalies, List<AnomalyDetector> detectors) {
        return cluster(anomalies, detectors, true);
    }

    public static List<List<Anomaly>> cluster(List<Anomaly> anomalies, List<AnomalyDetector> detectors, boolean includeSingletons) {
        return cluster(
            anomalies,
            detectors,
            DELTA_TOL,
            KAPPA,
            ALPHA,
            MIN_OVERLAP,
            MODE,
            LAM,
            TAU_CONTAIN,
            RHO_MAX,
            CONTAINMENT_RELAX,
            includeSingletons
        );
    }
}
