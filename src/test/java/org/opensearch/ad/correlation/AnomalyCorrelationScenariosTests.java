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
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.model.Feature;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;

public class AnomalyCorrelationScenariosTests extends OpenSearchTestCase {

    public void test_cluster_with_event_windows_matches_expected() {
        for (Scenario scenario : scenarioProvider()) {
            List<AnomalyCorrelation.Cluster> clusters = AnomalyCorrelation.clusterWithEventWindows(scenario.anomalies, scenario.detectors);

            List<List<String>> actualClusters = toIdClustersFromClusters(clusters);
            assertEquals("Cluster membership mismatch for scenario " + scenario.name, scenario.expectedClusterIds, actualClusters);

            List<AnomalyCorrelation.EventWindow> actualWindows = toEventWindowsFromClusters(clusters);
            List<AnomalyCorrelation.EventWindow> expectedWindows = expectedEventWindows(scenario.anomalies, scenario.expectedClusterIds);
            assertEquals("Cluster event window mismatch for scenario " + scenario.name, expectedWindows, actualWindows);
        }
    }

    public void test_cluster_with_event_windows_omits_singletons_when_disabled() {
        for (Scenario scenario : scenarioProvider()) {
            List<AnomalyCorrelation.Cluster> clusters = AnomalyCorrelation
                .clusterWithEventWindows(scenario.anomalies, scenario.detectors, false);

            List<List<String>> actualClusters = toIdClustersFromClusters(clusters);
            List<List<String>> expectedClusters = filterSingletonClusters(scenario.expectedClusterIds);
            assertEquals("Cluster membership mismatch for scenario " + scenario.name, expectedClusters, actualClusters);

            List<AnomalyCorrelation.EventWindow> actualWindows = toEventWindowsFromClusters(clusters);
            List<AnomalyCorrelation.EventWindow> expectedWindows = expectedEventWindows(scenario.anomalies, expectedClusters);
            assertEquals("Cluster event window mismatch for scenario " + scenario.name, expectedWindows, actualWindows);
        }
    }

    public void test_cluster_value_semantics() {
        Instant start = Instant.parse("2025-03-01T09:00:00Z");
        Instant end = start.plus(Duration.ofMinutes(5));
        Anomaly anomaly = anomaly("A-1", "detector_A", start, end);
        List<Anomaly> anomalies = Arrays.asList(anomaly);
        AnomalyCorrelation.EventWindow window = new AnomalyCorrelation.EventWindow(start, end);

        AnomalyCorrelation.Cluster cluster = new AnomalyCorrelation.Cluster(window, anomalies);
        AnomalyCorrelation.Cluster same = new AnomalyCorrelation.Cluster(
            new AnomalyCorrelation.EventWindow(start, end),
            new ArrayList<>(anomalies)
        );
        AnomalyCorrelation.Cluster different = new AnomalyCorrelation.Cluster(
            new AnomalyCorrelation.EventWindow(start, end.plusSeconds(1)),
            anomalies
        );

        assertEquals(window, cluster.getEventWindow());
        assertEquals(anomalies, cluster.getAnomalies());

        assertEquals(cluster, cluster);
        assertNotEquals(cluster, null);
        assertNotEquals(cluster, "not-a-cluster");
        assertEquals(cluster, same);
        assertEquals(cluster.hashCode(), same.hashCode());
        assertNotEquals(cluster, different);

        assertEquals(
            "Cluster{eventWindow=EventWindow{start=2025-03-01T09:00:00Z, end=2025-03-01T09:05:00Z}, "
                + "anomalies=[Anomaly{id='A-1', detectorName='detector_A', dataStartTime=2025-03-01T09:00:00Z, "
                + "dataEndTime=2025-03-01T09:05:00Z}]}",
            cluster.toString()
        );
    }

    public void test_cluster_rejects_invalid_inputs() {
        Instant start = Instant.parse("2025-03-01T09:00:00Z");
        Instant end = start.plus(Duration.ofMinutes(1));
        Anomaly anomaly = anomaly("A-1", "detector_A", start, end);
        AnomalyCorrelation.EventWindow window = new AnomalyCorrelation.EventWindow(start, end);

        NullPointerException nullWindow = expectThrows(
            NullPointerException.class,
            () -> new AnomalyCorrelation.Cluster(null, Arrays.asList(anomaly))
        );
        assertEquals("eventWindow", nullWindow.getMessage());

        NullPointerException nullAnomalies = expectThrows(NullPointerException.class, () -> new AnomalyCorrelation.Cluster(window, null));
        assertEquals("anomalies", nullAnomalies.getMessage());

        IllegalArgumentException emptyAnomalies = expectThrows(
            IllegalArgumentException.class,
            () -> new AnomalyCorrelation.Cluster(window, new ArrayList<>())
        );
        assertEquals("anomalies must not be empty", emptyAnomalies.getMessage());
    }

    public void test_deduplication_logic() {
        Instant t0 = Instant.parse("2025-01-01T00:00:00Z");

        // 1. Exact duplicates -> should be deduped
        Anomaly a1 = anomaly("modelA", "detectorA", t0, t0.plus(Duration.ofMinutes(10)));
        Anomaly a2 = anomaly("modelA", "detectorA", t0, t0.plus(Duration.ofMinutes(10))); // Identical to a1

        // 2. Same model, different time -> should NOT be deduped
        Anomaly b1 = anomaly("modelA", "detectorA", t0.plus(Duration.ofMinutes(20)), t0.plus(Duration.ofMinutes(30)));

        // 3. Different model, same time -> should NOT be deduped
        Anomaly c1 = anomaly("modelB", "detectorB", t0, t0.plus(Duration.ofMinutes(10)));

        List<Anomaly> input = Arrays.asList(a1, a2, b1, c1);

        // Used cluster method directly which calls dedupe internally
        List<List<Anomaly>> clusters = AnomalyCorrelation.cluster(input, detectorsForAnomalies(input, Collections.emptyMap()));

        // Flatten clusters to check anomalies preserved
        List<Anomaly> allMembers = new ArrayList<>();
        for (List<Anomaly> cluster : clusters) {
            allMembers.addAll(cluster);
        }

        // We expect a1 (or a2), b1, and c1 to be present. Total 3 unique anomalies.
        assertEquals(3, allMembers.size());
        assertTrue(allMembers.contains(a1));
        assertTrue(allMembers.contains(b1));
        assertTrue(allMembers.contains(c1));

        // Verify duplicates are gone (Set size check implicitly covers this, but being
        // explicit)
        int a1Count = 0;
        for (Anomaly a : allMembers) {
            if (a.equals(a1))
                a1Count++;
        }
        assertEquals(1, a1Count);
    }

    public void test_no_backward_dilation_for_min_max_percentiles() {
        List<Anomaly> anomalies = scenarioCoarseDetectorBridgesGap();
        Map<String, Duration> coarseOverrides = new HashMap<>();
        coarseOverrides.put("NijxmpsBGB6Rcmbr_K1n", Duration.ofMinutes(5));
        coarseOverrides.put("aT1I7ZoBpHa2ZXX_08qa", Duration.ofMinutes(20));
        coarseOverrides.put("YD1I7ZoBpHa2ZXX_c8on", Duration.ofMinutes(60));

        List<Supplier<AggregationBuilder>> aggregationSuppliers = Arrays
            .asList(
                () -> AggregationBuilders.min("min").field("value"),
                () -> AggregationBuilders.max("max").field("value"),
                () -> AggregationBuilders.percentiles("percentiles").field("value")
            );
        List<List<String>> expectedClusters = expectedCoarseDetectorNoBridge();

        for (Supplier<AggregationBuilder> aggregationSupplier : aggregationSuppliers) {
            String aggregationType = aggregationSupplier.get().getType();
            List<AnomalyDetector> detectors = detectorsForAnomalies(anomalies, coarseOverrides, aggregationSupplier);
            List<List<Anomaly>> clusters = AnomalyCorrelation.cluster(anomalies, detectors);
            List<List<String>> actualClusters = toIdClusters(clusters);
            assertEquals("Cluster membership mismatch for aggregation " + aggregationType, expectedClusters, actualClusters);
        }
    }

    // -----------------------
    // Provider (all scenarios except jackie)
    // -----------------------
    static List<Scenario> scenarioProvider() {
        List<Anomaly> baseSample = sampleData();
        List<Anomaly> globalLag = scenarioGlobalLag();
        List<Anomaly> nestedShortInsideLong = scenarioNestedShortInsideLong();
        List<Anomaly> braidedIncidentsClose = scenarioBraidedIncidentsClose();
        List<Anomaly> nearMissRequiresDelta = scenarioNearMissRequiresDelta();
        List<Anomaly> oneLongManyShorts = scenarioOneLongManyShorts();
        List<Anomaly> fragmentationManyToOne = scenarioFragmentationManyToOne();
        List<Anomaly> noiseOutliers = scenarioNoiseOutliers();
        List<Anomaly> midnightBoundary = scenarioMidnightBoundary();
        List<Anomaly> twoCloseIncidentsClearSplit = scenarioTwoCloseIncidentsClearSplit();
        List<Anomaly> randomSyntheticFixture = scenarioRandomSyntheticFixture();
        List<Anomaly> coarseDetectorBridgesGap = scenarioCoarseDetectorBridgesGap();

        Map<String, Duration> coarseOverrides = new HashMap<>();
        coarseOverrides.put("NijxmpsBGB6Rcmbr_K1n", Duration.ofMinutes(5));
        coarseOverrides.put("aT1I7ZoBpHa2ZXX_08qa", Duration.ofMinutes(20));
        coarseOverrides.put("YD1I7ZoBpHa2ZXX_c8on", Duration.ofMinutes(60));

        return Arrays
            .asList(
                new Scenario("base_sample", baseSample, detectorsForAnomalies(baseSample, Collections.emptyMap()), expectedBaseSample()),
                new Scenario("global_lag", globalLag, detectorsForAnomalies(globalLag, Collections.emptyMap()), expectedGlobalLag()),
                new Scenario(
                    "nested_short_inside_long",
                    nestedShortInsideLong,
                    detectorsForAnomalies(nestedShortInsideLong, Collections.emptyMap()),
                    expectedNestedShortInsideLong()
                ),
                new Scenario(
                    "braided_incidents_close",
                    braidedIncidentsClose,
                    detectorsForAnomalies(braidedIncidentsClose, Collections.emptyMap()),
                    expectedBraidedIncidentsClose()
                ),
                new Scenario(
                    "near_miss_requires_delta",
                    nearMissRequiresDelta,
                    detectorsForAnomalies(nearMissRequiresDelta, Collections.emptyMap()),
                    expectedNearMissRequiresDelta()
                ),
                new Scenario(
                    "scenario_one_long_many_shorts",
                    oneLongManyShorts,
                    detectorsForAnomalies(oneLongManyShorts, Collections.emptyMap()),
                    expectedOneLongManyShorts()
                ),
                new Scenario(
                    "fragmentation_many_to_one",
                    fragmentationManyToOne,
                    detectorsForAnomalies(fragmentationManyToOne, Collections.emptyMap()),
                    expectedFragmentationManyToOne()
                ),
                new Scenario(
                    "noise_outliers",
                    noiseOutliers,
                    detectorsForAnomalies(noiseOutliers, Collections.emptyMap()),
                    expectedNoiseOutliers()
                ),
                new Scenario(
                    "midnight_boundary",
                    midnightBoundary,
                    detectorsForAnomalies(midnightBoundary, Collections.emptyMap()),
                    expectedMidnightBoundary()
                ),
                new Scenario(
                    "two_close_incidents_clear_split",
                    twoCloseIncidentsClearSplit,
                    detectorsForAnomalies(twoCloseIncidentsClearSplit, Collections.emptyMap()),
                    expectedTwoCloseIncidentsClearSplit()
                ),
                new Scenario(
                    "random_synthetic_seed7_fixture",
                    randomSyntheticFixture,
                    detectorsForAnomalies(randomSyntheticFixture, Collections.emptyMap()),
                    expectedRandomSyntheticSeed7Fixture()
                ),
                new Scenario(
                    "coarse_detector_bridges_gap",
                    coarseDetectorBridgesGap,
                    detectorsForAnomalies(coarseDetectorBridgesGap, coarseOverrides),
                    expectedCoarseDetectorBridgesGap()
                )
            );
    }

    // -----------------------
    // Helpers
    // -----------------------
    private static final Duration DEFAULT_TEST_DETECTOR_INTERVAL = Duration.ofMinutes(5);

    private static AnomalyDetector detector(String detectorId, Duration interval) {
        return detector(detectorId, interval, AggregationBuilders.avg("avg").field("value"));
    }

    private static AnomalyDetector detector(String detectorId, Duration interval, AggregationBuilder aggregation) {
        long intervalSeconds = Math.max(1L, interval.getSeconds());
        IntervalTimeConfiguration intervalConfig = new IntervalTimeConfiguration(intervalSeconds, ChronoUnit.SECONDS);
        Feature feature = new Feature(detectorId + "-feature", detectorId + "-feature", true, aggregation);
        return new AnomalyDetector(
            detectorId,
            1L,
            detectorId,
            null,
            "timestamp",
            Collections.singletonList("index"),
            Collections.singletonList(feature),
            QueryBuilders.matchAllQuery(),
            intervalConfig,
            new IntervalTimeConfiguration(0, ChronoUnit.MINUTES),
            1,
            null,
            1,
            Instant.EPOCH,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    private static List<AnomalyDetector> detectorsForAnomalies(List<Anomaly> anomalies, Map<String, Duration> overrides) {
        return detectorsForAnomalies(anomalies, overrides, () -> AggregationBuilders.avg("avg").field("value"));
    }

    private static List<AnomalyDetector> detectorsForAnomalies(
        List<Anomaly> anomalies,
        Map<String, Duration> overrides,
        Supplier<AggregationBuilder> aggregationSupplier
    ) {
        Map<String, Duration> intervalsById = new HashMap<>();
        for (Anomaly anomaly : anomalies) {
            intervalsById.putIfAbsent(anomaly.getConfigId(), DEFAULT_TEST_DETECTOR_INTERVAL);
        }
        if (overrides != null) {
            intervalsById.putAll(overrides);
        }
        List<AnomalyDetector> detectors = new ArrayList<>(intervalsById.size());
        for (Map.Entry<String, Duration> entry : intervalsById.entrySet()) {
            detectors.add(detector(entry.getKey(), entry.getValue(), aggregationSupplier.get()));
        }
        return detectors;
    }

    private static Anomaly anomaly(String modelId, String configId, Instant dataStartTime, Instant dataEndTime) {
        return new Anomaly(modelId, configId, dataStartTime, dataEndTime);
    }

    private static List<List<String>> toIdClusters(List<List<Anomaly>> clusters) {
        List<List<String>> out = new ArrayList<>();
        for (List<Anomaly> c : clusters) {
            List<String> ids = new ArrayList<>(c.size());
            for (Anomaly a : c)
                ids.add(a.getModelId());
            out.add(ids);
        }
        return out;
    }

    private static List<List<String>> toIdClustersFromClusters(List<AnomalyCorrelation.Cluster> clusters) {
        List<List<Anomaly>> anomalies = new ArrayList<>(clusters.size());
        for (AnomalyCorrelation.Cluster cluster : clusters) {
            anomalies.add(cluster.getAnomalies());
        }
        return toIdClusters(anomalies);
    }

    private static List<AnomalyCorrelation.EventWindow> toEventWindowsFromClusters(List<AnomalyCorrelation.Cluster> clusters) {
        List<AnomalyCorrelation.EventWindow> windows = new ArrayList<>(clusters.size());
        for (AnomalyCorrelation.Cluster cluster : clusters) {
            windows.add(cluster.getEventWindow());
        }
        return windows;
    }

    private static List<List<String>> filterSingletonClusters(List<List<String>> clusters) {
        List<List<String>> out = new ArrayList<>();
        for (List<String> cluster : clusters) {
            if (cluster.size() > 1) {
                out.add(cluster);
            }
        }
        return out;
    }

    private static final class Scenario {
        private final String name;
        private final List<Anomaly> anomalies;
        private final List<AnomalyDetector> detectors;
        private final List<List<String>> expectedClusterIds;

        private Scenario(String name, List<Anomaly> anomalies, List<AnomalyDetector> detectors, List<List<String>> expectedClusterIds) {
            this.name = name;
            this.anomalies = anomalies;
            this.detectors = detectors;
            this.expectedClusterIds = expectedClusterIds;
        }
    }

    private static List<AnomalyCorrelation.EventWindow> expectedEventWindows(
        List<Anomaly> anomalies,
        List<List<String>> expectedClusterIds
    ) {
        Map<String, Anomaly> byId = new HashMap<>();
        for (Anomaly anomaly : anomalies) {
            byId.put(anomaly.getModelId(), anomaly);
        }

        List<AnomalyCorrelation.EventWindow> windows = new ArrayList<>(expectedClusterIds.size());
        for (List<String> clusterIds : expectedClusterIds) {
            Instant minStart = null;
            Instant maxEnd = null;
            for (String id : clusterIds) {
                Anomaly anomaly = byId.get(id);
                if (anomaly == null) {
                    throw new IllegalArgumentException("Missing anomaly id " + id);
                }
                Instant start = anomaly.getDataStartTime();
                Instant end = anomaly.getDataEndTime();
                if (minStart == null || start.isBefore(minStart)) {
                    minStart = start;
                }
                if (maxEnd == null || end.isAfter(maxEnd)) {
                    maxEnd = end;
                }
            }
            windows.add(new AnomalyCorrelation.EventWindow(minStart, maxEnd));
        }
        return windows;
    }

    private static List<List<String>> L2(List<String>... lists) {
        return Arrays.asList(lists);
    }

    private static List<String> L(String... ids) {
        return Arrays.asList(ids);
    }

    // -----------------------
    // Scenario builders
    // -----------------------

    private static List<Anomaly> sampleData() {
        Instant T0 = Instant.parse("2025-03-01T09:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // detector_A
        a.add(anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(25))));
        a.add(anomaly("A-2", "detector_A", T0.plus(Duration.ofMinutes(50)), T0.plus(Duration.ofMinutes(75))));
        a.add(anomaly("A-3", "detector_A", T0.plus(Duration.ofMinutes(100)), T0.plus(Duration.ofMinutes(115))));
        a.add(anomaly("A-4", "detector_A", T0.plus(Duration.ofMinutes(520)), T0.plus(Duration.ofMinutes(595))));

        // detector_B
        a.add(anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(12)), T0.plus(Duration.ofMinutes(23))));
        a.add(anomaly("B-2", "detector_B", T0.plus(Duration.ofMinutes(53)), T0.plus(Duration.ofMinutes(70))));
        a.add(anomaly("B-3", "detector_B", T0.plus(Duration.ofMinutes(105)), T0.plus(Duration.ofMinutes(120))));
        a.add(anomaly("B-4", "detector_B", T0.plus(Duration.ofMinutes(135)), T0.plus(Duration.ofMinutes(150))));

        // detector_C
        a.add(anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(8)), T0.plus(Duration.ofMinutes(30))));
        a.add(anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(59)), T0.plus(Duration.ofMinutes(105))));
        a.add(anomaly("C-3", "detector_C", T0.plus(Duration.ofMinutes(135)), T0.plus(Duration.ofMinutes(140))));

        return a;
    }

    private static List<Anomaly> scenarioGlobalLag() {
        Instant T0 = Instant.parse("2025-03-01T09:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(25))));
        a.add(anomaly("A-2", "detector_A", T0.plus(Duration.ofMinutes(50)), T0.plus(Duration.ofMinutes(75))));
        a.add(anomaly("A-3", "detector_A", T0.plus(Duration.ofMinutes(100)), T0.plus(Duration.ofMinutes(115))));

        // B
        a.add(anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(13)), T0.plus(Duration.ofMinutes(24))));
        a.add(anomaly("B-2", "detector_B", T0.plus(Duration.ofMinutes(53)), T0.plus(Duration.ofMinutes(71))));
        a.add(anomaly("B-3", "detector_B", T0.plus(Duration.ofMinutes(103)), T0.plus(Duration.ofMinutes(118))));
        a.add(anomaly("B-4", "detector_B", T0.plus(Duration.ofMinutes(140)), T0.plus(Duration.ofMinutes(150))));

        // C
        a.add(anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(11)), T0.plus(Duration.ofMinutes(27))));
        a.add(anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(51)), T0.plus(Duration.ofMinutes(77))));
        a.add(anomaly("C-3", "detector_C", T0.plus(Duration.ofMinutes(101)), T0.plus(Duration.ofMinutes(116))));

        return a;
    }

    private static List<Anomaly> scenarioNestedShortInsideLong() {
        Instant T0 = Instant.parse("2025-03-02T09:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(15))));
        a.add(anomaly("A-2", "detector_A", T0.plus(Duration.ofMinutes(60)), T0.plus(Duration.ofMinutes(65))));

        // B
        a.add(anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(12)), T0.plus(Duration.ofMinutes(14))));
        a.add(anomaly("B-2", "detector_B", T0.plus(Duration.ofMinutes(61)), T0.plus(Duration.ofMinutes(67))));

        // C
        a.add(anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(8)), T0.plus(Duration.ofMinutes(70))));
        a.add(anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(95)), T0.plus(Duration.ofMinutes(130))));

        return a;
    }

    private static List<Anomaly> scenarioBraidedIncidentsClose() {
        Instant T0 = Instant.parse("2025-03-02T12:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(50)), T0.plus(Duration.ofMinutes(64))));
        a.add(anomaly("A-2", "detector_A", T0.plus(Duration.ofMinutes(71)), T0.plus(Duration.ofMinutes(85))));

        // B
        a.add(anomaly("B-bridge", "detector_B", T0.plus(Duration.ofMinutes(52)), T0.plus(Duration.ofMinutes(83))));

        // C
        a.add(anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(49)), T0.plus(Duration.ofMinutes(55))));
        a.add(anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(77)), T0.plus(Duration.ofMinutes(86))));

        return a;
    }

    private static List<Anomaly> scenarioNearMissRequiresDelta() {
        Instant T0 = Instant.parse("2025-03-03T09:00:00Z");
        return Arrays
            .asList(
                anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(20))),
                anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(20)), T0.plus(Duration.ofMinutes(30))),
                anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(60)), T0.plus(Duration.ofMinutes(70)))
            );
    }

    private static List<Anomaly> scenarioOneLongManyShorts() {
        Instant T0 = Instant.parse("2025-03-06T10:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(anomaly("A-long", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(70))));

        // B
        a.add(anomaly("B-s1", "detector_B", T0.plus(Duration.ofMinutes(15)), T0.plus(Duration.ofMinutes(22))));
        a.add(anomaly("B-s2", "detector_B", T0.plus(Duration.ofMinutes(28)), T0.plus(Duration.ofMinutes(33))));
        a.add(anomaly("B-s3", "detector_B", T0.plus(Duration.ofMinutes(40)), T0.plus(Duration.ofMinutes(45))));
        a.add(anomaly("B-s4", "detector_B", T0.plus(Duration.ofMinutes(55)), T0.plus(Duration.ofMinutes(61))));

        return a;
    }

    private static List<Anomaly> scenarioFragmentationManyToOne() {
        Instant T0 = Instant.parse("2025-03-03T11:00:00Z");
        return Arrays
            .asList(
                anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(40))),
                anomaly("B-1a", "detector_B", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(25))),
                anomaly("B-1b", "detector_B", T0.plus(Duration.ofMinutes(26)), T0.plus(Duration.ofMinutes(39))),
                anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(15)), T0.plus(Duration.ofMinutes(35)))
            );
    }

    private static List<Anomaly> scenarioNoiseOutliers() {
        Instant T0 = Instant.parse("2025-03-03T13:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(30)), T0.plus(Duration.ofMinutes(50))));

        // B
        a.add(anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(31)), T0.plus(Duration.ofMinutes(49))));
        a.add(anomaly("B-2", "detector_B", T0.plus(Duration.ofMinutes(5)), T0.plus(Duration.ofMinutes(10))));
        a.add(anomaly("B-3", "detector_B", T0.plus(Duration.ofMinutes(120)), T0.plus(Duration.ofMinutes(130))));

        // C
        a.add(anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(32)), T0.plus(Duration.ofMinutes(48))));
        a.add(anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(170)), T0.plus(Duration.ofMinutes(175))));

        return a;
    }

    private static List<Anomaly> scenarioMidnightBoundary() {
        Instant T0 = Instant.parse("2025-03-03T23:50:00Z");
        return Arrays
            .asList(
                anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(8)), T0.plus(Duration.ofMinutes(20))),
                anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(9)), T0.plus(Duration.ofMinutes(17))),
                anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(12)), T0.plus(Duration.ofMinutes(21)))
            );
    }

    private static List<Anomaly> scenarioTwoCloseIncidentsClearSplit() {
        Instant T0 = Instant.parse("2025-03-04T14:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(20))));
        a.add(anomaly("A-2", "detector_A", T0.plus(Duration.ofMinutes(33)), T0.plus(Duration.ofMinutes(43))));

        // B
        a.add(anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(12)), T0.plus(Duration.ofMinutes(22))));
        a.add(anomaly("B-2", "detector_B", T0.plus(Duration.ofMinutes(35)), T0.plus(Duration.ofMinutes(45))));

        // C
        a.add(anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(11)), T0.plus(Duration.ofMinutes(19))));
        a.add(anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(34)), T0.plus(Duration.ofMinutes(44))));

        return a;
    }

    /**
     * Stress test with lag, jitter, drops, fragmentation, and spurious events.
     * 
     * @return The list of anomalies.
     */
    private static List<Anomaly> scenarioRandomSyntheticFixture() {
        List<Anomaly> anomalies = new ArrayList<>();

        anomalies
            .add(
                anomaly(
                    "detector_A-1.1",
                    "detector_A",
                    Instant.parse("2025-03-05T09:19:00.935217773Z"),
                    Instant.parse("2025-03-05T09:24:00.935217773Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_A-2.1",
                    "detector_A",
                    Instant.parse("2025-03-05T09:33:00.935217773Z"),
                    Instant.parse("2025-03-05T09:43:00.935217773Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_A-3.1",
                    "detector_A",
                    Instant.parse("2025-03-05T09:47:00.935217773Z"),
                    Instant.parse("2025-03-05T09:59:00.935217773Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_A-4.1",
                    "detector_A",
                    Instant.parse("2025-03-05T10:05:00.935217773Z"),
                    Instant.parse("2025-03-05T10:26:00.935217773Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_A-5.1",
                    "detector_A",
                    Instant.parse("2025-03-05T10:19:00.935217773Z"),
                    Instant.parse("2025-03-05T10:30:00.935217773Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_A-5.2",
                    "detector_A",
                    Instant.parse("2025-03-05T10:30:00.935217773Z"),
                    Instant.parse("2025-03-05T10:35:00.935217773Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_A-6.1",
                    "detector_A",
                    Instant.parse("2025-03-05T10:38:00.935217773Z"),
                    Instant.parse("2025-03-05T10:47:00.935217773Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_A-7.1",
                    "detector_A",
                    Instant.parse("2025-03-05T10:53:00.935217773Z"),
                    Instant.parse("2025-03-05T11:12:00.935217773Z")
                )
            );
        anomalies
            .add(anomaly("detector_A-spur-1", "detector_A", Instant.parse("2025-03-05T11:01:00Z"), Instant.parse("2025-03-05T11:12:00Z")));

        anomalies
            .add(
                anomaly(
                    "detector_B-1.1",
                    "detector_B",
                    Instant.parse("2025-03-05T09:19:00.034569848Z"),
                    Instant.parse("2025-03-05T09:24:00.034569848Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_B-2.1",
                    "detector_B",
                    Instant.parse("2025-03-05T09:32:00.034569848Z"),
                    Instant.parse("2025-03-05T09:44:00.034569848Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_B-3.1",
                    "detector_B",
                    Instant.parse("2025-03-05T09:48:00.034569848Z"),
                    Instant.parse("2025-03-05T09:58:00.034569848Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_B-4.1",
                    "detector_B",
                    Instant.parse("2025-03-05T10:05:00.034569848Z"),
                    Instant.parse("2025-03-05T10:18:00.034569848Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_B-4.2",
                    "detector_B",
                    Instant.parse("2025-03-05T10:19:00.034569848Z"),
                    Instant.parse("2025-03-05T10:26:00.034569848Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_B-5.1",
                    "detector_B",
                    Instant.parse("2025-03-05T10:19:00.034569848Z"),
                    Instant.parse("2025-03-05T10:29:00.034569848Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_B-6.1",
                    "detector_B",
                    Instant.parse("2025-03-05T10:39:00.034569848Z"),
                    Instant.parse("2025-03-05T10:48:00.034569848Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_B-7.1",
                    "detector_B",
                    Instant.parse("2025-03-05T10:55:00.034569848Z"),
                    Instant.parse("2025-03-05T11:05:00.034569848Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_B-7.2",
                    "detector_B",
                    Instant.parse("2025-03-05T11:05:00.034569848Z"),
                    Instant.parse("2025-03-05T11:12:00.034569848Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_B-8.1",
                    "detector_B",
                    Instant.parse("2025-03-05T11:06:00.034569848Z"),
                    Instant.parse("2025-03-05T11:19:00.034569848Z")
                )
            );
        anomalies
            .add(anomaly("detector_B-spur-1", "detector_B", Instant.parse("2025-03-05T11:08:00Z"), Instant.parse("2025-03-05T11:19:00Z")));
        anomalies
            .add(
                anomaly(
                    "detector_B-8.2",
                    "detector_B",
                    Instant.parse("2025-03-05T11:19:00.034569848Z"),
                    Instant.parse("2025-03-05T11:26:00.034569848Z")
                )
            );

        anomalies
            .add(
                anomaly(
                    "detector_C-1.1",
                    "detector_C",
                    Instant.parse("2025-03-05T09:18:59.999458629Z"),
                    Instant.parse("2025-03-05T09:24:59.999458629Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_C-2.1",
                    "detector_C",
                    Instant.parse("2025-03-05T09:34:59.999458629Z"),
                    Instant.parse("2025-03-05T09:43:59.999458629Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_C-3.1",
                    "detector_C",
                    Instant.parse("2025-03-05T09:47:59.999458629Z"),
                    Instant.parse("2025-03-05T09:59:59.999458629Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_C-4.1",
                    "detector_C",
                    Instant.parse("2025-03-05T10:04:59.999458629Z"),
                    Instant.parse("2025-03-05T10:25:59.999458629Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_C-7.1",
                    "detector_C",
                    Instant.parse("2025-03-05T10:52:59.999458629Z"),
                    Instant.parse("2025-03-05T11:12:59.999458629Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_C-8.1",
                    "detector_C",
                    Instant.parse("2025-03-05T11:05:59.999458629Z"),
                    Instant.parse("2025-03-05T11:26:59.999458629Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_C-8.2",
                    "detector_C",
                    Instant.parse("2025-03-05T11:26:59.999458629Z"),
                    Instant.parse("2025-03-05T11:29:59.999458629Z")
                )
            );
        anomalies
            .add(anomaly("detector_C-spur-2", "detector_C", Instant.parse("2025-03-05T10:57:00Z"), Instant.parse("2025-03-05T11:05:00Z")));
        anomalies
            .add(anomaly("detector_C-spur-3", "detector_C", Instant.parse("2025-03-05T12:21:00Z"), Instant.parse("2025-03-05T12:30:00Z")));
        anomalies
            .add(anomaly("detector_C-spur-4", "detector_C", Instant.parse("2025-03-05T12:40:00Z"), Instant.parse("2025-03-05T12:48:00Z")));
        anomalies
            .add(anomaly("detector_C-spur-1", "detector_C", Instant.parse("2025-03-05T13:07:00Z"), Instant.parse("2025-03-05T13:11:00Z")));

        anomalies
            .add(
                anomaly(
                    "detector_D-1.1",
                    "detector_D",
                    Instant.parse("2025-03-05T09:19:00.710323073Z"),
                    Instant.parse("2025-03-05T09:24:00.710323073Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_D-2.1",
                    "detector_D",
                    Instant.parse("2025-03-05T09:33:00.710323073Z"),
                    Instant.parse("2025-03-05T09:43:00.710323073Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_D-3.1",
                    "detector_D",
                    Instant.parse("2025-03-05T09:48:00.710323073Z"),
                    Instant.parse("2025-03-05T09:58:00.710323073Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_D-4.1",
                    "detector_D",
                    Instant.parse("2025-03-05T10:06:00.710323073Z"),
                    Instant.parse("2025-03-05T10:25:00.710323073Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_D-5.1",
                    "detector_D",
                    Instant.parse("2025-03-05T10:19:00.710323073Z"),
                    Instant.parse("2025-03-05T10:29:00.710323073Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_D-6.1",
                    "detector_D",
                    Instant.parse("2025-03-05T10:39:00.710323073Z"),
                    Instant.parse("2025-03-05T10:48:00.710323073Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_D-7.1",
                    "detector_D",
                    Instant.parse("2025-03-05T10:54:00.710323073Z"),
                    Instant.parse("2025-03-05T11:05:00.710323073Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_D-7.2",
                    "detector_D",
                    Instant.parse("2025-03-05T11:05:00.710323073Z"),
                    Instant.parse("2025-03-05T11:12:00.710323073Z")
                )
            );
        anomalies
            .add(
                anomaly(
                    "detector_D-8.1",
                    "detector_D",
                    Instant.parse("2025-03-05T11:06:00.710323073Z"),
                    Instant.parse("2025-03-05T11:18:00.710323073Z")
                )
            );
        anomalies
            .add(anomaly("detector_D-spur-2", "detector_D", Instant.parse("2025-03-05T10:34:00Z"), Instant.parse("2025-03-05T10:41:00Z")));
        anomalies
            .add(anomaly("detector_D-spur-3", "detector_D", Instant.parse("2025-03-05T10:52:00Z"), Instant.parse("2025-03-05T10:59:00Z")));
        anomalies
            .add(anomaly("detector_D-spur-1", "detector_D", Instant.parse("2025-03-05T11:03:00Z"), Instant.parse("2025-03-05T11:11:00Z")));
        anomalies
            .add(anomaly("detector_D-spur-4", "detector_D", Instant.parse("2025-03-05T09:45:00Z"), Instant.parse("2025-03-05T09:50:00Z")));

        return anomalies;
    }

    private static List<Anomaly> scenarioCoarseDetectorBridgesGap() {
        Instant t10 = Instant.parse("2023-09-29T10:00:00Z");
        Instant t11 = Instant.parse("2023-09-29T11:00:00Z");

        Duration I5 = Duration.ofMinutes(5);
        Duration I20 = Duration.ofMinutes(20);
        Duration I60 = Duration.ofMinutes(60);

        List<Anomaly> a = new ArrayList<>();

        // Cluster 6
        a.add(anomaly("BL27wJsBhNwQmcgY3Umv", "NijxmpsBGB6Rcmbr_K1n", t10, t10.plus(I5)));
        a.add(anomaly("DouMwJsBhNwQmcgYoLll", "NijxmpsBGB6Rcmbr_K1n", t10, t10.plus(I5)));
        a.add(anomaly("I4KHwJsBhNwQmcgYr6RD", "NijxmpsBGB6Rcmbr_K1n", t10, t10.plus(I5)));
        a.add(anomaly("en12wJsBhNwQmcgYE2sN", "aT1I7ZoBpHa2ZXX_08qa", t10, t10.plus(I20)));

        // Cluster 7
        a.add(anomaly("SpiowJsBhNwQmcgYXODf", "YD1I7ZoBpHa2ZXX_c8on", t11, t11.plus(I60)));
        a.add(anomaly("Ur28wJsBhNwQmcgYLMKm", "NijxmpsBGB6Rcmbr_K1n", t11, t11.plus(I5)));
        a.add(anomaly("aKOuwJsBhNwQmcgYNxfo", "NijxmpsBGB6Rcmbr_K1n", t11, t11.plus(I5)));
        a.add(anomaly("c4WJwJsBhNwQmcgYifkR", "NijxmpsBGB6Rcmbr_K1n", t11, t11.plus(I5)));

        return a;
    }

    // -----------------------
    // Expected clusters (based on the scenario)
    // -----------------------

    private static List<List<String>> expectedBaseSample() {
        return L2(L("A-1", "B-1", "C-1"), L("A-2", "B-2"), L("A-3", "B-3"), L("A-4"), L("B-4", "C-3"), L("C-2"));
    }

    private static List<List<String>> expectedGlobalLag() {
        return L2(L("A-1", "B-1", "C-1"), L("A-2", "B-2", "C-2"), L("A-3", "B-3", "C-3"), L("B-4"));
    }

    private static List<List<String>> expectedNestedShortInsideLong() {
        return L2(L("A-1", "B-1"), L("A-2", "B-2"), L("C-1"), L("C-2"));
    }

    private static List<List<String>> expectedBraidedIncidentsClose() {
        return L2(L("A-1", "C-1"), L("A-2", "C-2"), L("B-bridge"));
    }

    private static List<List<String>> expectedNearMissRequiresDelta() {
        return L2(L("A-1", "B-1"), L("C-1"));
    }

    private static List<List<String>> expectedOneLongManyShorts() {
        return L2(L("A-long", "B-s1", "B-s2", "B-s3", "B-s4"));
    }

    private static List<List<String>> expectedFragmentationManyToOne() {
        return L2(L("A-1", "B-1a", "B-1b", "C-1"));
    }

    private static List<List<String>> expectedNoiseOutliers() {
        return L2(L("A-1", "B-1", "C-1"), L("B-2"), L("B-3"), L("C-2"));
    }

    private static List<List<String>> expectedMidnightBoundary() {
        return L2(L("A-1", "B-1", "C-1"));
    }

    private static List<List<String>> expectedTwoCloseIncidentsClearSplit() {
        return L2(L("A-1", "B-1", "C-1"), L("A-2", "B-2", "C-2"));
    }

    private static List<List<String>> expectedRandomSyntheticSeed7Fixture() {
        return L2(
            L("detector_A-1.1", "detector_B-1.1", "detector_C-1.1", "detector_D-1.1"),
            L(
                "detector_A-2.1",
                "detector_A-3.1",
                "detector_B-2.1",
                "detector_B-3.1",
                "detector_C-2.1",
                "detector_C-3.1",
                "detector_D-2.1",
                "detector_D-3.1",
                "detector_D-spur-4"
            ),
            L(
                "detector_A-4.1",
                "detector_A-5.1",
                "detector_A-5.2",
                "detector_A-6.1",
                "detector_B-4.1",
                "detector_B-4.2",
                "detector_B-5.1",
                "detector_B-6.1",
                "detector_C-4.1",
                "detector_D-4.1",
                "detector_D-5.1",
                "detector_D-6.1",
                "detector_D-spur-2"
            ),
            L(
                "detector_A-7.1",
                "detector_A-spur-1",
                "detector_B-7.1",
                "detector_B-7.2",
                "detector_B-8.1",
                "detector_B-8.2",
                "detector_B-spur-1",
                "detector_C-7.1",
                "detector_C-8.1",
                "detector_C-8.2",
                "detector_C-spur-2",
                "detector_D-7.1",
                "detector_D-7.2",
                "detector_D-8.1",
                "detector_D-spur-1",
                "detector_D-spur-3"
            ),
            L("detector_C-spur-3"),
            L("detector_C-spur-4"),
            L("detector_C-spur-1")
        );
    }

    private static List<List<String>> expectedCoarseDetectorBridgesGap() {
        return L2(
            L(
                "BL27wJsBhNwQmcgY3Umv",
                "DouMwJsBhNwQmcgYoLll",
                "I4KHwJsBhNwQmcgYr6RD",
                "SpiowJsBhNwQmcgYXODf",
                "Ur28wJsBhNwQmcgYLMKm",
                "aKOuwJsBhNwQmcgYNxfo",
                "c4WJwJsBhNwQmcgYifkR",
                "en12wJsBhNwQmcgYE2sN"
            )
        );
    }

    private static List<List<String>> expectedCoarseDetectorNoBridge() {
        return L2(
            L("BL27wJsBhNwQmcgY3Umv", "DouMwJsBhNwQmcgYoLll", "I4KHwJsBhNwQmcgYr6RD", "en12wJsBhNwQmcgYE2sN"),
            L("SpiowJsBhNwQmcgYXODf", "Ur28wJsBhNwQmcgYLMKm", "aKOuwJsBhNwQmcgYNxfo", "c4WJwJsBhNwQmcgYifkR")
        );
    }
}
