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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.test.OpenSearchTestCase;

public class AnomalyCorrelationScenariosTests extends OpenSearchTestCase {

    public void test_cluster_with_event_windows_matches_expected() {
        for (Scenario scenario : scenarioProvider()) {
            List<AnomalyCorrelation.Cluster> clusters = AnomalyCorrelation.clusterWithEventWindows(scenario.anomalies);

            List<List<String>> actualClusters = toIdClustersFromClusters(clusters);
            assertEquals("Cluster membership mismatch for scenario " + scenario.name, scenario.expectedClusterIds, actualClusters);

            List<AnomalyCorrelation.EventWindow> actualWindows = toEventWindowsFromClusters(clusters);
            List<AnomalyCorrelation.EventWindow> expectedWindows = expectedEventWindows(scenario.anomalies, scenario.expectedClusterIds);
            assertEquals("Cluster event window mismatch for scenario " + scenario.name, expectedWindows, actualWindows);
        }
    }

    public void test_cluster_with_event_windows_omits_singletons_when_disabled() {
        for (Scenario scenario : scenarioProvider()) {
            List<AnomalyCorrelation.Cluster> clusters = AnomalyCorrelation.clusterWithEventWindows(scenario.anomalies, false);

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
        Anomaly anomaly = new Anomaly("A-1", "detector_A", start, end);
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
        Anomaly anomaly = new Anomaly("A-1", "detector_A", start, end);
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

    // -----------------------
    // Provider (all scenarios except jackie)
    // -----------------------
    static List<Scenario> scenarioProvider() {
        return Arrays
            .asList(
                new Scenario("base_sample", sampleData(), expectedBaseSample()),
                new Scenario("global_lag", scenarioGlobalLag(), expectedGlobalLag()),
                new Scenario("nested_short_inside_long", scenarioNestedShortInsideLong(), expectedNestedShortInsideLong()),
                new Scenario("braided_incidents_close", scenarioBraidedIncidentsClose(), expectedBraidedIncidentsClose()),
                new Scenario("near_miss_requires_delta", scenarioNearMissRequiresDelta(), expectedNearMissRequiresDelta()),
                new Scenario("scenario_one_long_many_shorts", scenarioOneLongManyShorts(), expectedOneLongManyShorts()),
                new Scenario("fragmentation_many_to_one", scenarioFragmentationManyToOne(), expectedFragmentationManyToOne()),
                new Scenario("noise_outliers", scenarioNoiseOutliers(), expectedNoiseOutliers()),
                new Scenario("midnight_boundary", scenarioMidnightBoundary(), expectedMidnightBoundary()),
                new Scenario(
                    "two_close_incidents_clear_split",
                    scenarioTwoCloseIncidentsClearSplit(),
                    expectedTwoCloseIncidentsClearSplit()
                ),
                new Scenario("random_synthetic_seed7_fixture", scenarioRandomSyntheticFixture(), expectedRandomSyntheticSeed7Fixture())
            );
    }

    // -----------------------
    // Helpers
    // -----------------------
    private static List<List<String>> toIdClusters(List<List<Anomaly>> clusters) {
        List<List<String>> out = new ArrayList<>();
        for (List<Anomaly> c : clusters) {
            List<String> ids = new ArrayList<>(c.size());
            for (Anomaly a : c)
                ids.add(a.getId());
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
        private final List<List<String>> expectedClusterIds;

        private Scenario(String name, List<Anomaly> anomalies, List<List<String>> expectedClusterIds) {
            this.name = name;
            this.anomalies = anomalies;
            this.expectedClusterIds = expectedClusterIds;
        }
    }

    private static List<AnomalyCorrelation.EventWindow> expectedEventWindows(
        List<Anomaly> anomalies,
        List<List<String>> expectedClusterIds
    ) {
        Map<String, Anomaly> byId = new HashMap<>();
        for (Anomaly anomaly : anomalies) {
            byId.put(anomaly.getId(), anomaly);
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
        a.add(new Anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(25))));
        a.add(new Anomaly("A-2", "detector_A", T0.plus(Duration.ofMinutes(50)), T0.plus(Duration.ofMinutes(75))));
        a.add(new Anomaly("A-3", "detector_A", T0.plus(Duration.ofMinutes(100)), T0.plus(Duration.ofMinutes(115))));
        a.add(new Anomaly("A-4", "detector_A", T0.plus(Duration.ofMinutes(520)), T0.plus(Duration.ofMinutes(595))));

        // detector_B
        a.add(new Anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(12)), T0.plus(Duration.ofMinutes(23))));
        a.add(new Anomaly("B-2", "detector_B", T0.plus(Duration.ofMinutes(53)), T0.plus(Duration.ofMinutes(70))));
        a.add(new Anomaly("B-3", "detector_B", T0.plus(Duration.ofMinutes(105)), T0.plus(Duration.ofMinutes(120))));
        a.add(new Anomaly("B-4", "detector_B", T0.plus(Duration.ofMinutes(135)), T0.plus(Duration.ofMinutes(150))));

        // detector_C
        a.add(new Anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(8)), T0.plus(Duration.ofMinutes(30))));
        a.add(new Anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(59)), T0.plus(Duration.ofMinutes(105))));
        a.add(new Anomaly("C-3", "detector_C", T0.plus(Duration.ofMinutes(135)), T0.plus(Duration.ofMinutes(140))));

        return a;
    }

    private static List<Anomaly> scenarioGlobalLag() {
        Instant T0 = Instant.parse("2025-03-01T09:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(new Anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(25))));
        a.add(new Anomaly("A-2", "detector_A", T0.plus(Duration.ofMinutes(50)), T0.plus(Duration.ofMinutes(75))));
        a.add(new Anomaly("A-3", "detector_A", T0.plus(Duration.ofMinutes(100)), T0.plus(Duration.ofMinutes(115))));

        // B
        a.add(new Anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(13)), T0.plus(Duration.ofMinutes(24))));
        a.add(new Anomaly("B-2", "detector_B", T0.plus(Duration.ofMinutes(53)), T0.plus(Duration.ofMinutes(71))));
        a.add(new Anomaly("B-3", "detector_B", T0.plus(Duration.ofMinutes(103)), T0.plus(Duration.ofMinutes(118))));
        a.add(new Anomaly("B-4", "detector_B", T0.plus(Duration.ofMinutes(140)), T0.plus(Duration.ofMinutes(150))));

        // C
        a.add(new Anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(11)), T0.plus(Duration.ofMinutes(27))));
        a.add(new Anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(51)), T0.plus(Duration.ofMinutes(77))));
        a.add(new Anomaly("C-3", "detector_C", T0.plus(Duration.ofMinutes(101)), T0.plus(Duration.ofMinutes(116))));

        return a;
    }

    private static List<Anomaly> scenarioNestedShortInsideLong() {
        Instant T0 = Instant.parse("2025-03-02T09:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(new Anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(15))));
        a.add(new Anomaly("A-2", "detector_A", T0.plus(Duration.ofMinutes(60)), T0.plus(Duration.ofMinutes(65))));

        // B
        a.add(new Anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(12)), T0.plus(Duration.ofMinutes(14))));
        a.add(new Anomaly("B-2", "detector_B", T0.plus(Duration.ofMinutes(61)), T0.plus(Duration.ofMinutes(67))));

        // C
        a.add(new Anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(8)), T0.plus(Duration.ofMinutes(70))));
        a.add(new Anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(95)), T0.plus(Duration.ofMinutes(130))));

        return a;
    }

    private static List<Anomaly> scenarioBraidedIncidentsClose() {
        Instant T0 = Instant.parse("2025-03-02T12:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(new Anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(50)), T0.plus(Duration.ofMinutes(64))));
        a.add(new Anomaly("A-2", "detector_A", T0.plus(Duration.ofMinutes(71)), T0.plus(Duration.ofMinutes(85))));

        // B
        a.add(new Anomaly("B-bridge", "detector_B", T0.plus(Duration.ofMinutes(52)), T0.plus(Duration.ofMinutes(83))));

        // C
        a.add(new Anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(49)), T0.plus(Duration.ofMinutes(55))));
        a.add(new Anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(77)), T0.plus(Duration.ofMinutes(86))));

        return a;
    }

    private static List<Anomaly> scenarioNearMissRequiresDelta() {
        Instant T0 = Instant.parse("2025-03-03T09:00:00Z");
        return Arrays
            .asList(
                new Anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(20))),
                new Anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(20)), T0.plus(Duration.ofMinutes(30))),
                new Anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(60)), T0.plus(Duration.ofMinutes(70)))
            );
    }

    private static List<Anomaly> scenarioOneLongManyShorts() {
        Instant T0 = Instant.parse("2025-03-06T10:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(new Anomaly("A-long", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(70))));

        // B
        a.add(new Anomaly("B-s1", "detector_B", T0.plus(Duration.ofMinutes(15)), T0.plus(Duration.ofMinutes(22))));
        a.add(new Anomaly("B-s2", "detector_B", T0.plus(Duration.ofMinutes(28)), T0.plus(Duration.ofMinutes(33))));
        a.add(new Anomaly("B-s3", "detector_B", T0.plus(Duration.ofMinutes(40)), T0.plus(Duration.ofMinutes(45))));
        a.add(new Anomaly("B-s4", "detector_B", T0.plus(Duration.ofMinutes(55)), T0.plus(Duration.ofMinutes(61))));

        return a;
    }

    private static List<Anomaly> scenarioFragmentationManyToOne() {
        Instant T0 = Instant.parse("2025-03-03T11:00:00Z");
        return Arrays
            .asList(
                new Anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(40))),
                new Anomaly("B-1a", "detector_B", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(25))),
                new Anomaly("B-1b", "detector_B", T0.plus(Duration.ofMinutes(26)), T0.plus(Duration.ofMinutes(39))),
                new Anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(15)), T0.plus(Duration.ofMinutes(35)))
            );
    }

    private static List<Anomaly> scenarioNoiseOutliers() {
        Instant T0 = Instant.parse("2025-03-03T13:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(new Anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(30)), T0.plus(Duration.ofMinutes(50))));

        // B
        a.add(new Anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(31)), T0.plus(Duration.ofMinutes(49))));
        a.add(new Anomaly("B-2", "detector_B", T0.plus(Duration.ofMinutes(5)), T0.plus(Duration.ofMinutes(10))));
        a.add(new Anomaly("B-3", "detector_B", T0.plus(Duration.ofMinutes(120)), T0.plus(Duration.ofMinutes(130))));

        // C
        a.add(new Anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(32)), T0.plus(Duration.ofMinutes(48))));
        a.add(new Anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(170)), T0.plus(Duration.ofMinutes(175))));

        return a;
    }

    private static List<Anomaly> scenarioMidnightBoundary() {
        Instant T0 = Instant.parse("2025-03-03T23:50:00Z");
        return Arrays
            .asList(
                new Anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(8)), T0.plus(Duration.ofMinutes(20))),
                new Anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(9)), T0.plus(Duration.ofMinutes(17))),
                new Anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(12)), T0.plus(Duration.ofMinutes(21)))
            );
    }

    private static List<Anomaly> scenarioTwoCloseIncidentsClearSplit() {
        Instant T0 = Instant.parse("2025-03-04T14:00:00Z");
        List<Anomaly> a = new ArrayList<>();

        // A
        a.add(new Anomaly("A-1", "detector_A", T0.plus(Duration.ofMinutes(10)), T0.plus(Duration.ofMinutes(20))));
        a.add(new Anomaly("A-2", "detector_A", T0.plus(Duration.ofMinutes(33)), T0.plus(Duration.ofMinutes(43))));

        // B
        a.add(new Anomaly("B-1", "detector_B", T0.plus(Duration.ofMinutes(12)), T0.plus(Duration.ofMinutes(22))));
        a.add(new Anomaly("B-2", "detector_B", T0.plus(Duration.ofMinutes(35)), T0.plus(Duration.ofMinutes(45))));

        // C
        a.add(new Anomaly("C-1", "detector_C", T0.plus(Duration.ofMinutes(11)), T0.plus(Duration.ofMinutes(19))));
        a.add(new Anomaly("C-2", "detector_C", T0.plus(Duration.ofMinutes(34)), T0.plus(Duration.ofMinutes(44))));

        return a;
    }

    /**
     * Stress test with lag, jitter, drops, fragmentation, and spurious events.
     * @return The list of anomalies.
     */
    private static List<Anomaly> scenarioRandomSyntheticFixture() {
        List<Anomaly> anomalies = new ArrayList<>();

        anomalies
            .add(
                new Anomaly(
                    "detector_A-1.1",
                    "detector_A",
                    Instant.parse("2025-03-05T09:19:00.935217773Z"),
                    Instant.parse("2025-03-05T09:24:00.935217773Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_A-2.1",
                    "detector_A",
                    Instant.parse("2025-03-05T09:33:00.935217773Z"),
                    Instant.parse("2025-03-05T09:43:00.935217773Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_A-3.1",
                    "detector_A",
                    Instant.parse("2025-03-05T09:47:00.935217773Z"),
                    Instant.parse("2025-03-05T09:59:00.935217773Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_A-4.1",
                    "detector_A",
                    Instant.parse("2025-03-05T10:05:00.935217773Z"),
                    Instant.parse("2025-03-05T10:26:00.935217773Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_A-5.1",
                    "detector_A",
                    Instant.parse("2025-03-05T10:19:00.935217773Z"),
                    Instant.parse("2025-03-05T10:30:00.935217773Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_A-5.2",
                    "detector_A",
                    Instant.parse("2025-03-05T10:30:00.935217773Z"),
                    Instant.parse("2025-03-05T10:35:00.935217773Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_A-6.1",
                    "detector_A",
                    Instant.parse("2025-03-05T10:38:00.935217773Z"),
                    Instant.parse("2025-03-05T10:47:00.935217773Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_A-7.1",
                    "detector_A",
                    Instant.parse("2025-03-05T10:53:00.935217773Z"),
                    Instant.parse("2025-03-05T11:12:00.935217773Z")
                )
            );
        anomalies
            .add(
                new Anomaly("detector_A-spur-1", "detector_A", Instant.parse("2025-03-05T11:01:00Z"), Instant.parse("2025-03-05T11:12:00Z"))
            );

        anomalies
            .add(
                new Anomaly(
                    "detector_B-1.1",
                    "detector_B",
                    Instant.parse("2025-03-05T09:19:00.034569848Z"),
                    Instant.parse("2025-03-05T09:24:00.034569848Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_B-2.1",
                    "detector_B",
                    Instant.parse("2025-03-05T09:32:00.034569848Z"),
                    Instant.parse("2025-03-05T09:44:00.034569848Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_B-3.1",
                    "detector_B",
                    Instant.parse("2025-03-05T09:48:00.034569848Z"),
                    Instant.parse("2025-03-05T09:58:00.034569848Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_B-4.1",
                    "detector_B",
                    Instant.parse("2025-03-05T10:05:00.034569848Z"),
                    Instant.parse("2025-03-05T10:18:00.034569848Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_B-4.2",
                    "detector_B",
                    Instant.parse("2025-03-05T10:19:00.034569848Z"),
                    Instant.parse("2025-03-05T10:26:00.034569848Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_B-5.1",
                    "detector_B",
                    Instant.parse("2025-03-05T10:19:00.034569848Z"),
                    Instant.parse("2025-03-05T10:29:00.034569848Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_B-6.1",
                    "detector_B",
                    Instant.parse("2025-03-05T10:39:00.034569848Z"),
                    Instant.parse("2025-03-05T10:48:00.034569848Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_B-7.1",
                    "detector_B",
                    Instant.parse("2025-03-05T10:55:00.034569848Z"),
                    Instant.parse("2025-03-05T11:05:00.034569848Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_B-7.2",
                    "detector_B",
                    Instant.parse("2025-03-05T11:05:00.034569848Z"),
                    Instant.parse("2025-03-05T11:12:00.034569848Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_B-8.1",
                    "detector_B",
                    Instant.parse("2025-03-05T11:06:00.034569848Z"),
                    Instant.parse("2025-03-05T11:19:00.034569848Z")
                )
            );
        anomalies
            .add(
                new Anomaly("detector_B-spur-1", "detector_B", Instant.parse("2025-03-05T11:08:00Z"), Instant.parse("2025-03-05T11:19:00Z"))
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_B-8.2",
                    "detector_B",
                    Instant.parse("2025-03-05T11:19:00.034569848Z"),
                    Instant.parse("2025-03-05T11:26:00.034569848Z")
                )
            );

        anomalies
            .add(
                new Anomaly(
                    "detector_C-1.1",
                    "detector_C",
                    Instant.parse("2025-03-05T09:18:59.999458629Z"),
                    Instant.parse("2025-03-05T09:24:59.999458629Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_C-2.1",
                    "detector_C",
                    Instant.parse("2025-03-05T09:34:59.999458629Z"),
                    Instant.parse("2025-03-05T09:43:59.999458629Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_C-3.1",
                    "detector_C",
                    Instant.parse("2025-03-05T09:47:59.999458629Z"),
                    Instant.parse("2025-03-05T09:59:59.999458629Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_C-4.1",
                    "detector_C",
                    Instant.parse("2025-03-05T10:04:59.999458629Z"),
                    Instant.parse("2025-03-05T10:25:59.999458629Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_C-7.1",
                    "detector_C",
                    Instant.parse("2025-03-05T10:52:59.999458629Z"),
                    Instant.parse("2025-03-05T11:12:59.999458629Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_C-8.1",
                    "detector_C",
                    Instant.parse("2025-03-05T11:05:59.999458629Z"),
                    Instant.parse("2025-03-05T11:26:59.999458629Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_C-8.2",
                    "detector_C",
                    Instant.parse("2025-03-05T11:26:59.999458629Z"),
                    Instant.parse("2025-03-05T11:29:59.999458629Z")
                )
            );
        anomalies
            .add(
                new Anomaly("detector_C-spur-2", "detector_C", Instant.parse("2025-03-05T10:57:00Z"), Instant.parse("2025-03-05T11:05:00Z"))
            );
        anomalies
            .add(
                new Anomaly("detector_C-spur-3", "detector_C", Instant.parse("2025-03-05T12:21:00Z"), Instant.parse("2025-03-05T12:30:00Z"))
            );
        anomalies
            .add(
                new Anomaly("detector_C-spur-4", "detector_C", Instant.parse("2025-03-05T12:40:00Z"), Instant.parse("2025-03-05T12:48:00Z"))
            );
        anomalies
            .add(
                new Anomaly("detector_C-spur-1", "detector_C", Instant.parse("2025-03-05T13:07:00Z"), Instant.parse("2025-03-05T13:11:00Z"))
            );

        anomalies
            .add(
                new Anomaly(
                    "detector_D-1.1",
                    "detector_D",
                    Instant.parse("2025-03-05T09:19:00.710323073Z"),
                    Instant.parse("2025-03-05T09:24:00.710323073Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_D-2.1",
                    "detector_D",
                    Instant.parse("2025-03-05T09:33:00.710323073Z"),
                    Instant.parse("2025-03-05T09:43:00.710323073Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_D-3.1",
                    "detector_D",
                    Instant.parse("2025-03-05T09:48:00.710323073Z"),
                    Instant.parse("2025-03-05T09:58:00.710323073Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_D-4.1",
                    "detector_D",
                    Instant.parse("2025-03-05T10:06:00.710323073Z"),
                    Instant.parse("2025-03-05T10:25:00.710323073Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_D-5.1",
                    "detector_D",
                    Instant.parse("2025-03-05T10:19:00.710323073Z"),
                    Instant.parse("2025-03-05T10:29:00.710323073Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_D-6.1",
                    "detector_D",
                    Instant.parse("2025-03-05T10:39:00.710323073Z"),
                    Instant.parse("2025-03-05T10:48:00.710323073Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_D-7.1",
                    "detector_D",
                    Instant.parse("2025-03-05T10:54:00.710323073Z"),
                    Instant.parse("2025-03-05T11:05:00.710323073Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_D-7.2",
                    "detector_D",
                    Instant.parse("2025-03-05T11:05:00.710323073Z"),
                    Instant.parse("2025-03-05T11:12:00.710323073Z")
                )
            );
        anomalies
            .add(
                new Anomaly(
                    "detector_D-8.1",
                    "detector_D",
                    Instant.parse("2025-03-05T11:06:00.710323073Z"),
                    Instant.parse("2025-03-05T11:18:00.710323073Z")
                )
            );
        anomalies
            .add(
                new Anomaly("detector_D-spur-2", "detector_D", Instant.parse("2025-03-05T10:34:00Z"), Instant.parse("2025-03-05T10:41:00Z"))
            );
        anomalies
            .add(
                new Anomaly("detector_D-spur-3", "detector_D", Instant.parse("2025-03-05T10:52:00Z"), Instant.parse("2025-03-05T10:59:00Z"))
            );
        anomalies
            .add(
                new Anomaly("detector_D-spur-1", "detector_D", Instant.parse("2025-03-05T11:03:00Z"), Instant.parse("2025-03-05T11:11:00Z"))
            );
        anomalies
            .add(
                new Anomaly("detector_D-spur-4", "detector_D", Instant.parse("2025-03-05T09:45:00Z"), Instant.parse("2025-03-05T09:50:00Z"))
            );

        return anomalies;
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
}
