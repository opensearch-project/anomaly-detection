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

package org.opensearch.ad.ml;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Map.Entry;

import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.Feature;
import org.opensearch.ad.model.IntervalTimeConfiguration;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.DiscoveryNodeFilterer;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.test.OpenSearchTestCase;

import com.google.common.collect.ImmutableList;

public class ModelPartitionerTests extends OpenSearchTestCase {
    private ModelPartitioner partitioner;
    private DiscoveryNodeFilterer nodeFilter;
    private MemoryTracker memoryTracker;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        nodeFilter = mock(DiscoveryNodeFilterer.class);
        memoryTracker = mock(MemoryTracker.class);
        when(memoryTracker.getDesiredModelSize()).thenReturn(200_000L);
        when(memoryTracker.estimateRCFModelSize(any())).thenReturn(352_064L);
        partitioner = new ModelPartitioner(
            AnomalyDetectorSettings.NUM_SAMPLES_PER_TREE,
            AnomalyDetectorSettings.NUM_TREES,
            nodeFilter,
            memoryTracker
        );
    }

    /**
     * The detector has a rcf model of size 352064 bytes.  Since the desired model
     * size is 200_000, we can have two models;
     *
     * @throws IOException when failing to generate random query for detector config
     */
    public void testGetPartitionedForestSizes() throws IOException {
        DiscoveryNode[] nodes = new DiscoveryNode[2];
        nodes[0] = mock(DiscoveryNode.class);
        nodes[1] = mock(DiscoveryNode.class);
        when(nodeFilter.getEligibleDataNodes()).thenReturn(nodes);
        Feature feature1 = TestHelpers.randomFeature(true);
        Feature feature2 = TestHelpers.randomFeature(false);

        AnomalyDetector detector = new AnomalyDetector(
            randomAlphaOfLength(10),
            randomLong(),
            randomAlphaOfLength(20),
            randomAlphaOfLength(30),
            randomAlphaOfLength(5),
            ImmutableList.of(randomAlphaOfLength(20)),
            // 1 feature enabled
            ImmutableList.of(feature1, feature2),
            TestHelpers.randomQuery(),
            new IntervalTimeConfiguration(5, ChronoUnit.MINUTES),
            TestHelpers.randomIntervalTimeConfiguration(),
            4, // shingle size 4
            null,
            randomInt(),
            null,
            null,
            null
        );

        Entry<Integer, Integer> partition = partitioner.getPartitionedForestSizes(detector);

        // 2 partitions
        assertEquals(2, partition.getKey().intValue());
        // each partition has AnomalyDetectorSettings.NUM_TREES / 2 trees
        assertEquals(AnomalyDetectorSettings.NUM_TREES / 2, partition.getValue().intValue());
    }
}
