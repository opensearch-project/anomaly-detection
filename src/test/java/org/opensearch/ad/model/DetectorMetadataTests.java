/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.opensearch.test.OpenSearchTestCase;

public class DetectorMetadataTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        String detectorId = "detector-123";
        String detectorName = "Test Detector";
        List<String> indices = Arrays.asList("index-1", "index-2");

        DetectorMetadata metadata = new DetectorMetadata(detectorId, detectorName, indices);

        assertEquals(detectorId, metadata.getDetectorId());
        assertEquals(detectorName, metadata.getDetectorName());
        assertEquals(indices, metadata.getIndices());
    }

    public void testWithEmptyIndices() {
        String detectorId = "detector-456";
        String detectorName = "Empty Index Detector";
        List<String> indices = Collections.emptyList();

        DetectorMetadata metadata = new DetectorMetadata(detectorId, detectorName, indices);

        assertEquals(detectorId, metadata.getDetectorId());
        assertEquals(detectorName, metadata.getDetectorName());
        assertTrue(metadata.getIndices().isEmpty());
    }

    public void testWithSingleIndex() {
        String detectorId = "detector-789";
        String detectorName = "Single Index Detector";
        List<String> indices = Collections.singletonList("my-index");

        DetectorMetadata metadata = new DetectorMetadata(detectorId, detectorName, indices);

        assertEquals(detectorId, metadata.getDetectorId());
        assertEquals(detectorName, metadata.getDetectorName());
        assertEquals(1, metadata.getIndices().size());
        assertEquals("my-index", metadata.getIndices().get(0));
    }

    public void testWithMultipleIndices() {
        String detectorId = "detector-multi";
        String detectorName = "Multi Index Detector";
        List<String> indices = Arrays.asList("logs-*", "metrics-*", "traces-*");

        DetectorMetadata metadata = new DetectorMetadata(detectorId, detectorName, indices);

        assertEquals(detectorId, metadata.getDetectorId());
        assertEquals(detectorName, metadata.getDetectorName());
        assertEquals(3, metadata.getIndices().size());
        assertTrue(metadata.getIndices().contains("logs-*"));
        assertTrue(metadata.getIndices().contains("metrics-*"));
        assertTrue(metadata.getIndices().contains("traces-*"));
    }

    public void testWithNullValues() {
        // Test that metadata can be created with null values (no validation in constructor)
        DetectorMetadata metadata = new DetectorMetadata(null, null, null);

        assertNull(metadata.getDetectorId());
        assertNull(metadata.getDetectorName());
        assertNull(metadata.getIndices());
    }

    public void testWithSpecialCharacters() {
        String detectorId = "detector-特殊-字符";
        String detectorName = "Detector with émojis 🚀";
        List<String> indices = Arrays.asList("index-with-dashes", "index_with_underscores", "index.with.dots");

        DetectorMetadata metadata = new DetectorMetadata(detectorId, detectorName, indices);

        assertEquals(detectorId, metadata.getDetectorId());
        assertEquals(detectorName, metadata.getDetectorName());
        assertEquals(3, metadata.getIndices().size());
    }
}
