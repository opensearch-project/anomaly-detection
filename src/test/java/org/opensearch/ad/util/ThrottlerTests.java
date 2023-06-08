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

package org.opensearch.ad.util;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.time.Clock;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.test.OpenSearchTestCase;

public class ThrottlerTests extends OpenSearchTestCase {
    private Throttler throttler;

    @Before
    public void setup() {
        Clock clock = mock(Clock.class);
        this.throttler = new Throttler(clock);
    }

    @Test
    public void testGetFilteredQuery() {
        AnomalyDetector detector = mock(AnomalyDetector.class);
        when(detector.getId()).thenReturn("test detector Id");
        SearchRequest dummySearchRequest = new SearchRequest();
        throttler.insertFilteredQuery(detector.getId(), dummySearchRequest);
        // case 1: key exists
        assertTrue(throttler.getFilteredQuery(detector.getId()).isPresent());
        // case 2: key doesn't exist
        assertFalse(throttler.getFilteredQuery("different test detector Id").isPresent());
    }

    @Test
    public void testInsertFilteredQuery() {
        AnomalyDetector detector = mock(AnomalyDetector.class);
        when(detector.getId()).thenReturn("test detector Id");
        SearchRequest dummySearchRequest = new SearchRequest();
        // first time: key doesn't exist
        assertTrue(throttler.insertFilteredQuery(detector.getId(), dummySearchRequest));
        // second time: key exists
        assertFalse(throttler.insertFilteredQuery(detector.getId(), dummySearchRequest));
    }

    @Test
    public void testClearFilteredQuery() {
        AnomalyDetector detector = mock(AnomalyDetector.class);
        when(detector.getId()).thenReturn("test detector Id");
        SearchRequest dummySearchRequest = new SearchRequest();
        assertTrue(throttler.insertFilteredQuery(detector.getId(), dummySearchRequest));
        throttler.clearFilteredQuery(detector.getId());
        assertTrue(throttler.insertFilteredQuery(detector.getId(), dummySearchRequest));
    }

}
