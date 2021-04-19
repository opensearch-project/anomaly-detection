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

package com.amazon.opendistroforelasticsearch.ad.util;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

import java.time.Clock;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.test.OpenSearchTestCase;

import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;

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
        when(detector.getDetectorId()).thenReturn("test detector Id");
        SearchRequest dummySearchRequest = new SearchRequest();
        throttler.insertFilteredQuery(detector.getDetectorId(), dummySearchRequest);
        // case 1: key exists
        assertTrue(throttler.getFilteredQuery(detector.getDetectorId()).isPresent());
        // case 2: key doesn't exist
        assertFalse(throttler.getFilteredQuery("different test detector Id").isPresent());
    }

    @Test
    public void testInsertFilteredQuery() {
        AnomalyDetector detector = mock(AnomalyDetector.class);
        when(detector.getDetectorId()).thenReturn("test detector Id");
        SearchRequest dummySearchRequest = new SearchRequest();
        // first time: key doesn't exist
        assertTrue(throttler.insertFilteredQuery(detector.getDetectorId(), dummySearchRequest));
        // second time: key exists
        assertFalse(throttler.insertFilteredQuery(detector.getDetectorId(), dummySearchRequest));
    }

    @Test
    public void testClearFilteredQuery() {
        AnomalyDetector detector = mock(AnomalyDetector.class);
        when(detector.getDetectorId()).thenReturn("test detector Id");
        SearchRequest dummySearchRequest = new SearchRequest();
        assertTrue(throttler.insertFilteredQuery(detector.getDetectorId(), dummySearchRequest));
        throttler.clearFilteredQuery(detector.getDetectorId());
        assertTrue(throttler.insertFilteredQuery(detector.getDetectorId(), dummySearchRequest));
    }

}
