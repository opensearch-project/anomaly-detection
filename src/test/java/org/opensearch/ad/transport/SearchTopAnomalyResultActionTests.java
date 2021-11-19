/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import org.opensearch.ad.HistoricalAnalysisIntegTestCase;

public class SearchTopAnomalyResultActionTests extends HistoricalAnalysisIntegTestCase {

    public void testInstanceAndNameValid() {
        assertNotNull(SearchTopAnomalyResultAction.INSTANCE.name());
        assertEquals(SearchTopAnomalyResultAction.INSTANCE.name(), SearchTopAnomalyResultAction.NAME);
    }

}
