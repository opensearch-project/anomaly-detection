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

package org.opensearch.ad.model;

import static java.util.Arrays.asList;
import static org.opensearch.ad.TestHelpers.randomHCADAnomalyDetectResult;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.opensearch.ad.stats.ADStatsResponse;
import org.opensearch.test.OpenSearchTestCase;

public class EntityAnomalyResultTests extends OpenSearchTestCase {

    @Test
    public void testGetAnomalyResults() {
        AnomalyResult anomalyResult1 = randomHCADAnomalyDetectResult(0.25, 0.25, "error");
        AnomalyResult anomalyResult2 = randomHCADAnomalyDetectResult(0.5, 0.5, "error");
        List<AnomalyResult> anomalyResults = new ArrayList<AnomalyResult>() {
            {
                add(anomalyResult1);
                add(anomalyResult2);
            }
        };
        EntityAnomalyResult entityAnomalyResult = new EntityAnomalyResult(anomalyResults);

        assertEquals(anomalyResults, entityAnomalyResult.getAnomalyResults());
    }

    @Test
    public void testMerge() {
        AnomalyResult anomalyResult1 = randomHCADAnomalyDetectResult(0.25, 0.25, "error");
        AnomalyResult anomalyResult2 = randomHCADAnomalyDetectResult(0.5, 0.5, "error");

        EntityAnomalyResult entityAnomalyResult1 = new EntityAnomalyResult(new ArrayList<AnomalyResult>() {
            {
                add(anomalyResult1);
            }
        });
        EntityAnomalyResult entityAnomalyResult2 = new EntityAnomalyResult(new ArrayList<AnomalyResult>() {
            {
                add(anomalyResult2);
            }
        });
        entityAnomalyResult2.merge(entityAnomalyResult1);

        assertEquals(asList(anomalyResult2, anomalyResult1), entityAnomalyResult2.getAnomalyResults());
    }

    @Test
    public void testMerge_null() {
        AnomalyResult anomalyResult = randomHCADAnomalyDetectResult(0.25, 0.25, "error");

        EntityAnomalyResult entityAnomalyResult = new EntityAnomalyResult(new ArrayList<AnomalyResult>() {
            {
                add(anomalyResult);
            }
        });

        entityAnomalyResult.merge(null);

        assertEquals(asList(anomalyResult), entityAnomalyResult.getAnomalyResults());
    }

    @Test
    public void testMerge_self() {
        AnomalyResult anomalyResult = randomHCADAnomalyDetectResult(0.25, 0.25, "error");

        EntityAnomalyResult entityAnomalyResult = new EntityAnomalyResult(new ArrayList<AnomalyResult>() {
            {
                add(anomalyResult);
            }
        });

        entityAnomalyResult.merge(entityAnomalyResult);

        assertEquals(asList(anomalyResult), entityAnomalyResult.getAnomalyResults());
    }

    @Test
    public void testMerge_otherClass() {
        ADStatsResponse adStatsResponse = new ADStatsResponse();
        AnomalyResult anomalyResult = randomHCADAnomalyDetectResult(0.25, 0.25, "error");

        EntityAnomalyResult entityAnomalyResult = new EntityAnomalyResult(new ArrayList<AnomalyResult>() {
            {
                add(anomalyResult);
            }
        });

        entityAnomalyResult.merge(adStatsResponse);

        assertEquals(asList(anomalyResult), entityAnomalyResult.getAnomalyResults());
    }

}
