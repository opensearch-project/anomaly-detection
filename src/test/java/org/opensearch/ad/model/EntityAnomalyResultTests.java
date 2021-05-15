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

package org.opensearch.ad.model;

import static java.util.Arrays.asList;
import static org.opensearch.ad.TestHelpers.randomMutlEntityAnomalyDetectResult;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.opensearch.ad.stats.ADStatsResponse;
import org.opensearch.test.OpenSearchTestCase;

public class EntityAnomalyResultTests extends OpenSearchTestCase {

    @Test
    public void testGetAnomalyResults() {
        AnomalyResult anomalyResult1 = randomMutlEntityAnomalyDetectResult(0.25, 0.25, "error");
        AnomalyResult anomalyResult2 = randomMutlEntityAnomalyDetectResult(0.5, 0.5, "error");
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
        AnomalyResult anomalyResult1 = randomMutlEntityAnomalyDetectResult(0.25, 0.25, "error");
        AnomalyResult anomalyResult2 = randomMutlEntityAnomalyDetectResult(0.5, 0.5, "error");

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
        AnomalyResult anomalyResult = randomMutlEntityAnomalyDetectResult(0.25, 0.25, "error");

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
        AnomalyResult anomalyResult = randomMutlEntityAnomalyDetectResult(0.25, 0.25, "error");

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
        AnomalyResult anomalyResult = randomMutlEntityAnomalyDetectResult(0.25, 0.25, "error");

        EntityAnomalyResult entityAnomalyResult = new EntityAnomalyResult(new ArrayList<AnomalyResult>() {
            {
                add(anomalyResult);
            }
        });

        entityAnomalyResult.merge(adStatsResponse);

        assertEquals(asList(anomalyResult), entityAnomalyResult.getAnomalyResults());
    }

}
