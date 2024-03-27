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

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.ml.SingleStreamModelIdMapper;

public class SingleStreamModelIdMapperTests extends OpenSearchTestCase {
    public void testGetThresholdModelIdFromRCFModelId() {
        assertEquals(
            "Y62IGnwBFHAk-4HQQeoo_model_threshold",
            SingleStreamModelIdMapper.getThresholdModelIdFromRCFModelId("Y62IGnwBFHAk-4HQQeoo_model_rcf_1")
        );
    }

}
