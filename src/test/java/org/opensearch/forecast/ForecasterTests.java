/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import java.io.IOException;
import java.util.Arrays;

import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.common.exception.ValidationException;

import com.google.common.collect.ImmutableList;

public class ForecasterTests extends OpenSearchTestCase {

    public void testInvalidSeasonality() throws IOException {
        assertThrows(
            ValidationException.class,
            () -> TestHelpers.ForecasterBuilder
                .newInstance()
                .setConfigId("123")
                .setTimeField("timestamp")
                .setIndices(ImmutableList.of("test-index"))
                .setFeatureAttributes(Arrays.asList())
                .setCategoryFields(Arrays.asList("a"))
                .setNullImputationOption()
                // should trigger ValidationException, as we need at least 8 in forecasting
                .setSeasonality(7)
                .build()
        );
    }
}
