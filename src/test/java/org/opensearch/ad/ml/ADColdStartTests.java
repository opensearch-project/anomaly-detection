/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.ml;

import java.io.IOException;
import java.util.ArrayList;

import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;

import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

public class ADColdStartTests extends OpenSearchTestCase {
    private int baseDimensions = 1;
    private int shingleSize = 8;
    private int dimensions;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dimensions = baseDimensions * shingleSize;
    }

    /**
     * Test if no explicit rule is provided, we apply 20% rule.
     * @throws IOException when failing to constructor detector
     */
    public void testEmptyRule() throws IOException {
        AnomalyDetector detector = TestHelpers.AnomalyDetectorBuilder.newInstance(1).setRules(new ArrayList<>()).build();
        ThresholdedRandomCutForest.Builder builder = new ThresholdedRandomCutForest.Builder<>()
            .dimensions(dimensions)
            .shingleSize(shingleSize);
        ADColdStart.applyRule(builder, detector);

        ThresholdedRandomCutForest forest = builder.build();
        double[] ignore = forest.getPredictorCorrector().getIgnoreNearExpected();

        // Specify a small delta for floating-point comparison
        double delta = 1e-6;

        assertArrayEquals("The double arrays are not equal", new double[] { 0, 0, 0.2, 0.2 }, ignore, delta);
    }

    public void testNullRule() throws IOException {
        AnomalyDetector detector = TestHelpers.AnomalyDetectorBuilder.newInstance(1).setRules(null).build();
        ThresholdedRandomCutForest.Builder builder = new ThresholdedRandomCutForest.Builder<>()
            .dimensions(dimensions)
            .shingleSize(shingleSize);
        ADColdStart.applyRule(builder, detector);

        ThresholdedRandomCutForest forest = builder.build();
        double[] ignore = forest.getPredictorCorrector().getIgnoreNearExpected();

        // Specify a small delta for floating-point comparison
        double delta = 1e-6;

        assertArrayEquals("The double arrays are not equal", new double[] { 0, 0, 0.2, 0.2 }, ignore, delta);
    }
}
