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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.google.gson.Gson;

public class KllFloatsSketchSerDeTests {

    private Gson gson;

    private HybridThresholdingModel hybridModel;

    @Before
    public void setup() {
        gson = new Gson();

        hybridModel = new HybridThresholdingModel(/*minPvalueThreshold*/ 0.95,
            /*maxRankError*/ 1e-4,
            /*maxScore*/ 4,
            /*numLogNormalQuantiles*/ 10000,
            /*downsampleNumSamples*/ 100_000,
            /*downsampleMaxNumObservations*/ 200_000L
        );
    }

    @Test
    public void serialize_deserialize_returnOriginalModel() {
        hybridModel.train(new Random().doubles(10_000L, 0.1, 3.9).toArray());

        String json = gson.toJson(hybridModel);
        HybridThresholdingModel deserialized = gson.fromJson(json, HybridThresholdingModel.class);

        double delta = 1e-6;
        assertEquals(hybridModel.getMinPvalueThreshold(), deserialized.getMinPvalueThreshold(), delta);
        assertEquals(hybridModel.getMaxRankError(), deserialized.getMaxRankError(), delta);
        assertEquals(hybridModel.getMaxScore(), deserialized.getMaxScore(), delta);
        assertEquals(hybridModel.getNumLogNormalQuantiles(), deserialized.getNumLogNormalQuantiles());
        for (double score : new Random().doubles(1000L, 0.1, 3.9).toArray()) {
            assertEquals(hybridModel.grade(score), deserialized.grade(score), delta);
            assertEquals(hybridModel.confidence(), deserialized.confidence(), delta);
            hybridModel.update(score);
            deserialized.update(score);
        }
    }
}
