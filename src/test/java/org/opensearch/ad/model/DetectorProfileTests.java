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

import java.io.IOException;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import com.google.common.collect.ImmutableMap;

public class DetectorProfileTests extends OpenSearchTestCase {

    public void testParseDetectorProfile() throws IOException {
        String detectorId = randomAlphaOfLength(10);
        String[] runningEntities = new String[] { randomAlphaOfLength(5) };
        DetectorProfile detectorProfile = new DetectorProfile.Builder()
            .state(DetectorState.INIT)
            .error(randomAlphaOfLength(5))
            .modelProfile(
                new ModelProfileOnNode[] {
                    new ModelProfileOnNode(
                        randomAlphaOfLength(10),
                        new ModelProfile(
                            randomAlphaOfLength(5),
                            Entity.createSingleAttributeEntity(randomAlphaOfLength(5), randomAlphaOfLength(5)),
                            randomLong()
                        )
                    ) }
            )
            .shingleSize(randomInt())
            .coordinatingNode(randomAlphaOfLength(10))
            .totalEntities(randomLong())
            .activeEntities(randomLong())
            .adTaskProfiles(
                ImmutableMap
                    .of(
                        randomAlphaOfLength(5),
                        new ADTaskProfile(randomAlphaOfLength(5), randomInt(), randomInt(), randomInt(), runningEntities)
                    )
            )
            .build();

        BytesStreamOutput output = new BytesStreamOutput();
        detectorProfile.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        // StreamInput input = output.bytes().streamInput();
        DetectorProfile parsedDetectorProfile = new DetectorProfile(input);
        assertEquals("Detector profile serialization doesn't work", detectorProfile, parsedDetectorProfile);
    }
}
