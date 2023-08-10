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
import java.util.Map;

import org.opensearch.ad.TestHelpers;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

public class DetectorProfileTests extends OpenSearchTestCase {

    private DetectorProfile createRandomDetectorProfile() {
        return new DetectorProfile.Builder()
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
            .totalSizeInBytes(-1)
            .totalEntities(randomLong())
            .activeEntities(randomLong())
            .adTaskProfile(
                new ADTaskProfile(
                    randomAlphaOfLength(5),
                    randomInt(),
                    randomLong(),
                    randomBoolean(),
                    randomInt(),
                    randomLong(),
                    randomAlphaOfLength(5)
                )
            )
            .build();
    }

    public void testParseDetectorProfile() throws IOException {
        DetectorProfile detectorProfile = createRandomDetectorProfile();
        BytesStreamOutput output = new BytesStreamOutput();
        detectorProfile.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        DetectorProfile parsedDetectorProfile = new DetectorProfile(input);
        assertEquals("Detector profile serialization doesn't work", detectorProfile, parsedDetectorProfile);
    }

    public void testMergeDetectorProfile() {
        DetectorProfile detectorProfileOne = createRandomDetectorProfile();
        DetectorProfile detectorProfileTwo = createRandomDetectorProfile();
        String errorPreMerge = detectorProfileOne.getError();
        detectorProfileOne.merge(detectorProfileTwo);
        assertTrue(detectorProfileOne.toString().contains(detectorProfileTwo.getError()));
        assertFalse(detectorProfileOne.toString().contains(errorPreMerge));
        assertTrue(detectorProfileOne.toString().contains(detectorProfileTwo.getCoordinatingNode()));
    }

    public void testDetectorProfileToXContent() throws IOException {
        DetectorProfile detectorProfile = createRandomDetectorProfile();
        String detectorProfileString = TestHelpers.xContentBuilderToString(detectorProfile.toXContent(TestHelpers.builder()));
        XContentParser parser = TestHelpers.parser(detectorProfileString);
        Map<String, Object> parsedMap = parser.map();
        assertEquals(detectorProfile.getCoordinatingNode(), parsedMap.get("coordinating_node"));
        assertEquals(detectorProfile.getState().toString(), parsedMap.get("state"));
        assertTrue(parsedMap.get("models").toString().contains(detectorProfile.getModelProfile()[0].getModelId()));
    }

    public void testDetectorProfileName() throws IllegalArgumentException {
        assertEquals("ad_task", DetectorProfileName.getName(CommonName.AD_TASK).getName());
        assertEquals("state", DetectorProfileName.getName(CommonName.STATE).getName());
        assertEquals("error", DetectorProfileName.getName(CommonName.ERROR).getName());
        assertEquals("coordinating_node", DetectorProfileName.getName(CommonName.COORDINATING_NODE).getName());
        assertEquals("shingle_size", DetectorProfileName.getName(CommonName.SHINGLE_SIZE).getName());
        assertEquals("total_size_in_bytes", DetectorProfileName.getName(CommonName.TOTAL_SIZE_IN_BYTES).getName());
        assertEquals("models", DetectorProfileName.getName(CommonName.MODELS).getName());
        assertEquals("init_progress", DetectorProfileName.getName(CommonName.INIT_PROGRESS).getName());
        assertEquals("total_entities", DetectorProfileName.getName(CommonName.TOTAL_ENTITIES).getName());
        assertEquals("active_entities", DetectorProfileName.getName(CommonName.ACTIVE_ENTITIES).getName());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> DetectorProfileName.getName("abc"));
        assertEquals(exception.getMessage(), CommonErrorMessages.UNSUPPORTED_PROFILE_TYPE);
    }

    public void testDetectorProfileSet() throws IllegalArgumentException {
        DetectorProfile detectorProfileOne = createRandomDetectorProfile();
        detectorProfileOne.setShingleSize(20);
        assertEquals(20, detectorProfileOne.getShingleSize());
        detectorProfileOne.setActiveEntities(10L);
        assertEquals(10L, (long) detectorProfileOne.getActiveEntities());
        detectorProfileOne.setModelCount(10L);
        assertEquals(10L, (long) detectorProfileOne.getActiveEntities());
    }
}
