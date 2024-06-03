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

import org.opensearch.ad.constant.ADCommonMessages;
import org.opensearch.ad.constant.ADCommonName;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.constant.CommonName;
import org.opensearch.timeseries.model.ConfigProfile;
import org.opensearch.timeseries.model.ConfigState;
import org.opensearch.timeseries.model.Entity;
import org.opensearch.timeseries.model.ModelProfile;
import org.opensearch.timeseries.model.ModelProfileOnNode;
import org.opensearch.timeseries.model.ProfileName;

public class DetectorProfileTests extends OpenSearchTestCase {

    private ConfigProfile createRandomDetectorProfile() {
        return new DetectorProfile.Builder()
            .state(ConfigState.INIT)
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
            .coordinatingNode(randomAlphaOfLength(10))
            .totalSizeInBytes(-1)
            .totalEntities(randomLong())
            .activeEntities(randomLong())
            .taskProfile(
                new ADTaskProfile(randomAlphaOfLength(5), randomLong(), randomBoolean(), randomInt(), randomLong(), randomAlphaOfLength(5))
            )
            .build();
    }

    public void testParseDetectorProfile() throws IOException {
        ConfigProfile detectorProfile = createRandomDetectorProfile();
        BytesStreamOutput output = new BytesStreamOutput();
        detectorProfile.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ConfigProfile parsedDetectorProfile = new DetectorProfile(input);
        assertEquals("Detector profile serialization doesn't work", detectorProfile, parsedDetectorProfile);
    }

    public void testMergeDetectorProfile() {
        ConfigProfile detectorProfileOne = createRandomDetectorProfile();
        ConfigProfile detectorProfileTwo = createRandomDetectorProfile();
        String errorPreMerge = detectorProfileOne.getError();
        detectorProfileOne.merge(detectorProfileTwo);
        assertTrue(detectorProfileOne.toString().contains(detectorProfileTwo.getError()));
        assertFalse(detectorProfileOne.toString().contains(errorPreMerge));
        assertTrue(detectorProfileOne.toString().contains(detectorProfileTwo.getCoordinatingNode()));
    }

    public void testDetectorProfileToXContent() throws IOException {
        ConfigProfile detectorProfile = createRandomDetectorProfile();
        String detectorProfileString = TestHelpers.xContentBuilderToString(detectorProfile.toXContent(TestHelpers.builder()));
        XContentParser parser = TestHelpers.parser(detectorProfileString);
        Map<String, Object> parsedMap = parser.map();
        assertEquals(detectorProfile.getCoordinatingNode(), parsedMap.get("coordinating_node"));
        assertEquals(detectorProfile.getState().toString(), parsedMap.get("state"));
        assertTrue(parsedMap.get("models").toString().contains(detectorProfile.getModelProfile()[0].getModelId()));
    }

    public void testDetectorProfileName() throws IllegalArgumentException {
        assertEquals("ad_task", ProfileName.getName(ADCommonName.AD_TASK).getName());
        assertEquals("state", ProfileName.getName(CommonName.STATE).getName());
        assertEquals("error", ProfileName.getName(CommonName.ERROR).getName());
        assertEquals("coordinating_node", ProfileName.getName(CommonName.COORDINATING_NODE).getName());
        assertEquals("total_size_in_bytes", ProfileName.getName(CommonName.TOTAL_SIZE_IN_BYTES).getName());
        assertEquals("models", ProfileName.getName(CommonName.MODELS).getName());
        assertEquals("init_progress", ProfileName.getName(CommonName.INIT_PROGRESS).getName());
        assertEquals("total_entities", ProfileName.getName(CommonName.TOTAL_ENTITIES).getName());
        assertEquals("active_entities", ProfileName.getName(CommonName.ACTIVE_ENTITIES).getName());
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> ProfileName.getName("abc"));
        assertEquals(exception.getMessage(), ADCommonMessages.UNSUPPORTED_PROFILE_TYPE);
    }

    public void testDetectorProfileSet() throws IllegalArgumentException {
        ConfigProfile detectorProfileOne = createRandomDetectorProfile();
        detectorProfileOne.setActiveEntities(10L);
        assertEquals(10L, (long) detectorProfileOne.getActiveEntities());
        detectorProfileOne.setModelCount(10L);
        assertEquals(10L, (long) detectorProfileOne.getActiveEntities());
    }
}
