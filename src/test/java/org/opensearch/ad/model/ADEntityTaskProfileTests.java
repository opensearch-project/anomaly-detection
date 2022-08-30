/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.model;

import java.util.Collection;
import java.util.TreeMap;

import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

public class ADEntityTaskProfileTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, AnomalyDetectorPlugin.class);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return getInstanceFromNode(NamedWriteableRegistry.class);
    }

    private ADEntityTaskProfile createADEntityTaskProfile() {
        Entity entity = createEntityAndAttributes();
        return new ADEntityTaskProfile(1, 23L, false, 1, 2L, "1234", entity, "4321", ADTaskType.HISTORICAL_HC_ENTITY.name());
    }

    private Entity createEntityAndAttributes() {
        TreeMap<String, String> attributes = new TreeMap<>();
        String name1 = "host";
        String val1 = "server_2";
        String name2 = "service";
        String val2 = "app_4";
        attributes.put(name1, val1);
        attributes.put(name2, val2);
        return Entity.createEntityFromOrderedMap(attributes);
    }

    /*public void testADEntityTaskProfileSerialization() throws IOException {
        ADEntityTaskProfile entityTask = createADEntityTaskProfile();
        BytesStreamOutput output = new BytesStreamOutput();
        entityTask.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADEntityTaskProfile parsedEntityTask = new ADEntityTaskProfile(input);
        assertEquals(entityTask, parsedEntityTask);
    }*/

    /*public void testParseADEntityTaskProfile() throws IOException {
        ADEntityTaskProfile entityTask = createADEntityTaskProfile();
        String adEntityTaskProfileString = TestHelpers
            .xContentBuilderToString(entityTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ADEntityTaskProfile parsedEntityTask = ADEntityTaskProfile.parse(TestHelpers.parser(adEntityTaskProfileString));
        assertEquals(entityTask, parsedEntityTask);
    }*/

    /*public void testParseADEntityTaskProfileWithNullEntity() throws IOException {
        ADEntityTaskProfile entityTask = new ADEntityTaskProfile(
            1,
            23L,
            false,
            1,
            2L,
            "1234",
            null,
            "4321",
            ADTaskType.HISTORICAL_HC_ENTITY.name()
        );
        assertEquals(Integer.valueOf(1), entityTask.getShingleSize());
        assertEquals(23L, (long) entityTask.getRcfTotalUpdates());
        assertNull(entityTask.getEntity());
        String adEntityTaskProfileString = TestHelpers
            .xContentBuilderToString(entityTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ADEntityTaskProfile parsedEntityTask = ADEntityTaskProfile.parse(TestHelpers.parser(adEntityTaskProfileString));
        assertEquals(entityTask, parsedEntityTask);
    }*/

    /*public void testADEntityTaskProfileEqual() {
        ADEntityTaskProfile entityTaskOne = createADEntityTaskProfile();
        ADEntityTaskProfile entityTaskTwo = createADEntityTaskProfile();
        ADEntityTaskProfile entityTaskThree = new ADEntityTaskProfile(
            null,
            null,
            false,
            1,
            null,
            "1234",
            null,
            "4321",
            ADTaskType.HISTORICAL_HC_ENTITY.name()
        );
        assertTrue(entityTaskOne.equals(entityTaskTwo));
        assertFalse(entityTaskOne.equals(entityTaskThree));
    }*/

    /*public void testParseADEntityTaskProfileWithMultipleNullFields() throws IOException {
        Entity entity = createEntityAndAttributes();
        ADEntityTaskProfile entityTask = new ADEntityTaskProfile(
            null,
            null,
            false,
            1,
            null,
            "1234",
            entity,
            "4321",
            ADTaskType.HISTORICAL_HC_ENTITY.name()
        );
        String adEntityTaskProfileString = TestHelpers
            .xContentBuilderToString(entityTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ADEntityTaskProfile parsedEntityTask = ADEntityTaskProfile.parse(TestHelpers.parser(adEntityTaskProfileString));
        assertEquals(entityTask, parsedEntityTask);
    }*/
}
