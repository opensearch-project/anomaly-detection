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
import java.util.Collection;
import java.util.TreeMap;

import org.opensearch.ad.AnomalyDetectorPlugin;
import org.opensearch.ad.TestHelpers;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.xcontent.ToXContent;
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

    public void testADEntityTaskProfileSerialization() throws IOException {
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
        BytesStreamOutput output = new BytesStreamOutput();
        entityTask.writeTo(output);
        NamedWriteableAwareStreamInput input = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), writableRegistry());
        ADEntityTaskProfile parsedEntityTask = new ADEntityTaskProfile(input);
        assertEquals(entityTask, parsedEntityTask);
    }

    public void testParseADEntityTaskProfile() throws IOException {
        TreeMap<String, String> attributes = new TreeMap<>();
        String name1 = "host";
        String val1 = "server_2";
        String name2 = "service";
        String val2 = "app_4";
        attributes.put(name1, val1);
        attributes.put(name2, val2);
        Entity entity = Entity.createEntityFromOrderedMap(attributes);
        ADEntityTaskProfile entityTask = new ADEntityTaskProfile(
            1,
            23L,
            false,
            1,
            2L,
            "1234",
            entity,
            "4321",
            ADTaskType.HISTORICAL_HC_ENTITY.name()
        );
        String adEntityTaskProfileString = TestHelpers
            .xContentBuilderToString(entityTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ADEntityTaskProfile parsedEntityTask = ADEntityTaskProfile.parse(TestHelpers.parser(adEntityTaskProfileString));
        assertEquals(entityTask, parsedEntityTask);
    }

    public void testParseADEntityTaskProfileWithNullEntity() throws IOException {
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
        String adEntityTaskProfileString = TestHelpers
            .xContentBuilderToString(entityTask.toXContent(TestHelpers.builder(), ToXContent.EMPTY_PARAMS));
        ADEntityTaskProfile parsedEntityTask = ADEntityTaskProfile.parse(TestHelpers.parser(adEntityTaskProfileString));
        assertEquals(entityTask, parsedEntityTask);
    }
}
