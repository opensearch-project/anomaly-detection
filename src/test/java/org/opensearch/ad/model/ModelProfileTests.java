package org.opensearch.ad.model;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.opensearch.ad.AbstractADTest;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;

import test.org.opensearch.ad.util.JsonDeserializer;

public class ModelProfileTests extends AbstractADTest {

    public void testToXContent() throws IOException {
        ModelProfile profile1 = new ModelProfile(
            randomAlphaOfLength(5),
            Entity.createSingleAttributeEntity(randomAlphaOfLength(5), randomAlphaOfLength(5)),
            0
        );
        XContentBuilder builder = jsonBuilder();
        builder.startObject();
        profile1.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String json = Strings.toString(builder);
        assertTrue(JsonDeserializer.hasChildNode(json, CommonName.ENTITY_KEY));
        assertFalse(JsonDeserializer.hasChildNode(json, CommonName.MODEL_SIZE_IN_BYTES));

        ModelProfile profile2 = new ModelProfile(randomAlphaOfLength(5), null, 1);

        builder = jsonBuilder();
        builder.startObject();
        profile2.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        json = Strings.toString(builder);

        assertFalse(JsonDeserializer.hasChildNode(json, CommonName.ENTITY_KEY));
        assertTrue(JsonDeserializer.hasChildNode(json, CommonName.MODEL_SIZE_IN_BYTES));

    }
}
