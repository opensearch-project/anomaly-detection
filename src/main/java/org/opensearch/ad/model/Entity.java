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

/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.opensearch.ad.model;

import static org.opensearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.opensearch.ad.annotation.Generated;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.common.Numbers;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentParser.Token;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.query.TermQueryBuilder;

import com.google.common.base.Joiner;
import com.google.common.base.Objects;

/**
 * Categorical field name and its value
 *
 */
public class Entity implements ToXContentObject, Writeable {
    private static final Logger LOG = LogManager.getLogger(Entity.class);

    private static final long RANDOM_SEED = 42;
    private static final String MODEL_ID_INFIX = "_entity_";

    public static final String ATTRIBUTE_NAME_FIELD = "name";
    public static final String ATTRIBUTE_VALUE_FIELD = "value";

    // model id
    private SetOnce<String> modelId = new SetOnce<>();
    // a map from attribute name like "host" to its value like "server_1"
    // Use SortedMap so that the attributes are ordered and we can derive the unique
    // string representation used in the hash ring.
    private final SortedMap<String, String> attributes;

    /**
     * Create an entity that has multiple attributes
     * @param detectorId Detector Id
     * @param attrs what we parsed from query output as a map of attribute and its values.
     * @return the created entity
     */
    public static Entity createEntityByReordering(String detectorId, Map<String, Object> attrs) {
        SortedMap<String, String> sortedMap = new TreeMap<>();
        for (Map.Entry<String, Object> categoryValuePair : attrs.entrySet()) {
            sortedMap.put(categoryValuePair.getKey(), categoryValuePair.getValue().toString());
        }
        return new Entity(sortedMap);
    }

    /**
     * Create an entity that has only one attribute
     * @param detectorId Detector Id
     * @param attributeName the attribute's name
     * @param attributeVal the attribute's value
     * @return the created entity
     */
    public static Entity createSingleAttributeEntity(String detectorId, String attributeName, String attributeVal) {
        SortedMap<String, String> sortedMap = new TreeMap<>();
        sortedMap.put(attributeName, attributeVal);
        return new Entity(sortedMap);
    }

    /**
     * Create an entity from ordered attributes based on attribute names
     * @param detectorId Detector Id
     * @param attrs attribute map
     * @return the created entity
     */
    public static Entity createEntityFromOrderedMap(String detectorId, SortedMap<String, String> attrs) {
        return new Entity(attrs);
    }

    private Entity(SortedMap<String, String> orderedAttrs) {
        this.attributes = orderedAttrs;
    }

    public Entity(StreamInput input) throws IOException {
        this.attributes = new TreeMap<>(input.readMap(StreamInput::readString, StreamInput::readString));
    }

    /**
     * Formatter when serializing to json.  Used in cases when saving anomaly result for HCAD.
     * The order is Alphabetical sorting (the one used by JDK to compare Strings).
     * Example:
     *  z0
     *  z11
     *  z2
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray();
        for (Map.Entry<String, String> attr : attributes.entrySet()) {
            builder.startObject().field(ATTRIBUTE_NAME_FIELD, attr.getKey()).field(ATTRIBUTE_VALUE_FIELD, attr.getValue()).endObject();
        }
        builder.endArray();
        return builder;
    }

    /**
     * Return a map representing the entity, used in the stats API.
     *
     * A stats API broadcasts requests to all nodes and renders node responses using toXContent.
     *
     * For the local node, the stats API's calls toXContent on the node response directly.
     * For remote node, the coordinating node gets a serialized content from
     * ADStatsNodeResponse.writeTo, deserializes the content, and renders the result using toXContent.
     * Since ADStatsNodeResponse.writeTo uses StreamOutput::writeGenericValue, we can only use
     *  a List&lt;Map&lt;String, String&gt;&gt; instead of the Entity object itself as
     *  StreamOutput::writeGenericValue only recognizes built-in types.
     *
     * This functions returns a map consistent with what toXContent returns.
     *
     * @return a map representing the entity
     */
    public List<Map<String, String>> toStat() {
        List<Map<String, String>> res = new ArrayList<>(attributes.size() * 2);
        for (Map.Entry<String, String> attr : attributes.entrySet()) {
            Map<String, String> elements = new TreeMap<>();
            elements.put(ATTRIBUTE_NAME_FIELD, attr.getKey());
            elements.put(ATTRIBUTE_VALUE_FIELD, attr.getValue());
            res.add(elements);
        }
        return res;
    }

    public static Entity parse(XContentParser parser) throws IOException {
        SortedMap<String, String> entities = new TreeMap<>();
        String parsedValue = null;
        String parsedName = null;

        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            while (parser.nextToken() != Token.END_OBJECT) {
                String fieldName = parser.currentName();
                // move to the field value
                parser.nextToken();
                switch (fieldName) {
                    case ATTRIBUTE_NAME_FIELD:
                        parsedName = parser.text();
                        break;
                    case ATTRIBUTE_VALUE_FIELD:
                        parsedValue = parser.text();
                        break;
                    default:
                        break;
                }
            }
            // reset every time I have seen a name-value pair.
            if (parsedName != null && parsedValue != null) {
                entities.put(parsedName, parsedValue);
                parsedValue = null;
                parsedName = null;
            }
        }
        return new Entity(entities);
    }

    @Generated
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Entity that = (Entity) o;
        return Objects.equal(attributes, that.attributes);
    }

    @Generated
    @Override
    public int hashCode() {
        return Objects.hashCode(attributes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(attributes, StreamOutput::writeString, StreamOutput::writeString);
    }

    /**
     * Used to print Entity info and localizing a node in a hash ring.
     * @return a normalized String representing the entity.
     */
    @Override
    public String toString() {
        return normalizedAttributes(attributes);
    }

    /**
    * Return a string of the attributes in the ascending order of attribute names
    * @return a normalized String corresponding to the Map.  The string is
    *  deterministic (i.e., no matter in what order we insert values,
    *  the returned the string is the same).  This is to ensure keys with the
    *  same content mapped to the same node in our hash ring.
    *
    */
    private static String normalizedAttributes(SortedMap<String, String> attributes) {
        return Joiner.on(",").withKeyValueSeparator("=").join(attributes);
    }

    /**
    * Create model Id out of detector Id and attribute name and value pairs
    *
    * HCAD v1 uses the categorical value as part of the model document Id, but
    *  OpenSearch’s document Id can be at most 512 bytes. Categorical values are
    *  usually less than 256 characters, but can grow to 32766 in theory.
    *  HCAD v1 skips an entity if the entity's name is more than 256 characters.
    *  We cannot do that in v2 as that can reject a lot of entities. To overcome
    *  the obstacle, we hash categorical values to a 128-bit string (like SHA-1
    *  that git uses) and use the hash as part of the model document Id.
    *
    * We have choices to make regarding when to use the hash as part of a model
    * document Id: for all HC detectors or a HC detector with multiple categorical
    * fields. The challenge lies in providing backward compatibility of looking for
    * a model checkpoint in the case of a HC detector with one categorical field.
    * If using hashes for all HC detectors, we need two get requests to ensure that
    * a model checkpoint exists. One uses the document Id without a hash, while one
    * uses the document Id with a hash. The dual get requests are ineffective. If
    * limiting hashes to a HC detector with multiple categorical fields, there is
    * no backward compatibility issue. However, the code will be branchy. One may
    * wonder if backward compatibility can be ignored; indeed, the old checkpoints
    * will be gone after a transition period during upgrading. During the transition
    * period, HC detectors can experience unnecessary cold starts as if the
    * detectors were just started. Checkpoint index size can double if every model
    * has two model documents. The transition period can be as long as 3 days since
    * our checkpoint retention period is 3 days. There is no perfect solution. We
    * prefer limiting hashes to an HC detector with multiple categorical fields as
    * its customer impact is none.
    *
    * @param detectorId Detector Id
    * @param attributes Attributes of an entity
    * @return the model Id
    */
    public static Optional<String> getModelId(String detectorId, SortedMap<String, String> attributes) {
        if (attributes.isEmpty()) {
            return Optional.empty();
        } else if (attributes.size() == 1) {
            for (Map.Entry<String, String> categoryValuePair : attributes.entrySet()) {
                // For OpenSearch, the limit of the document ID is 512 bytes.
                // skip an entity if the entity's name is more than 256 characters
                // since we are using it as part of document id.
                String categoricalValue = categoryValuePair.getValue().toString();
                if (categoricalValue.length() > AnomalyDetectorSettings.MAX_ENTITY_LENGTH) {
                    return Optional.empty();
                }
                return Optional.of(detectorId + MODEL_ID_INFIX + categoricalValue);
            }
            return Optional.empty();
        } else {
            String normalizedFields = normalizedAttributes(attributes);
            MurmurHash3.Hash128 hashFunc = MurmurHash3
                .hash128(
                    normalizedFields.getBytes(StandardCharsets.UTF_8),
                    0,
                    normalizedFields.length(),
                    RANDOM_SEED,
                    new MurmurHash3.Hash128()
                );
            // 16 bytes = 128 bits
            byte[] bytes = new byte[16];
            System.arraycopy(Numbers.longToBytes(hashFunc.h1), 0, bytes, 0, 8);
            System.arraycopy(Numbers.longToBytes(hashFunc.h2), 0, bytes, 8, 8);
            // Some bytes like 10 in ascii is corrupted in some systems. Base64 ensures we use safe bytes: https://tinyurl.com/mxmrhmhf
            return Optional.of(detectorId + MODEL_ID_INFIX + Base64.getUrlEncoder().withoutPadding().encodeToString(bytes));
        }
    }

    /**
     * Get the cached model Id if present. Or recompute one if missing.
     *
     * @param detectorId Detector Id.  Used as part of model Id.
     * @return Model Id.  Can be missing (e.g., the field value is too long for single-category detector)
     */
    public Optional<String> getModelId(String detectorId) {
        if (modelId.get() == null) {
            // computing model id is not cheap and the result is deterministic. We only do it once.
            Optional<String> computedModelId = Entity.getModelId(detectorId, attributes);
            if (computedModelId.isPresent()) {
                this.modelId.set(computedModelId.get());
            } else {
                this.modelId.set(null);
            }
        }
        return Optional.ofNullable(modelId.get());
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    /**
     * Generate multi-term query filter like
     * GET /company/_search
        {
          "query": {
            "bool": {
              "filter": [
                {
                  "term": {
                    "ip": "1.2.3.4"
                  }
                },
                {
                  "term": {
                    "name.keyword": "Kaituo"
                  }
                }
              ]
            }
          }
        }
     *
     *@return a list of term query builder
     */
    public List<TermQueryBuilder> getTermQueryBuilders() {
        List<TermQueryBuilder> res = new ArrayList<>();
        for (Map.Entry<String, String> attribute : attributes.entrySet()) {
            res.add(new TermQueryBuilder(attribute.getKey(), attribute.getValue()));
        }
        return res;
    }

    public List<TermQueryBuilder> getTermQueryBuilders(String pathPrefix) {
        List<TermQueryBuilder> res = new ArrayList<>();
        for (Map.Entry<String, String> attribute : attributes.entrySet()) {
            res.add(new TermQueryBuilder(pathPrefix + attribute.getKey(), attribute.getValue()));
        }
        return res;
    }

    /**
     * From json to Entity instance
     * @param entityValue json array consisting attributes
     * @return Entity instance
     * @throws IOException when there is an deserialization issue.
     */
    public static Entity fromJsonArray(Object entityValue) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field(CommonName.ENTITY_KEY, entityValue);
        content.endObject();

        try (
            InputStream stream = BytesReference.bytes(content).streamInput();
            XContentParser parser = JsonXContent.jsonXContent
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, stream)
        ) {
            // move to content.StartObject
            parser.nextToken();
            // move to CommonName.ENTITY_KEY
            parser.nextToken();
            // move to start of the array
            parser.nextToken();
            return Entity.parse(parser);
        }
    }

    public static Optional<Entity> fromJsonObject(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (false == CommonName.ENTITY_KEY.equals(parser.currentName())) {
                // not an object with "entity" as the root key
                return Optional.empty();
            }
            // move to start of the array
            parser.nextToken();
            return Optional.of(Entity.parse(parser));
        }
        return Optional.empty();
    }
}
