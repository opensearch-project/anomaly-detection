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

import java.lang.reflect.Type;
import java.util.Base64;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.yahoo.memory.Memory;
import com.yahoo.sketches.kll.KllFloatsSketch;

/**
 * Serializes/deserailizes KllFloatsSketch.
 *
 * A sketch is serialized to a byte array and then encoded in Base64.
 */
public class KllFloatsSketchSerDe implements JsonSerializer<KllFloatsSketch>, JsonDeserializer<KllFloatsSketch> {

    @Override
    public JsonElement serialize(KllFloatsSketch src, Type typeOfSrc, JsonSerializationContext context) {
        return new JsonPrimitive(Base64.getEncoder().encodeToString(src.toByteArray()));
    }

    @Override
    public KllFloatsSketch deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) {
        return KllFloatsSketch.heapify(Memory.wrap(Base64.getDecoder().decode(json.getAsString())));
    }
}
