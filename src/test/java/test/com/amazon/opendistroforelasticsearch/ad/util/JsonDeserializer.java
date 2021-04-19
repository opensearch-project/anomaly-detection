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

package test.com.amazon.opendistroforelasticsearch.ad.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang.StringUtils;

import com.amazon.opendistroforelasticsearch.ad.common.exception.JsonPathNotFoundException;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;

public class JsonDeserializer {
    private static JsonParser parser = new JsonParser();

    /**
     * Search a Gson JsonObject inside a JSON string matching the input path
     * expression
     *
     * @param jsonString
     *            an encoded JSON string
     * @param paths
     *            path fragments
     * @return the matching Gson JsonObject or null in case of no match.
     * @throws IOException if the underlying input source has IO issues during parsing
     */
    public static JsonElement getChildNode(String jsonString, String... paths) throws IOException {
        JsonElement rootElement = parse(jsonString);
        return getChildNode(rootElement, paths);
    }

    /**
     * JsonParseException is an unchecked exception.  Rethrow a checked exception
     * to force the client to catch that exception.
     * @param jsonString json string to parse
     * @return a parse tree of JsonElements corresponding to the specified JSON
     * @throws IOException if the underlying input source has IO issues during parsing
     */
    public static JsonElement parse(String jsonString) throws IOException {
        try {
            return parser.parse(jsonString);
        } catch (JsonParseException e) {
            throw new IOException(e.getCause());
        }
    }

    /**
     * Json validation: is the parameter a valid json string?
     *
     * @param jsonString an encoded JSON string
     * @return whether this is a valid json string
     */
    public static boolean isValidJson(String jsonString) {
        try {
            parser.parse(jsonString);
        } catch (JsonParseException e) {
            return false;
        }
        return true;
    }

    /**
     * Get the root node inside a JSON string
     *
     * @param jsonString
     *            an encoded JSON string
     * @return the root node.
     * @throws JsonPathNotFoundException if json path is invalid
     * @throws IOException if the underlying input source has IO issues during parsing
     */
    public static JsonObject getRootNode(String jsonString) throws JsonPathNotFoundException, IOException {
        if (StringUtils.isBlank(jsonString))
            throw new JsonPathNotFoundException();

        return parse(jsonString).getAsJsonObject();
    }

    /**
     * Check if there is a Gson JsonObject inside a JSON string matching the
     * input path expression. Only exact path match works (e.g.
     * JSONUtils.hasChildNode("{\"qst\":3}", "qs") return false).
     *
     * @param jsonString
     *            an encoded JSON string
     * @param paths
     *            path fragments
     * @return true in case of match; otherwise false.
     * @throws IOException if the underlying input source has IO issues during parsing
     */
    public static boolean hasChildNode(String jsonString, String... paths) throws IOException {
        if (StringUtils.isBlank(jsonString))
            return false;
        return getChildNode(jsonString, paths) != null;
    }

    /**
     * Check if there is a Gson JsonObject inside a JSON string matching the
     * input path expression. Only exact path match works (e.g.
     * JSONUtils.hasChildNode("{\"qst\":3}", "qs") return false).
     *
     * @param jsonObject
     *            a Gson JsonObject
     * @param paths
     *            path fragments
     * @return true in case of match; otherwise false.
     */
    public static boolean hasChildNode(JsonObject jsonObject, String... paths) {
        if (jsonObject == null)
            return false;
        return getChildNode(jsonObject, paths) != null;
    }

    /**
     * Search a Gson JsonObject inside a Gson JsonObject matching the
     * input path expression
     *
     * @param jsonElement
     *            a Gson JsonObject
     * @param paths
     *            path fragments
     * @return the matching Gson JsonObject or null in case of no match.
     */
    public static JsonElement getChildNode(JsonElement jsonElement, String... paths) {
        if (paths == null) {
            return null;
        }
        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];
            if (!(jsonElement instanceof JsonObject)) {
                return null;
            }
            JsonObject jsonObject = jsonElement.getAsJsonObject();
            if (!jsonObject.has(path))
                return null;

            jsonElement = jsonObject.get(path);
        }

        return jsonElement;
    }

    /**
     * Search a string inside a JSON string matching the input path expression
     *
     * @param jsonElement
     *            a Gson JsonElement
     * @param paths
     *            path fragments
     * @return the matching string or null in case of no match.
     */
    public static String getTextValue(JsonElement jsonElement, String... paths) throws JsonPathNotFoundException {

        jsonElement = getChildNode(jsonElement, paths);
        if (jsonElement != null) {
            return jsonElement.getAsString();
        }

        throw new JsonPathNotFoundException();
    }

    /**
     * Search a long number inside a JSON string matching the input path
     * expression
     *
     * @param jsonElement
     *            a Gson JsonElement
     * @param paths
     *            path fragments
     * @return the matching long number or null in case of no match.
     */
    public static long getLongValue(JsonElement jsonElement, String... paths) throws JsonPathNotFoundException {

        jsonElement = getChildNode(jsonElement, paths);
        if (jsonElement != null) {
            return jsonElement.getAsLong();
        }

        throw new JsonPathNotFoundException();
    }

    /**
     * Search an int number inside a JSON string matching the input path
     * expression
     *
     * @param jsonElement
     *            a Gson JsonElement
     * @param paths
     *            path fragments
     * @return the matching int number or null in case of no match.
     */
    public static int getIntValue(JsonElement jsonElement, String... paths) throws JsonPathNotFoundException {

        jsonElement = getChildNode(jsonElement, paths);
        if (jsonElement != null) {
            return jsonElement.getAsInt();
        }

        throw new JsonPathNotFoundException();
    }

    /**
     * Search a string inside a JSON string matching the input path expression
     *
     * @param jsonString
     *            an encoded JSON string
     * @param paths
     *            path fragments
     * @return the matching string or null in case of no match.
     * @throws JsonPathNotFoundException if json path is invalid
     * @throws IOException if the underlying input source has IO issues during parsing
     */
    public static String getTextValue(String jsonString, String... paths) throws JsonPathNotFoundException, IOException {
        if (paths != null && paths.length > 0) {
            JsonElement jsonElement = getChildNode(jsonString, paths);
            if (jsonElement != null) {
                return jsonElement.getAsString();
            }
        }

        throw new JsonPathNotFoundException();
    }

    /**
     * Search a long number inside a JSON string matching the input path
     * expression
     *
     * @param jsonString
     *            an encoded JSON string
     * @param paths
     *            path fragments
     * @return the matching long number or null in case of no match.
     * @throws JsonPathNotFoundException if json path is invalid
     * @throws IOException if the underlying input source has IO issues during parsing
     */
    public static long getLongValue(String jsonString, String... paths) throws JsonPathNotFoundException, IOException {
        if (paths != null && paths.length > 0) {
            JsonElement jsonElement = getChildNode(jsonString, paths);
            if (jsonElement != null) {
                return jsonElement.getAsLong();
            }
        }

        throw new JsonPathNotFoundException();
    }

    /**
     * Search an int number inside a JSON string matching the input path
     * expression
     *
     * @param jsonString
     *            an encoded JSON string
     * @param paths
     *            path fragments
     * @return the matching int number or null in case of no match.
     * @throws JsonPathNotFoundException if json path is invalid
     * @throws IOException if the underlying input source has IO issues during parsing
     */
    public static int getIntValue(String jsonString, String... paths) throws JsonPathNotFoundException, IOException {
        if (paths != null && paths.length > 0) {
            JsonElement jsonElement = getChildNode(jsonString, paths);
            if (jsonElement != null) {
                return jsonElement.getAsInt();
            }
        }

        throw new JsonPathNotFoundException();
    }

    private static String handleJsonPathNotFoundException(boolean returnEmptyStringIfMissing) throws JsonPathNotFoundException {
        if (returnEmptyStringIfMissing) {
            return "";
        } else {
            throw new JsonPathNotFoundException();
        }
    }

    /**
     * Search JSON element from a JSON root.
     * If returnEmptyStringIfMissing is true, return "" when json path not found.
     *
     * @param jsonElement
     *            a Gson JsonElement
     * @param path
     *            path fragment
     * @return the matching string or null in case of no match.
     */
    public static String getTextValue(JsonElement jsonElement, String path, boolean returnEmptyStringIfMissing)
        throws JsonPathNotFoundException {
        try {
            return getTextValue(jsonElement, path);
        } catch (JsonPathNotFoundException e) {
            return handleJsonPathNotFoundException(returnEmptyStringIfMissing);
        }
    }

    /**
     * Search a string inside a JSON string matching the input path expression
     *
     * @param jsonString
     *            an encoded JSON string
     * @param paths
     *            path fragments
     * @return the matching string or null in case of no match.
     * @throws JsonPathNotFoundException if json path is invalid
     * @throws IOException if the underlying input source has IO issues during parsing
     */
    public static String getTextValue(String jsonString, String paths, boolean returnEmptyStringIfMissing) throws JsonPathNotFoundException,
        IOException {
        try {
            return getTextValue(jsonString, paths);
        } catch (JsonPathNotFoundException e) {
            return handleJsonPathNotFoundException(returnEmptyStringIfMissing);
        }
    }

    /**
     * Search an array inside a JSON string matching the input path expression and convert each element using a function
     *
     * @param jsonString an encoded JSON string
     * @param function function to parse each element
     * @param paths path fragments
     * @return an array of values
     * @throws JsonPathNotFoundException if json path is invalid
     * @throws IOException               if the underlying input source has problems
     *                                   during parsing
     */
    @SuppressWarnings("unchecked")
    public static <T> T[] getArrayValue(String jsonString, Function<JsonElement, T> function, String... paths)
        throws JsonPathNotFoundException,
        IOException {
        JsonElement jsonNode = getChildNode(jsonString, paths);
        if (jsonNode != null && jsonNode.isJsonArray()) {
            JsonArray array = jsonNode.getAsJsonArray();
            Object[] values = new Object[array.size()];
            for (int i = 0; i < array.size(); i++) {
                values[i] = function.apply(array.get(i));
            }
            return (T[]) values;
        }
        throw new JsonPathNotFoundException();
    }

    /**
     * Search an array inside a JSON string matching the input path expression
     *
     * @param jsonString an encoded JSON string
     * @param paths      path fragments
     * @throws JsonPathNotFoundException if json path is invalid
     * @throws IOException               if the underlying input source has problems
     *                                   during parsing
     */
    @SuppressWarnings("unchecked")
    public static JsonArray getArrayValue(String jsonString, String... paths) throws JsonPathNotFoundException, IOException {
        JsonElement jsonNode = getChildNode(jsonString, paths);
        if (jsonNode != null && jsonNode.isJsonArray()) {
            return jsonNode.getAsJsonArray();
        }
        throw new JsonPathNotFoundException();
    }

    /**
     * Search a double number inside a JSON string matching the input path
     * expression
     *
     * @param jsonString an encoded JSON string
     * @param paths      path fragments
     * @return the matching double number
     * @throws JsonPathNotFoundException if json path is invalid
     * @throws IOException               if the underlying input source has problems
     *                                   during parsing
     */
    public static double getDoubleValue(String jsonString, String... paths) throws JsonPathNotFoundException, IOException {
        JsonElement jsonNode = getChildNode(jsonString, paths);
        if (jsonNode != null) {
            return jsonNode.getAsDouble();
        }
        throw new JsonPathNotFoundException();
    }

    /**
     * Search an int number inside a JSON string matching the input path expression
     *
     * @param jsonString an encoded JSON string
     * @param paths      path fragments
     * @return list of double
     * @throws JsonPathNotFoundException if json path is invalid
     * @throws IOException               if the underlying input source has problems
     *                                   during parsing
     */
    public static double[] getDoubleArrayValue(String jsonString, String... paths) throws JsonPathNotFoundException, IOException {
        JsonElement jsonNode = getChildNode(jsonString, paths);
        if (jsonNode != null && jsonNode.isJsonArray()) {
            JsonArray array = jsonNode.getAsJsonArray();
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                values.add(array.get(i).getAsDouble());
            }
            return values.stream().mapToDouble(i -> i).toArray();
        }
        throw new JsonPathNotFoundException();
    }

    public static <T> List<T> getListValue(String jsonString, Function<JsonElement, T> function, String... paths)
        throws JsonPathNotFoundException,
        IOException {
        JsonElement jsonNode = getChildNode(jsonString, paths);
        if (jsonNode != null && jsonNode.isJsonArray()) {
            JsonArray array = jsonNode.getAsJsonArray();
            List<T> values = new ArrayList<>(array.size());
            for (int i = 0; i < array.size(); i++) {
                values.add(function.apply(array.get(i)));
            }
            return values;
        }
        throw new JsonPathNotFoundException();
    }

    public static double getDoubleValue(JsonElement jsonElement, String... paths) throws JsonPathNotFoundException, IOException {
        JsonElement jsonNode = getChildNode(jsonElement, paths);
        if (jsonNode != null) {
            return jsonNode.getAsDouble();
        }
        throw new JsonPathNotFoundException();
    }
}
