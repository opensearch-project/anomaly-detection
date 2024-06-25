/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.opensearch.timeseries.AbstractSyntheticDataTest;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;

public class AbstractForecastSyntheticDataTest extends AbstractSyntheticDataTest {
    /**
     * Read data from a json array file up to a specified size
     * @param datasetFileName data set file name
     * @param size the limit of json elements to read
     * @return the read JsonObject list
     * @throws URISyntaxException when failing to find datasetFileName
     * @throws Exception when there is a parsing error.
     */
    public static List<JsonObject> readJsonArrayWithLimit(String datasetFileName, int limit) throws URISyntaxException {
        List<JsonObject> jsonObjects = new ArrayList<>();

        try (
            FileReader fileReader = new FileReader(
                new File(AbstractForecastSyntheticDataTest.class.getClassLoader().getResource(datasetFileName).toURI()),
                Charset.defaultCharset()
            );
            JsonReader jsonReader = new JsonReader(fileReader)
        ) {

            Gson gson = new Gson();
            JsonArray jsonArray = gson.fromJson(jsonReader, JsonArray.class);

            for (int i = 0; i < limit && i < jsonArray.size(); i++) {
                JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
                jsonObjects.add(jsonObject);
            }

        } catch (IOException e) {
            LOG.error("fail to read json array", e);
        }

        return jsonObjects;
    }
}
