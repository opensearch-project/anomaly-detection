/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.indices;

import java.io.IOException;

import org.opensearch.test.OpenSearchTestCase;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ForecastIndexMappingTests extends OpenSearchTestCase {

    public void testGetForecastResultMappings() throws IOException {
        String mapping = ForecastIndexManagement.getResultMappings();

        // Use Jackson to convert the string into a JsonNode
        ObjectMapper mapper = new ObjectMapper();
        JsonNode mappingJson = mapper.readTree(mapping);

        // Check the existence of some fields
        assertTrue("forecaster_id field is missing", mappingJson.path("properties").has("forecaster_id"));
        assertTrue("feature_data field is missing", mappingJson.path("properties").has("feature_data"));
        assertTrue("data_start_time field is missing", mappingJson.path("properties").has("data_start_time"));
        assertTrue("execution_start_time field is missing", mappingJson.path("properties").has("execution_start_time"));
        assertTrue("user field is missing", mappingJson.path("properties").has("user"));
        assertTrue("entity field is missing", mappingJson.path("properties").has("entity"));
        assertTrue("schema_version field is missing", mappingJson.path("properties").has("schema_version"));
        assertTrue("task_id field is missing", mappingJson.path("properties").has("task_id"));
        assertTrue("model_id field is missing", mappingJson.path("properties").has("model_id"));
        assertTrue("forecast_series field is missing", mappingJson.path("properties").has("forecast_value"));
    }

    public void testGetCheckpointMappings() throws IOException {
        String mapping = ForecastIndexManagement.getCheckpointMappings();

        // Use Jackson to convert the string into a JsonNode
        ObjectMapper mapper = new ObjectMapper();
        JsonNode mappingJson = mapper.readTree(mapping);

        // Check the existence of some fields
        assertTrue("forecaster_id field is missing", mappingJson.path("properties").has("forecaster_id"));
        assertTrue("timestamp field is missing", mappingJson.path("properties").has("timestamp"));
        assertTrue("schema_version field is missing", mappingJson.path("properties").has("schema_version"));
        assertTrue("entity field is missing", mappingJson.path("properties").has("entity"));
        assertTrue("model field is missing", mappingJson.path("properties").has("model"));
        assertTrue("samples field is missing", mappingJson.path("properties").has("samples"));
        assertTrue("last_processed_sample field is missing", mappingJson.path("properties").has("last_processed_sample"));
    }

    public void testGetStateMappings() throws IOException {
        String mapping = ForecastIndexManagement.getStateMappings();

        // Use Jackson to convert the string into a JsonNode
        ObjectMapper mapper = new ObjectMapper();
        JsonNode mappingJson = mapper.readTree(mapping);

        // Check the existence of some fields
        assertTrue("schema_version field is missing", mappingJson.path("properties").has("schema_version"));
        assertTrue("last_update_time field is missing", mappingJson.path("properties").has("last_update_time"));
        assertTrue("error field is missing", mappingJson.path("properties").has("error"));
        assertTrue("started_by field is missing", mappingJson.path("properties").has("started_by"));
        assertTrue("stopped_by field is missing", mappingJson.path("properties").has("stopped_by"));
        assertTrue("forecaster_id field is missing", mappingJson.path("properties").has("forecaster_id"));
        assertTrue("state field is missing", mappingJson.path("properties").has("state"));
        assertTrue("task_progress field is missing", mappingJson.path("properties").has("task_progress"));
        assertTrue("init_progress field is missing", mappingJson.path("properties").has("init_progress"));
        assertTrue("current_piece field is missing", mappingJson.path("properties").has("current_piece"));
        assertTrue("execution_start_time field is missing", mappingJson.path("properties").has("execution_start_time"));
        assertTrue("execution_end_time field is missing", mappingJson.path("properties").has("execution_end_time"));
        assertTrue("is_latest field is missing", mappingJson.path("properties").has("is_latest"));
        assertTrue("task_type field is missing", mappingJson.path("properties").has("task_type"));
        assertTrue("checkpoint_id field is missing", mappingJson.path("properties").has("checkpoint_id"));
        assertTrue("coordinating_node field is missing", mappingJson.path("properties").has("coordinating_node"));
        assertTrue("worker_node field is missing", mappingJson.path("properties").has("worker_node"));
        assertTrue("user field is missing", mappingJson.path("properties").has("user"));
        assertTrue("forecaster field is missing", mappingJson.path("properties").has("forecaster"));
        assertTrue("date_range field is missing", mappingJson.path("properties").has("date_range"));
        assertTrue("parent_task_id field is missing", mappingJson.path("properties").has("parent_task_id"));
        assertTrue("entity field is missing", mappingJson.path("properties").has("entity"));
        assertTrue("estimated_minutes_left field is missing", mappingJson.path("properties").has("estimated_minutes_left"));
    }

}
