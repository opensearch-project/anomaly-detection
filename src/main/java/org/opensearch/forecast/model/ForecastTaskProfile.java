/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.model;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

import java.io.IOException;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.forecast.constant.ForecastCommonName;
import org.opensearch.timeseries.TaskProfile;

public class ForecastTaskProfile extends TaskProfile<ForecastTask> {

    public ForecastTaskProfile(
        ForecastTask forecastTask,
        Long rcfTotalUpdates,
        Long modelSizeInBytes,
        String nodeId,
        String taskId,
        String taskType
    ) {
        super(forecastTask, rcfTotalUpdates, modelSizeInBytes, nodeId, taskId, taskType);
    }

    public ForecastTaskProfile(StreamInput input) throws IOException {
        if (input.readBoolean()) {
            this.task = new ForecastTask(input);
        } else {
            this.task = null;
        }
        this.rcfTotalUpdates = input.readOptionalLong();
        this.modelSizeInBytes = input.readOptionalLong();
        this.nodeId = input.readOptionalString();
        this.taskId = input.readOptionalString();
        this.taskType = input.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (task != null) {
            out.writeBoolean(true);
            task.writeTo(out);
        } else {
            out.writeBoolean(false);
        }

        out.writeOptionalLong(rcfTotalUpdates);
        out.writeOptionalLong(modelSizeInBytes);
        out.writeOptionalString(nodeId);
        out.writeOptionalString(taskId);
        out.writeOptionalString(taskType);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();
        super.toXContent(xContentBuilder);
        return xContentBuilder.endObject();
    }

    public static ForecastTaskProfile parse(XContentParser parser) throws IOException {
        ForecastTask forecastTask = null;
        Long rcfTotalUpdates = null;
        Long modelSizeInBytes = null;
        String nodeId = null;
        String taskId = null;
        String taskType = null;

        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken();

            switch (fieldName) {
                case ForecastCommonName.FORECAST_TASK:
                    forecastTask = ForecastTask.parse(parser);
                    break;
                case RCF_TOTAL_UPDATES_FIELD:
                    rcfTotalUpdates = parser.longValue();
                    break;
                case MODEL_SIZE_IN_BYTES:
                    modelSizeInBytes = parser.longValue();
                    break;
                case NODE_ID_FIELD:
                    nodeId = parser.text();
                    break;
                case TASK_ID_FIELD:
                    taskId = parser.text();
                    break;
                case TASK_TYPE_FIELD:
                    taskType = parser.text();
                    break;
                default:
                    parser.skipChildren();
                    break;
            }
        }
        return new ForecastTaskProfile(forecastTask, rcfTotalUpdates, modelSizeInBytes, nodeId, taskId, taskType);
    }

    @Override
    protected String getTaskFieldName() {
        return ForecastCommonName.FORECAST_TASK;
    }
}
