/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ad.transport;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

public class ADHCImputeRequest extends BaseNodesRequest<ADHCImputeRequest> {
    private final String configId;
    private final String taskId;
    private final long dataStartMillis;
    private final long dataEndMillis;

    public ADHCImputeRequest(String configId, String taskId, long startMillis, long endMillis, DiscoveryNode... nodes) {
        super(nodes);
        this.configId = configId;
        this.taskId = taskId;
        this.dataStartMillis = startMillis;
        this.dataEndMillis = endMillis;
    }

    public ADHCImputeRequest(StreamInput in) throws IOException {
        super(in);
        this.configId = in.readString();
        this.taskId = in.readOptionalString();
        this.dataStartMillis = in.readLong();
        this.dataEndMillis = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(configId);
        out.writeOptionalString(taskId);
        out.writeLong(dataStartMillis);
        out.writeLong(dataEndMillis);
    }

    public String getConfigId() {
        return configId;
    }

    public String getTaskId() {
        return taskId;
    }

    public long getDataStartMillis() {
        return dataStartMillis;
    }

    public long getDataEndMillis() {
        return dataEndMillis;
    }
}
