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

package org.opensearch.ad.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class ThresholdResultTransportAction extends HandledTransportAction<ThresholdResultRequest, ThresholdResultResponse> {

    private static final Logger LOG = LogManager.getLogger(ThresholdResultTransportAction.class);
    private ModelManager manager;

    @Inject
    public ThresholdResultTransportAction(ActionFilters actionFilters, TransportService transportService, ModelManager manager) {
        super(ThresholdResultAction.NAME, transportService, actionFilters, ThresholdResultRequest::new);
        this.manager = manager;
    }

    @Override
    protected void doExecute(Task task, ThresholdResultRequest request, ActionListener<ThresholdResultResponse> listener) {

        try {
            LOG.info("Serve threshold request for {}", request.getModelID());
            manager
                .getThresholdingResult(
                    request.getAdID(),
                    request.getModelID(),
                    request.getRCFScore(),
                    ActionListener
                        .wrap(
                            result -> listener.onResponse(new ThresholdResultResponse(result.getGrade(), result.getConfidence())),
                            exception -> listener.onFailure(exception)
                        )
                );
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }
    }

}
