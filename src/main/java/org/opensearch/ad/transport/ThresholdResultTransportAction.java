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

package org.opensearch.ad.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.ad.ml.ModelManager;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
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
