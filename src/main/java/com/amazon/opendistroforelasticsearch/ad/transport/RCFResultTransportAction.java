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

package com.amazon.opendistroforelasticsearch.ad.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import com.amazon.opendistroforelasticsearch.ad.breaker.ADCircuitBreakerService;
import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonErrorMessages;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;

public class RCFResultTransportAction extends HandledTransportAction<RCFResultRequest, RCFResultResponse> {

    private static final Logger LOG = LogManager.getLogger(RCFResultTransportAction.class);
    private ModelManager manager;
    private ADCircuitBreakerService adCircuitBreakerService;

    @Inject
    public RCFResultTransportAction(
        ActionFilters actionFilters,
        TransportService transportService,
        ModelManager manager,
        ADCircuitBreakerService adCircuitBreakerService
    ) {
        super(RCFResultAction.NAME, transportService, actionFilters, RCFResultRequest::new);
        this.manager = manager;
        this.adCircuitBreakerService = adCircuitBreakerService;
    }

    @Override
    protected void doExecute(Task task, RCFResultRequest request, ActionListener<RCFResultResponse> listener) {

        if (adCircuitBreakerService.isOpen()) {
            listener.onFailure(new LimitExceededException(request.getAdID(), CommonErrorMessages.MEMORY_CIRCUIT_BROKEN_ERR_MSG));
            return;
        }

        try {
            LOG.info("Serve rcf request for {}", request.getModelID());
            manager
                .getRcfResult(
                    request.getAdID(),
                    request.getModelID(),
                    request.getFeatures(),
                    ActionListener
                        .wrap(
                            result -> listener
                                .onResponse(
                                    new RCFResultResponse(
                                        result.getScore(),
                                        result.getConfidence(),
                                        result.getForestSize(),
                                        result.getAttribution()
                                    )
                                ),
                            exception -> {
                                LOG.warn(exception);
                                listener.onFailure(exception);
                            }
                        )
                );
        } catch (Exception e) {
            LOG.error(e);
            listener.onFailure(e);
        }

    }

}
