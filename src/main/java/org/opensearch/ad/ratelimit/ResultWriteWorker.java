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

package org.opensearch.ad.ratelimit;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.RESULT_WRITE_QUEUE_BATCH_SIZE;
import static org.opensearch.ad.settings.AnomalyDetectorSettings.RESULT_WRITE_QUEUE_CONCURRENCY;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ad.NodeStateManager;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.model.AnomalyResult;
import org.opensearch.ad.transport.ADResultBulkRequest;
import org.opensearch.ad.transport.ADResultBulkResponse;
import org.opensearch.ad.transport.handler.MultiEntityResultHandler;
import org.opensearch.ad.util.ExceptionUtil;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.threadpool.ThreadPool;

public class ResultWriteWorker extends BatchWorker<ResultWriteRequest, ADResultBulkRequest, ADResultBulkResponse> {
    private static final Logger LOG = LogManager.getLogger(ResultWriteWorker.class);
    public static final String WORKER_NAME = "result-write";

    private final MultiEntityResultHandler resultHandler;
    private NamedXContentRegistry xContentRegistry;

    public ResultWriteWorker(
        long heapSizeInBytes,
        int singleRequestSizeInBytes,
        Setting<Float> maxHeapPercentForQueueSetting,
        ClusterService clusterService,
        Random random,
        ADCircuitBreakerService adCircuitBreakerService,
        ThreadPool threadPool,
        Settings settings,
        float maxQueuedTaskRatio,
        Clock clock,
        float mediumSegmentPruneRatio,
        float lowSegmentPruneRatio,
        int maintenanceFreqConstant,
        Duration executionTtl,
        MultiEntityResultHandler resultHandler,
        NamedXContentRegistry xContentRegistry,
        NodeStateManager stateManager,
        Duration stateTtl
    ) {
        super(
            WORKER_NAME,
            heapSizeInBytes,
            singleRequestSizeInBytes,
            maxHeapPercentForQueueSetting,
            clusterService,
            random,
            adCircuitBreakerService,
            threadPool,
            settings,
            maxQueuedTaskRatio,
            clock,
            mediumSegmentPruneRatio,
            lowSegmentPruneRatio,
            maintenanceFreqConstant,
            RESULT_WRITE_QUEUE_CONCURRENCY,
            executionTtl,
            RESULT_WRITE_QUEUE_BATCH_SIZE,
            stateTtl,
            stateManager
        );
        this.resultHandler = resultHandler;
        this.xContentRegistry = xContentRegistry;
    }

    @Override
    protected void executeBatchRequest(ADResultBulkRequest request, ActionListener<ADResultBulkResponse> listener) {
        if (request.numberOfActions() < 1) {
            listener.onResponse(null);
            return;
        }
        resultHandler.flush(request, listener);
    }

    @Override
    protected ADResultBulkRequest toBatchRequest(List<ResultWriteRequest> toProcess) {
        final ADResultBulkRequest bulkRequest = new ADResultBulkRequest();
        for (ResultWriteRequest request : toProcess) {
            bulkRequest.add(request);
        }
        return bulkRequest;
    }

    @Override
    protected ActionListener<ADResultBulkResponse> getResponseListener(
        List<ResultWriteRequest> toProcess,
        ADResultBulkRequest bulkRequest
    ) {
        return ActionListener.wrap(adResultBulkResponse -> {
            if (adResultBulkResponse == null || false == adResultBulkResponse.getRetryRequests().isPresent()) {
                // all successful
                return;
            }

            enqueueRetryRequestIteration(adResultBulkResponse.getRetryRequests().get(), 0);
        }, exception -> {
            if (ExceptionUtil.isRetryAble(exception)) {
                // retry all of them
                super.putAll(toProcess);
            } else if (ExceptionUtil.isOverloaded(exception)) {
                LOG.error("too many get AD model checkpoint requests or shard not avialble");
                setCoolDownStart();
            }

            for (ResultWriteRequest request : toProcess) {
                nodeStateManager.setException(request.getDetectorId(), exception);
            }
            LOG.error("Fail to save results", exception);
        });
    }

    private void enqueueRetryRequestIteration(List<IndexRequest> requestToRetry, int index) {
        if (index >= requestToRetry.size()) {
            return;
        }
        DocWriteRequest<?> currentRequest = requestToRetry.get(index);
        Optional<AnomalyResult> resultToRetry = getAnomalyResult(currentRequest);
        if (false == resultToRetry.isPresent()) {
            enqueueRetryRequestIteration(requestToRetry, index + 1);
            return;
        }
        AnomalyResult result = resultToRetry.get();
        String detectorId = result.getDetectorId();
        nodeStateManager.getAnomalyDetector(detectorId, onGetDetector(requestToRetry, index, detectorId, result));
    }

    private ActionListener<Optional<AnomalyDetector>> onGetDetector(
        List<IndexRequest> requestToRetry,
        int index,
        String detectorId,
        AnomalyResult resultToRetry
    ) {
        return ActionListener.wrap(detectorOptional -> {
            if (false == detectorOptional.isPresent()) {
                LOG.warn(new ParameterizedMessage("AnomalyDetector [{}] is not available.", detectorId));
                enqueueRetryRequestIteration(requestToRetry, index + 1);
                return;
            }

            AnomalyDetector detector = detectorOptional.get();
            super.put(
                new ResultWriteRequest(
                    // expire based on execute start time
                    resultToRetry.getExecutionStartTime().toEpochMilli() + detector.getDetectorIntervalInMilliseconds(),
                    detectorId,
                    resultToRetry.isHighPriority() ? RequestPriority.HIGH : RequestPriority.MEDIUM,
                    resultToRetry,
                    detector.getResultIndex()
                )
            );

            enqueueRetryRequestIteration(requestToRetry, index + 1);

        }, exception -> {
            LOG.error(new ParameterizedMessage("fail to get detector [{}]", detectorId), exception);
            enqueueRetryRequestIteration(requestToRetry, index + 1);
        });
    }

    private Optional<AnomalyResult> getAnomalyResult(DocWriteRequest<?> request) {
        try {
            if (false == (request instanceof IndexRequest)) {
                LOG.error(new ParameterizedMessage("We should only send IndexRquest, but get [{}].", request));
                return Optional.empty();
            }
            // we send IndexRequest previously
            IndexRequest indexRequest = (IndexRequest) request;
            BytesReference indexSource = indexRequest.source();
            MediaType indexContentType = indexRequest.getContentType();
            try (
                XContentParser xContentParser = XContentHelper
                    .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, indexSource, indexContentType)
            ) {
                // the first character is null. Without skipping it, we get
                // org.opensearch.core.common.ParsingException: Failed to parse object: expecting token of type [START_OBJECT] but found
                // [null]
                xContentParser.nextToken();
                return Optional.of(AnomalyResult.parse(xContentParser));
            }
        } catch (Exception e) {
            LOG.error(new ParameterizedMessage("Fail to parse index request [{}]", request), e);
        }
        return Optional.empty();
    }
}
