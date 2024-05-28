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

package org.opensearch.timeseries.transport.handler;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

import java.util.Iterator;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ExceptionsHelper;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.common.exception.TimeSeriesException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.indices.IndexManagement;
import org.opensearch.timeseries.indices.TimeSeriesIndex;
import org.opensearch.timeseries.model.IndexableResult;
import org.opensearch.timeseries.util.BulkUtil;
import org.opensearch.timeseries.util.ClientUtil;
import org.opensearch.timeseries.util.IndexUtils;
import org.opensearch.timeseries.util.RestHandlerUtils;

public class ResultIndexingHandler<ResultType extends IndexableResult, IndexType extends Enum<IndexType> & TimeSeriesIndex, IndexManagementType extends IndexManagement<IndexType>> {
    private static final Logger LOG = LogManager.getLogger(ResultIndexingHandler.class);
    public static final String FAIL_TO_SAVE_ERR_MSG = "Fail to save %s: ";
    public static final String SUCCESS_SAVING_MSG = "Succeed in saving %s";
    public static final String CANNOT_SAVE_ERR_MSG = "Cannot save %s due to write block.";
    public static final String RETRY_SAVING_ERR_MSG = "Retry in saving %s: ";

    protected final Client client;

    protected final ThreadPool threadPool;
    protected final BackoffPolicy savingBackoffPolicy;
    protected final String defaultResultIndexName;
    protected final IndexManagementType timeSeriesIndices;
    // whether save to a specific doc id or not. False by default.
    protected boolean fixedDoc;
    protected final ClientUtil clientUtil;
    protected final IndexUtils indexUtils;
    protected final ClusterService clusterService;

    /**
     * Abstract class for index operation.
     *
     * @param client client to OpenSearch query
     * @param settings accessor for node settings.
     * @param threadPool used to invoke specific threadpool to execute
     * @param indexName name of index to save to
     * @param timeSeriesIndices anomaly detection indices
     * @param clientUtil client wrapper
     * @param indexUtils Index util classes
     * @param clusterService accessor to ES cluster service
     */
    public ResultIndexingHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        String indexName,
        IndexManagementType timeSeriesIndices,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService,
        Setting<TimeValue> backOffDelaySetting,
        Setting<Integer> maxRetrySetting
    ) {
        this.client = client;
        this.threadPool = threadPool;
        this.savingBackoffPolicy = BackoffPolicy.exponentialBackoff(backOffDelaySetting.get(settings), maxRetrySetting.get(settings));
        this.defaultResultIndexName = indexName;
        this.timeSeriesIndices = timeSeriesIndices;
        this.fixedDoc = false;
        this.clientUtil = clientUtil;
        this.indexUtils = indexUtils;
        this.clusterService = clusterService;
    }

    /**
     * Since the constructor needs to provide injected value and Guice does not allow Boolean to be there
     * (claiming it does not know how to instantiate it), caller needs to manually set it to true if
     * it want to save to a specific doc.
     * @param fixedDoc whether to save to a specific doc Id
     */
    public void setFixedDoc(boolean fixedDoc) {
        this.fixedDoc = fixedDoc;
    }

    // TODO: check if user has permission to index.
    public void index(ResultType toSave, String detectorId, String indexOrAliasName) {
        try {
            if (indexOrAliasName != null) {
                if (indexUtils.checkIndicesBlocked(clusterService.state(), ClusterBlockLevel.WRITE, indexOrAliasName)) {
                    LOG.warn(String.format(Locale.ROOT, CANNOT_SAVE_ERR_MSG, detectorId));
                    return;
                }
                // We create custom result index when creating a detector. Custom result index can be rolled over and thus we may need to
                // create a new one.
                if (!timeSeriesIndices.doesIndexExist(indexOrAliasName) && !timeSeriesIndices.doesAliasExist(indexOrAliasName)) {
                    timeSeriesIndices.initCustomResultIndexDirectly(indexOrAliasName, ActionListener.wrap(response -> {
                        if (response.isAcknowledged()) {
                            save(toSave, detectorId, indexOrAliasName);
                        } else {
                            throw new TimeSeriesException(detectorId, String.format(Locale.ROOT, "Creating custom result index %s with mappings call not acknowledged", indexOrAliasName));
                        }
                    }, exception -> {
                        if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                            // It is possible the index has been created while we sending the create request
                            save(toSave, detectorId, indexOrAliasName);
                        } else {
                            throw new TimeSeriesException(detectorId, String.format(Locale.ROOT, "cannot create result index %s", indexOrAliasName), exception);
                        }
                    }));
                } else {
                    timeSeriesIndices.validateResultIndexMapping(indexOrAliasName, ActionListener.wrap(valid -> {
                        if (!valid) {
                            throw new EndRunException(detectorId, "wrong index mapping of custom AD result index", true);
                        } else {
                            save(toSave, detectorId, indexOrAliasName);
                        }
                    }, exception -> {throw new TimeSeriesException(detectorId, String.format(Locale.ROOT, "cannot validate result index %s", indexOrAliasName), exception); }));
                }
            } else {
                if (indexUtils.checkIndicesBlocked(clusterService.state(), ClusterBlockLevel.WRITE, this.defaultResultIndexName)) {
                    LOG.warn(String.format(Locale.ROOT, CANNOT_SAVE_ERR_MSG, detectorId));
                    return;
                }
                if (!timeSeriesIndices.doesDefaultResultIndexExist()) {
                    timeSeriesIndices
                        .initDefaultResultIndexDirectly(
                            ActionListener.wrap(initResponse -> onCreateIndexResponse(initResponse, toSave, detectorId), exception -> {
                                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                                    // It is possible the index has been created while we sending the create request
                                    save(toSave, detectorId);
                                } else {
                                    throw new TimeSeriesException(
                                        detectorId,
                                        String.format(Locale.ROOT, "Unexpected error creating index %s", defaultResultIndexName),
                                        exception
                                    );
                                }
                            })
                        );
                } else {
                    save(toSave, detectorId);
                }
            }
        } catch (Exception e) {
            throw new TimeSeriesException(
                detectorId,
                String.format(Locale.ROOT, "Error in saving %s for detector %s", defaultResultIndexName, detectorId),
                e
            );
        }
    }

    private void onCreateIndexResponse(CreateIndexResponse response, ResultType toSave, String detectorId) {
        if (response.isAcknowledged()) {
            save(toSave, detectorId);
        } else {
            throw new TimeSeriesException(
                detectorId,
                String.format(Locale.ROOT, "Creating %s with mappings call not acknowledged.", defaultResultIndexName)
            );
        }
    }

    protected void save(ResultType toSave, String detectorId) {
        save(toSave, detectorId, defaultResultIndexName);
    }

    // TODO: Upgrade custom result index mapping to latest version?
    // It may bring some issue if we upgrade the custom result index mapping while user is using that index
    // for other use cases. One easy solution is to tell user only use custom result index for AD plugin.
    // For the first release of custom result index, it's not a issue. Will leave this to next phase.
    protected void save(ResultType toSave, String detectorId, String indexName) {
        try (XContentBuilder builder = jsonBuilder()) {
            IndexRequest indexRequest = new IndexRequest(indexName).source(toSave.toXContent(builder, RestHandlerUtils.XCONTENT_WITH_TYPE));
            if (fixedDoc) {
                indexRequest.id(detectorId);
            }

            saveIteration(indexRequest, detectorId, savingBackoffPolicy.iterator());
        } catch (Exception e) {
            LOG.error(String.format(Locale.ROOT, "Failed to save %s", indexName), e);
            throw new TimeSeriesException(detectorId, String.format(Locale.ROOT, "Cannot save %s", indexName));
        }
    }

    void saveIteration(IndexRequest indexRequest, String configId, Iterator<TimeValue> backoff) {
        clientUtil.<IndexRequest, IndexResponse>asyncRequest(indexRequest, client::index, ActionListener.<IndexResponse>wrap(response -> {
            LOG.debug(String.format(Locale.ROOT, SUCCESS_SAVING_MSG, configId));
        }, exception -> {
            // OpenSearch has a thread pool and a queue for write per node. A thread
            // pool will have N number of workers ready to handle the requests. When a
            // request comes and if a worker is free , this is handled by the worker. Now by
            // default the number of workers is equal to the number of cores on that CPU.
            // When the workers are full and there are more write requests, the request
            // will go to queue. The size of queue is also limited. If by default size is,
            // say, 200 and if there happens more parallel requests than this, then those
            // requests would be rejected as you can see OpenSearchRejectedExecutionException.
            // So OpenSearchRejectedExecutionException is the way that OpenSearch tells us that
            // it cannot keep up with the current indexing rate.
            // When it happens, we should pause indexing a bit before trying again, ideally
            // with randomized exponential backoff.
            Throwable cause = ExceptionsHelper.unwrapCause(exception);
            if (!(cause instanceof OpenSearchRejectedExecutionException) || !backoff.hasNext()) {
                LOG.error(String.format(Locale.ROOT, FAIL_TO_SAVE_ERR_MSG, configId), cause);
            } else {
                TimeValue nextDelay = backoff.next();
                LOG.warn(String.format(Locale.ROOT, RETRY_SAVING_ERR_MSG, configId), cause);
                threadPool
                    .schedule(
                        () -> saveIteration(BulkUtil.cloneIndexRequest(indexRequest), configId, backoff),
                        nextDelay,
                        ThreadPool.Names.SAME
                    );
            }
        }));
    }
}
