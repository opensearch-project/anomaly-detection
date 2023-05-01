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

package org.opensearch.ad.cluster;

import java.time.Clock;
import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.ad.util.ClientUtil;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;

@Deprecated
public class DailyCron implements Runnable {
    private static final Logger LOG = LogManager.getLogger(DailyCron.class);
    protected static final String FIELD_MODEL = "queue";
    static final String CANNOT_DELETE_OLD_CHECKPOINT_MSG = "Cannot delete old checkpoint.";
    static final String CHECKPOINT_NOT_EXIST_MSG = "Checkpoint index does not exist.";
    static final String CHECKPOINT_DELETED_MSG = "checkpoint docs get deleted";

    private final Clock clock;
    private final Duration checkpointTtl;
    private final ClientUtil clientUtil;

    public DailyCron(Clock clock, Duration checkpointTtl, ClientUtil clientUtil) {
        this.clock = clock;
        this.clientUtil = clientUtil;
        this.checkpointTtl = checkpointTtl;
    }

    @Override
    public void run() {
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(CommonName.CHECKPOINT_INDEX_NAME)
            .setQuery(
                QueryBuilders
                    .boolQuery()
                    .filter(
                        QueryBuilders
                            .rangeQuery(CheckpointDao.TIMESTAMP)
                            .lte(clock.millis() - checkpointTtl.toMillis())
                            .format(CommonName.EPOCH_MILLIS_FORMAT)
                    )
            )
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);
        clientUtil.execute(DeleteByQueryAction.INSTANCE, deleteRequest, ActionListener.wrap(response -> {
            // if 0 docs get deleted, it means our query cannot find any matching doc
            LOG.info("{} " + CHECKPOINT_DELETED_MSG, response.getDeleted());
        }, exception -> {
            if (exception instanceof IndexNotFoundException) {
                LOG.info(CHECKPOINT_NOT_EXIST_MSG);
            } else {
                // Gonna eventually delete in maintenance window.
                LOG.error(CANNOT_DELETE_OLD_CHECKPOINT_MSG, exception);
            }
        }));
    }

}
