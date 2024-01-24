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

package org.opensearch.ad.cluster.diskcleanup;

import java.time.Clock;
import java.time.Duration;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.constant.CommonName;
import org.opensearch.ad.ml.CheckpointDao;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;

/**
 * Model checkpoints cleanup of multi-entity detectors.
 * <p> <b>Problem:</b>
 *     In multi-entity detectors, we can have thousands, even millions of entities, of which the model checkpoints will consume
 *     lots of disk resources. To protect the our disk usage, the checkpoint index size will be limited with specified threshold.
 *     Once its size exceeds the threshold, the model checkpoints cleanup process will be activated.
 * </p>
 * <p> <b>Solution:</b>
 *     Before multi-entity detectors, there is daily cron job to clean up the inactive checkpoints longer than some configurable days.
 *     We will keep the this logic, and add new clean up way based on shard size.
 * </p>
 */
public class ModelCheckpointIndexRetention implements Runnable {
    private static final Logger LOG = LogManager.getLogger(ModelCheckpointIndexRetention.class);

    // The recommended max shard size is 50G, we don't wanna our index exceeds this number
    private static final long MAX_SHARD_SIZE_IN_BYTE = 50 * 1024 * 1024 * 1024L;
    // We can't clean up all of the checkpoints. At least keep models for 1 day
    private static final Duration MINIMUM_CHECKPOINT_TTL = Duration.ofDays(1);
    static final String CHECKPOINT_NOT_EXIST_MSG = "Checkpoint index does not exist.";

    private final Duration defaultCheckpointTtl;
    private final Clock clock;
    private final IndexCleanup indexCleanup;

    public ModelCheckpointIndexRetention(Duration defaultCheckpointTtl, Clock clock, IndexCleanup indexCleanup) {
        this.defaultCheckpointTtl = defaultCheckpointTtl;
        this.clock = clock;
        this.indexCleanup = indexCleanup;
    }

    @Override
    public void run() {
        indexCleanup
            .deleteDocsByQuery(
                CommonName.CHECKPOINT_INDEX_NAME,
                QueryBuilders
                    .boolQuery()
                    .filter(
                        QueryBuilders
                            .rangeQuery(CheckpointDao.TIMESTAMP)
                            .lte(clock.millis() - defaultCheckpointTtl.toMillis())
                            .format(CommonName.EPOCH_MILLIS_FORMAT)
                    ),
                ActionListener.wrap(response -> {
                    cleanupBasedOnShardSize(defaultCheckpointTtl.minusDays(1));
                },
                    // The docs will be deleted in next scheduled windows. No need for retrying.
                    exception -> LOG.error("delete docs by query fails for checkpoint index", exception)
                )
            );

    }

    private void cleanupBasedOnShardSize(Duration cleanUpTtl) {
        indexCleanup
            .deleteDocsBasedOnShardSize(
                CommonName.CHECKPOINT_INDEX_NAME,
                MAX_SHARD_SIZE_IN_BYTE,
                QueryBuilders
                    .boolQuery()
                    .filter(
                        QueryBuilders
                            .rangeQuery(CheckpointDao.TIMESTAMP)
                            .lte(clock.millis() - cleanUpTtl.toMillis())
                            .format(CommonName.EPOCH_MILLIS_FORMAT)
                    ),
                ActionListener.wrap(cleanupNeeded -> {
                    if (cleanupNeeded) {
                        if (cleanUpTtl.equals(MINIMUM_CHECKPOINT_TTL)) {
                            return;
                        }

                        Duration nextCleanupTtl = cleanUpTtl.minusDays(1);
                        if (nextCleanupTtl.compareTo(MINIMUM_CHECKPOINT_TTL) < 0) {
                            nextCleanupTtl = MINIMUM_CHECKPOINT_TTL;
                        }
                        cleanupBasedOnShardSize(nextCleanupTtl);
                    } else {
                        LOG.debug("clean up not needed anymore for checkpoint index");
                    }
                },
                    // The docs will be deleted in next scheduled windows. No need for retrying.
                    exception -> {
                        if (exception instanceof IndexNotFoundException) {
                            // the method will be called hourly
                            // don't log stack trace as most of OpenSearch domains have no AD installed
                            LOG.debug(CHECKPOINT_NOT_EXIST_MSG);
                        } else {
                            LOG.error("checkpoint index retention based on shard size fails", exception);
                        }
                    }
                )
            );
    }
}
