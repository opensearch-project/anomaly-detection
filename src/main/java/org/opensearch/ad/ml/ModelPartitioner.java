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

package org.opensearch.ad.ml;

import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Locale;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.MemoryTracker;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.constant.CommonErrorMessages;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.ad.settings.AnomalyDetectorSettings;
import org.opensearch.ad.util.DiscoveryNodeFilterer;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.config.Precision;

/**
 * This class breaks the circular dependency between NodeStateManager and ModelManager
 *
 */
public class ModelPartitioner {
    private static final Logger LOG = LogManager.getLogger(ModelPartitioner.class);
    protected static final String RCF_MODEL_ID_PATTERN = "%s_model_rcf_%d";
    protected static final String THRESHOLD_MODEL_ID_PATTERN = "%s_model_threshold";

    private int rcfNumSamplesInTree;
    private int rcfNumTrees;
    private DiscoveryNodeFilterer nodeFilter;
    private MemoryTracker memoryTracker;

    /**
     * Constructor
     * @param rcfNumSamplesInTree The sample size used by stream samplers in
     * this RCF forest
     * @param rcfNumTrees The number of trees in this RCF forest.
     * @param nodeFilter utility class to select nodes
     * @param memoryTracker AD memory usage tracker
     */
    public ModelPartitioner(int rcfNumSamplesInTree, int rcfNumTrees, DiscoveryNodeFilterer nodeFilter, MemoryTracker memoryTracker) {
        this.rcfNumSamplesInTree = rcfNumSamplesInTree;
        this.rcfNumTrees = rcfNumTrees;
        this.nodeFilter = nodeFilter;
        this.memoryTracker = memoryTracker;
    }

    /**
     * Construct a RCF model and then partition it by forest size.
     *
     * A RCF model is constructed based on the number of input features.
     *
     * Then a RCF model is first partitioned into desired size based on heap.
     * If there are more partitions than the number of nodes in the cluster,
     * the model is partitioned by the number of nodes and verified to
     * ensure the size of a partition does not exceed the max size limit based on heap.
     *
     * @param detector detector object
     * @return a pair of number of partitions and size of a parition (number of trees)
     * @throws LimitExceededException when there is no sufficient resource available
     */
    public Entry<Integer, Integer> getPartitionedForestSizes(AnomalyDetector detector) {
        int shingleSize = detector.getShingleSize();
        String detectorId = detector.getDetectorId();
        int rcfNumFeatures = detector.getEnabledFeatureIds().size() * shingleSize;
        return getPartitionedForestSizes(
            RandomCutForest
                .builder()
                .dimensions(rcfNumFeatures)
                .sampleSize(rcfNumSamplesInTree)
                .numberOfTrees(rcfNumTrees)
                .outputAfter(rcfNumSamplesInTree)
                .parallelExecutionEnabled(false)
                .compact(true)
                .precision(Precision.FLOAT_32)
                .boundingBoxCacheFraction(AnomalyDetectorSettings.REAL_TIME_BOUNDING_BOX_CACHE_RATIO)
                // same with dimension for opportunistic memory saving
                .shingleSize(detector.getShingleSize())
                .build(),
            detectorId
        );
    }

    /**
     * Partitions a RCF model by forest size.
     *
     * A RCF model is first partitioned into desired size based on heap.
     * If there are more partitions than the number of nodes in the cluster,
     * the model is partitioned by the number of nodes and verified to
     * ensure the size of a partition does not exceed the max size limit based on heap.
     *
     * @param forest RCF configuration, including forest size
     * @param detectorId ID of the detector with no effects on partitioning
     * @return a pair of number of partitions and size of a parition (number of trees)
     * @throws LimitExceededException when there is no sufficient resource available
     */
    public Entry<Integer, Integer> getPartitionedForestSizes(RandomCutForest forest, String detectorId) {
        long totalSize = memoryTracker.estimateTotalModelSize(forest);

        // desired partitioning
        long partitionSize = (Math.min(memoryTracker.getDesiredModelSize(), totalSize));
        int numPartitions = (int) Math.ceil((double) totalSize / (double) partitionSize);
        int forestSize = (int) Math.ceil((double) forest.getNumberOfTrees() / (double) numPartitions);

        int numNodes = nodeFilter.getEligibleDataNodes().length;
        if (numPartitions > numNodes) {
            // partition by cluster size
            partitionSize = (long) Math.ceil((double) totalSize / (double) numNodes);
            // verify against max size limit
            if (partitionSize <= memoryTracker.getHeapLimit()) {
                numPartitions = numNodes;
                forestSize = (int) Math.ceil((double) forest.getNumberOfTrees() / (double) numNodes);
            } else {
                throw new LimitExceededException(detectorId, CommonErrorMessages.MEMORY_LIMIT_EXCEEDED_ERR_MSG);
            }
        }

        return new SimpleImmutableEntry<>(numPartitions, forestSize);
    }

    /**
     * Returns the model ID for the RCF model partition.
     *
     * @param detectorId ID of the detector for which the RCF model is trained
     * @param partitionNumber number of the partition
     * @return ID for the RCF model partition
     */
    public String getRcfModelId(String detectorId, int partitionNumber) {
        return String.format(Locale.ROOT, RCF_MODEL_ID_PATTERN, detectorId, partitionNumber);
    }

    /**
     * Returns the model ID for the thresholding model.
     *
     * @param detectorId ID of the detector for which the thresholding model is trained
     * @return ID for the thresholding model
     */
    public String getThresholdModelId(String detectorId) {
        return String.format(Locale.ROOT, THRESHOLD_MODEL_ID_PATTERN, detectorId);
    }
}
