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

package org.opensearch.ad;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ad.breaker.ADCircuitBreakerService;
import org.opensearch.ad.common.exception.LimitExceededException;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.monitor.jvm.JvmService;

import com.amazon.randomcutforest.RandomCutForest;

/**
 * Class to track AD memory usage.
 *
 */
public class MemoryTracker {
    private static final Logger LOG = LogManager.getLogger(MemoryTracker.class);

    public enum Origin {
        SINGLE_ENTITY_DETECTOR,
        HC_DETECTOR,
        HISTORICAL_SINGLE_ENTITY_DETECTOR,
    }

    // memory tracker for total consumption of bytes
    private long totalMemoryBytes;
    private final Map<Origin, Long> totalMemoryBytesByOrigin;
    // reserved for models. Cannot be deleted at will.
    private long reservedMemoryBytes;
    private final Map<Origin, Long> reservedMemoryBytesByOrigin;
    private long heapSize;
    private long heapLimitBytes;
    private long desiredModelSize;
    // we observe threshold model uses a fixed size array and the size is the same
    private int thresholdModelBytes;
    private int sampleSize;
    private ADCircuitBreakerService adCircuitBreakerService;

    /**
     * Constructor
     *
     * @param jvmService Service providing jvm info
     * @param modelMaxSizePercentage Percentage of heap for the max size of a model
     * @param modelDesiredSizePercentage percentage of heap for the desired size of a model
     * @param clusterService Cluster service object
     * @param sampleSize The sample size used by stream samplers in a RCF forest
     * @param adCircuitBreakerService Memory circuit breaker
     */
    public MemoryTracker(
        JvmService jvmService,
        double modelMaxSizePercentage,
        double modelDesiredSizePercentage,
        ClusterService clusterService,
        int sampleSize,
        ADCircuitBreakerService adCircuitBreakerService
    ) {
        this.totalMemoryBytes = 0;
        this.totalMemoryBytesByOrigin = new EnumMap<Origin, Long>(Origin.class);
        this.reservedMemoryBytes = 0;
        this.reservedMemoryBytesByOrigin = new EnumMap<Origin, Long>(Origin.class);
        this.heapSize = jvmService.info().getMem().getHeapMax().getBytes();
        this.heapLimitBytes = (long) (heapSize * modelMaxSizePercentage);
        this.desiredModelSize = (long) (heapSize * modelDesiredSizePercentage);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MODEL_MAX_SIZE_PERCENTAGE, it -> this.heapLimitBytes = (long) (heapSize * it));
        this.thresholdModelBytes = 180_000;
        this.sampleSize = sampleSize;
        this.adCircuitBreakerService = adCircuitBreakerService;
    }

    /**
     * This function derives from the old code: https://tinyurl.com/2eaabja6
     *
     * @param detectorId Detector Id
     * @param rcf Random cut forest model
     * @return true if there is enough memory; otherwise throw LimitExceededException.
     */
    public synchronized boolean isHostingAllowed(String detectorId, RandomCutForest rcf) {
        long requiredBytes = estimateModelSize(rcf);
        if (canAllocateReserved(requiredBytes)) {
            return true;
        } else {
            throw new LimitExceededException(
                detectorId,
                String
                    .format(
                        Locale.ROOT,
                        "Exceeded memory limit. New size is %d bytes and max limit is %d bytes",
                        reservedMemoryBytes + requiredBytes,
                        heapLimitBytes
                    )
            );
        }
    }

    /**
     * @param requiredBytes required bytes to allocate
     * @return whether there is enough memory for the required bytes.  This is
     * true when circuit breaker is closed and there is enough reserved memory.
     */
    public synchronized boolean canAllocateReserved(long requiredBytes) {
        return (false == adCircuitBreakerService.isOpen() && reservedMemoryBytes + requiredBytes <= heapLimitBytes);
    }

    /**
     * @param bytes required bytes
     * @return whether there is enough memory for the required bytes.  This is
     * true when circuit breaker is closed and there is enough overall memory.
     */
    public synchronized boolean canAllocate(long bytes) {
        return false == adCircuitBreakerService.isOpen() && totalMemoryBytes + bytes <= heapLimitBytes;
    }

    public synchronized void consumeMemory(long memoryToConsume, boolean reserved, Origin origin) {
        totalMemoryBytes += memoryToConsume;
        adjustOriginMemoryConsumption(memoryToConsume, origin, totalMemoryBytesByOrigin);
        if (reserved) {
            reservedMemoryBytes += memoryToConsume;
            adjustOriginMemoryConsumption(memoryToConsume, origin, reservedMemoryBytesByOrigin);
        }
    }

    private void adjustOriginMemoryConsumption(long memoryToConsume, Origin origin, Map<Origin, Long> mapToUpdate) {
        Long originTotalMemoryBytes = mapToUpdate.getOrDefault(origin, 0L);
        mapToUpdate.put(origin, originTotalMemoryBytes + memoryToConsume);
    }

    public synchronized void releaseMemory(long memoryToShed, boolean reserved, Origin origin) {
        totalMemoryBytes -= memoryToShed;
        adjustOriginMemoryRelease(memoryToShed, origin, totalMemoryBytesByOrigin);
        if (reserved) {
            reservedMemoryBytes -= memoryToShed;
            adjustOriginMemoryRelease(memoryToShed, origin, reservedMemoryBytesByOrigin);
        }
    }

    private void adjustOriginMemoryRelease(long memoryToConsume, Origin origin, Map<Origin, Long> mapToUpdate) {
        Long originTotalMemoryBytes = mapToUpdate.get(origin);
        if (originTotalMemoryBytes != null) {
            mapToUpdate.put(origin, originTotalMemoryBytes - memoryToConsume);
        }
    }

    /**
     * Gets the estimated size of an entity's model.
     *
     * @param forest RCF forest object
     * @return estimated model size in bytes
     */
    public long estimateModelSize(RandomCutForest forest) {
        return estimateModelSize(forest.getDimensions(), forest.getNumberOfTrees(), forest.getSampleSize());
    }

    /**
     * Gets the estimated size of an entity's model according to
     * the detector configuration.
     *
     * @param detector detector config object
     * @param numberOfTrees the number of trees in a RCF forest
     * @return estimated model size in bytes
     */
    public long estimateModelSize(AnomalyDetector detector, int numberOfTrees) {
        return estimateModelSize(detector.getEnabledFeatureIds().size() * detector.getShingleSize(), numberOfTrees, sampleSize);
    }

    /**
     * Gets the estimated size of an entity's model.
     * RCF size:
     * (Num_trees * num_samples * ( (16*dimensions + 84) + (24*dimensions + 48))）
     *
     * (16*dimensions + 84) is for non-leaf node.  16 are for two doubles for min and max.
     * 84 is the meta-data size we observe from jmap data.
     * (24*dimensions + 48)) is for leaf node.  We find a leaf node has 3 vectors: leaf pointers,
     *  min, and max arrays from jmap data.  That’s why we use 24 ( 3 doubles).  48 is the
     *  meta-data size we observe from jmap data.
     *
     * Sampler size:
     * Number_of_trees * num_samples * ( 12 (object) + 8 (subsequence) + 8 (weight) + 8 (point reference))
     *
     * The range of mem usage of RCF model in our test(1feature, 1 shingle) is from ~400K to ~800K.
     * Using shingle size 1 and 1 feature (total dimension = 1), one rcf’s size is of 532 K,
     * which lies in our range of 400~800 k.
     *
     * @param dimension The number of feature dimensions in RCF
     * @param numberOfTrees The number of trees in RCF
     * @param numSamples The number of samples in RCF
     * @return estimated model size in bytes
     */
    public long estimateModelSize(int dimension, int numberOfTrees, int numSamples) {
        long totalSamples = (long) numberOfTrees * (long) numSamples;
        long rcfSize = totalSamples * (40 * dimension + 132);
        long samplerSize = totalSamples * 36;
        return rcfSize + samplerSize + thresholdModelBytes;
    }

    /**
     * Bytes to remove to keep AD memory usage within the limit
     * @return bytes to remove
     */
    public synchronized long memoryToShed() {
        return totalMemoryBytes - heapLimitBytes;
    }

    /**
     *
     * @return Allowed heap usage in bytes by AD models
     */
    public long getHeapLimit() {
        return heapLimitBytes;
    }

    /**
     *
     * @return Desired model partition size in bytes
     */
    public long getDesiredModelSize() {
        return desiredModelSize;
    }

    public long getTotalMemoryBytes() {
        return totalMemoryBytes;
    }

    /**
     * In case of bugs/race conditions or users dyanmically changing dedicated/shared
     * cache size, sync used bytes infrequently by recomputing memory usage.
     * @param origin Origin
     * @param totalBytes total bytes from recomputing
     * @param reservedBytes reserved bytes from recomputing
     * @return whether memory adjusted due to mismatch
     */
    public synchronized boolean syncMemoryState(Origin origin, long totalBytes, long reservedBytes) {
        long recordedTotalBytes = totalMemoryBytesByOrigin.getOrDefault(origin, 0L);
        long recordedReservedBytes = reservedMemoryBytesByOrigin.getOrDefault(origin, 0L);
        if (totalBytes == recordedTotalBytes && reservedBytes == recordedReservedBytes) {
            return false;
        }

        LOG
            .info(
                String
                    .format(
                        Locale.ROOT,
                        "Memory states do not match.  Recorded: total bytes %d, reserved bytes %d."
                            + "Actual: total bytes %d, reserved bytes: %d",
                        recordedTotalBytes,
                        recordedReservedBytes,
                        totalBytes,
                        reservedBytes
                    )
            );
        // reserved bytes mismatch
        long reservedDiff = reservedBytes - recordedReservedBytes;
        reservedMemoryBytesByOrigin.put(origin, reservedBytes);
        reservedMemoryBytes += reservedDiff;

        long totalDiff = totalBytes - recordedTotalBytes;
        totalMemoryBytesByOrigin.put(origin, totalBytes);
        totalMemoryBytes += totalDiff;
        return true;
    }

    public int getThresholdModelBytes() {
        return thresholdModelBytes;
    }
}
