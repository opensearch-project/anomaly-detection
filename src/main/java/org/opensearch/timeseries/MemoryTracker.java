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

package org.opensearch.timeseries;

import static org.opensearch.ad.settings.AnomalyDetectorSettings.AD_MODEL_MAX_SIZE_PERCENTAGE;

import java.util.EnumMap;
import java.util.Locale;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.timeseries.breaker.CircuitBreakerService;
import org.opensearch.timeseries.common.exception.LimitExceededException;

import com.amazon.randomcutforest.RandomCutForest;
import com.amazon.randomcutforest.parkservices.RCFCaster;
import com.amazon.randomcutforest.parkservices.ThresholdedRandomCutForest;

/**
 * Responsible for tracking and managing the memory consumption related to OpenSearch time series analysis.
 * It offers functionalities to:
 * - Track the total memory consumption and consumption per specific origin.
 * - Monitor reserved memory bytes.
 * - Decide if memory can be allocated based on the current usage and the heap limit.
 * - Estimate the memory size for a ThresholdedRandomCutForest model based on various parameters.
 *
 */
public class MemoryTracker {
    private static final Logger LOG = LogManager.getLogger(MemoryTracker.class);

    public enum Origin {
        REAL_TIME_DETECTOR,
        HISTORICAL_SINGLE_ENTITY_DETECTOR,
        REAL_TIME_FORECASTER
    }

    // memory tracker for total consumption of bytes
    protected long totalMemoryBytes;
    protected final Map<Origin, Long> totalMemoryBytesByOrigin;
    // reserved for models. Cannot be deleted at will.
    protected long reservedMemoryBytes;
    protected final Map<Origin, Long> reservedMemoryBytesByOrigin;
    protected long heapSize;
    protected long heapLimitBytes;
    // we observe threshold model uses a fixed size array and the size is the same
    protected int thresholdModelBytes;
    protected CircuitBreakerService timeSeriesCircuitBreakerService;

    /**
     * Constructor
     *
     * @param jvmService Service providing jvm info
     * @param modelMaxSizePercentage Percentage of heap for the max size of a model
     * @param clusterService Cluster service object
     * @param timeSeriesCircuitBreakerService Memory circuit breaker
     */
    public MemoryTracker(
        JvmService jvmService,
        double modelMaxSizePercentage,
        ClusterService clusterService,
        CircuitBreakerService timeSeriesCircuitBreakerService
    ) {
        this.totalMemoryBytes = 0;
        this.totalMemoryBytesByOrigin = new EnumMap<Origin, Long>(Origin.class);
        this.reservedMemoryBytes = 0;
        this.reservedMemoryBytesByOrigin = new EnumMap<Origin, Long>(Origin.class);
        this.heapSize = jvmService.info().getMem().getHeapMax().getBytes();
        this.heapLimitBytes = (long) (heapSize * modelMaxSizePercentage);
        if (clusterService != null) {
            clusterService
                .getClusterSettings()
                .addSettingsUpdateConsumer(AD_MODEL_MAX_SIZE_PERCENTAGE, it -> this.heapLimitBytes = (long) (heapSize * it));
        }

        this.thresholdModelBytes = 180_000;
        this.timeSeriesCircuitBreakerService = timeSeriesCircuitBreakerService;
    }

    /**
     * @param requiredBytes required bytes to allocate
     * @return whether there is enough memory for the required bytes.  This is
     * true when circuit breaker is closed and there is enough reserved memory.
     */
    public synchronized boolean canAllocateReserved(long requiredBytes) {
        return (false == timeSeriesCircuitBreakerService.isOpen() && reservedMemoryBytes + requiredBytes <= heapLimitBytes);
    }

    /**
     * @param bytes required bytes
     * @return whether there is enough memory for the required bytes.  This is
     * true when circuit breaker is closed and there is enough overall memory.
     */
    public synchronized boolean canAllocate(long bytes) {
        return false == timeSeriesCircuitBreakerService.isOpen() && totalMemoryBytes + bytes <= heapLimitBytes;
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
     * Gets the estimated size (bytes) of a TRCF model.
     *
     * RCF size:
     * I measured the memory size of a ThresholdedRandomCutForest/RCFCaster using heap dump.
     * Internal shingling is required. A ThresholdedRandomCutForest/RCFCaster comprises rcf
     * model, threshold model, and other fields.
     *
     * Symbols:
     * b   base dimension
     * s   shingle size
     * d   dimension = b * s
     * r   {@code point store constant = IF(s*0.05>1,s*0.05,IF(s>1,1.5,1)}
     * t   # trees
     * ss  sample size
     * br  bounding box ratio
     * c   max capacity of point store max(sampleSize * numberOfTrees + 1, 2 * sampleSize), defined in RandomCutForest
     * pt  {@code location list constant = 2 or 4. If shingleSize * capacity < Character.MAX_VALUE use PointStoreSmall, * 2; otherwise use PointStoreLarge, *4}
     * be  {@code br > 0}
     * ci  number of internal nodes = ss - 1
     * nt  {@code node store type = ci<256&&d<=256  => NodeStoreSmall
                             ci<65535&&d<=65535 => NodeStoreMedium
                             otherwise => NodeStoreLarge}
     * ns  {@code node store size = IF(AND(ci<256,d<=256),10*ss + 208,IF(AND(ci<65535,d<=65535),16*s + 202,20*s + 198))} (direct translation of RCF logic)
     *
     * A compact RCF forest consists of:
     * - Random number generator: 56 bytes
     * - PointStoreCoordinator: 24 bytes
     * - SequentialForestUpdateExecutor: 24 bytes
     * - SequentialForestTraversalExecutor: 16 bytes
     * + PointStoreFloat: ss * t * 4 * r * b+ss * t *  pt+ss * t +1776+48+s*4
     *   - IndexManager + HashMap+shingleSize*4: 1776+48+shingleSize*4 bytes
     *   - refCount: ss * trees * 1 (*1 since refCount is of byte[])
     *   - locationList: ss * trees *  pt
     *   - a float array for data store: ss * # trees * 4 bytes * point store constant * b,
     *     where ss * # trees is the maximum allowed points in the forest;  * 4 since float is of 4 bytes.
     *     since internal shingling is enabled, we don't use all of the array and need to multiply by
     *     some factor to account for saved space.
     *
     *     The average usage percentage depends on shingle size and if internal shingling is enabled.
     *     I did experiments with power-of-two shingle sizes and internal shingling on/off
     *     by running ThresholdedRandomCutForest over a million points.
     *     My experiment shows that
     *     - if internal shingling is off, data store is filled at full
     *     capacity.
     *     - otherwise, data store usage depends on shingle size:
     *       {@code IF(s*0.05>1,s*0.05,IF(s>1,1.5,1) }
     *
     * - ComponentList: an array of size numberOfTrees = (2*(ss* 4 + 16)+80+88 + (ns+128+(br*ss*d*8+16*be) + (br*ss*8+16*be)) + 24)  * t
     *   + SamplerPlusTree = (2*(ss* 4 + 16)+80+88 + (ns+128+(br*ss*d*8+16*be) + (br*ss*8+16*be)) + 24)
     *    - other: 24
     *    + CompactSampler: 2*(ss* 4 + 16)+80+88
     *      - weight: sample size* 4 + 16
     *      - pointIndex: sample size* 4 + 16
     *      - evictedPoint: 48
     *      - random: 32
     *    + RandomCutTree: ns+128+(br*ss*d*8+16*be) + (br*ss*8+16*be)
     *      - other fields: 80
     *      - leafMass: 48
     *      + NodeStore (ss-1)*4+20+(ss-1)*2+18+(ss-1)*2+18+(ss-1)+17+(ss-1)+17+80+48
     *        - cutValue: (sample size-1)*4+20
     *        - freeNodeManager: 80
     *                        The following fields are organized on node store type
     *                        NodeStoreSmall           NodeStoreMedium         NodeStoreLarge
     *        - leftIndex    (sample size-1)*2+18    (sample size-1)*4+18    (sample size-1)*4+18
     *        - rightIndex   (sample size-1)*2+18    (sample size-1)*4+18    (sample size-1)*4+18
     *        - cutDimension (sample size-1)+17      (sample size-1)*2+17    (sample size-1)*4+17
     *        - mass         (sample size-1)+17      (sample size-1)*2+17    (sample size-1)*4+17
     *      + BoxCacheFloat
     *        - other: 104
     *        - BoundingBoxFloat: {@code bounding box ratio * ss * dimension* 2 * 4 + (bounding box ratio > 0 ? 16 : 0) }
     *        - rangeSumData: {@code br * ss * 8 + (bounding box ratio > 0 ? 16 : 0) }
     *
     *
     *  Thresholder size
     *    + Preprocessor: 280+d*8+16+24+280+72+3*(d*8 + 16)+16+128
     *      + transformer: 280+dimension*8+16+24 (24 is object size)
     *        - deviations = 280
     *        - weights = dimension*8+16
     *      - timeStampDeviations = 280
     *      - dataQuality = 72
     *      - lastShingledInput, previousTimeStamps and lastShingledPoint = 3*(dimension*8 + 16) (3 due to 2 double arrays, 16 are array object size)
     *      - stopNormalization = 16
     *      - other: 128
     *  + PredictorCorrector: 472+4*(8*d+16)+184+(48*b+24)*2+32+96+104
     *    - thresholders: 472
     *    + lastDescriptor: 4*(8*dimension+16)+184
     *      - currentInput: 8*dimension+16(16 is array size)
     *      - RCFPoint: 8*dimension+16(16 is array size)
     *      - shift: 8*dimension+16(16 is array size)
     *      - scale: 8*dimension+16(16 is array size)
     *      - other: 184
     *    - deviationsActual: 48*base dimension+24 (24 is object size)
     *    - deviationsExpected: 48*base dimension+24 (24 is object size)
     *    - lastScore: 32
     *    - 4 ignores array: 96
     *  + lastAnomalyDescriptor: 24 + (b * 8+16)*7
     *    - attribution: 2 * (b * 8+16) + 24
     *      - high: basic dimension * 8+16
     *      - low: basic dimension * 8+16
     *    - currentInput: basic dimension * 8+16
     *    - RCFPoint: d * 8+16
     *    - scale: basic dimension * 8+16
     *    - shift: basic dimension * 8+16
     *    - postShift: basic dimension * 8+16
     *
     *  Total: 152*b + 4*d*r*ss*t + 64*d + pt*ss*t + 4*s + ss*t + t*(32*be + 8*br*d*ss + 8*br*ss + ns + 8*ss + 352) + 3944
     *
     * @param dimension The number of feature dimensions in RCF
     * @param numberOfTrees The number of trees in RCF
     * @param boundingBoxCacheFraction Bounding box cache usage in RCF
     * @param shingleSize shingle size
     * @param sampleSize sample size
     * @return estimated TRCF model size
     *
     */
    public long estimateTRCFModelSize(int dimension, int numberOfTrees, double boundingBoxCacheFraction, int shingleSize, int sampleSize) {
        double baseDimension = dimension / shingleSize;
        // rounding it up to the next power of two, in terms of selecting the pointStoreSizeConstant. T
        double pointStoreSizeConstant = 1;
        if (shingleSize == 1) {
            pointStoreSizeConstant = 1;
        } else if (shingleSize == 2) {
            pointStoreSizeConstant = 0.53;
        } else if (shingleSize <= 4) {
            pointStoreSizeConstant = 0.27;
        } else if (shingleSize <= 8) {
            pointStoreSizeConstant = 0.18;
        } else if (shingleSize <= 16) {
            pointStoreSizeConstant = 0.13;
        } else if (shingleSize <= 32) {
            pointStoreSizeConstant = 0.07;
        } else if (shingleSize <= 64) {
            pointStoreSizeConstant = 0.05;
        } else if (shingleSize <= 128) {
            pointStoreSizeConstant = 0.05;
        } else {
            throw new IllegalArgumentException("out of range shingle size " + shingleSize);
        }

        int pointStoreCapacity = Math.max(sampleSize * numberOfTrees + 1, 2 * sampleSize);
        int pointStoreTypeConstant = shingleSize * pointStoreCapacity >= Character.MAX_VALUE ? 4 : 2;
        int boundingBoxExistsConstant = boundingBoxCacheFraction > 0 ? 1 : 0;

        int nodeStoreSize = 0;
        int numberOfInternalNodes = sampleSize - 1;
        if (numberOfInternalNodes < 256 && dimension <= 256) {
            // NodeStoreSmall
            nodeStoreSize = 10 * sampleSize + 208;
        } else if (numberOfInternalNodes < 65535 && dimension <= 65535) {
            // NodeStoreMedium
            nodeStoreSize = 16 * sampleSize + 202;
        } else {
            // NodeStoreLarge
            nodeStoreSize = 20 * sampleSize + 198;
        }
        // NodeStoreLarge
        return (long) (152 * baseDimension + 4 * dimension * pointStoreSizeConstant * sampleSize * numberOfTrees + 64 * dimension
            + pointStoreTypeConstant * sampleSize * numberOfTrees + 4 * shingleSize + sampleSize * numberOfTrees + numberOfTrees * (32
                * boundingBoxExistsConstant + 8 * boundingBoxCacheFraction * dimension * sampleSize + 8 * boundingBoxCacheFraction
                    * sampleSize + nodeStoreSize + 8 * sampleSize + 352) + 3944);
    }

    /**
     * Gets the estimated size (bytes) of a RCFCaster model. On top of trcf model, RCFCaster adds an ErrorHandler.
     *
     * Symbols:
     * b   base dimension
     * h   horizon
     *
     * ErrorHandler size:
     *   - pastForecasts: h*(3*(l*4+16)+24), h RangeVector, we have 3 float array in RangeVector,
     *    and each float array is of size l, 16 is float array object size, 24 is RangeVector object size
     *   - rmseLowDeviations: l * 48 + 784 , l Deviation, each Deviation is of size 48, 784 is Deviation array size
     *   - rmseHighDeviations: l * 48 + 784, similar to   rmseLowDeviations
     * intervalPrecision l * 48 + 784    similar to   rmseLowDeviations
     * errorRMSE 2*(l*8+14)+24   2 double array of size l, plus 14 bytes for each array object; 24 is DiVector object size
     * errorDistribution (3*(l*4+16)+24) Similar to  pastForecasts, with only 1 RangeVector
     * errorMean 4*l+16  a float array of size l, plus array object size 16
     * lastInputs    8*2*b+16    a double array of size 2*b, plus double array object size 16
     * lastDataDeviations    4*b+16  a float array of size b, plus array object size 16
     * upperLimit    4*b+16  similar to  lastDataDeviations
     * lowerLimit    4*b+16  similar to  lastDataDeviations
     *
     * Total: 176*b*h + 28*b + 12*h*(b*h + 6) + 2556
     *
     * @param dimension The number of feature dimensions in RCF
     * @param numberOfTrees The number of trees in RCF
     * @param boundingBoxCacheFraction Bounding box cache usage in RCF
     * @param shingleSize shingle size
     * @param sampleSize sample size
     * @param horizon Forecast horizon
     * @return estimated RCFCaster model size
     */
    public long estimateCasterModelSize(
        int dimension,
        int numberOfTrees,
        double boundingBoxCacheFraction,
        int shingleSize,
        int sampleSize,
        int horizon
    ) {
        long trcfModelSize = estimateTRCFModelSize(dimension, numberOfTrees, boundingBoxCacheFraction, shingleSize, sampleSize);
        double baseDimension = dimension / shingleSize;
        double errorHandlerSize = 176 * baseDimension * horizon + 28 * baseDimension + 12 * horizon * (baseDimension * horizon + 6) + 2556;
        return (long) (trcfModelSize + errorHandlerSize);
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
                            + " Actual: total bytes %d, reserved bytes: %d",
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

    /**
     * Determines if hosting is allowed based on the estimated size of a given ThresholdedRandomCutForest and
     * the available memory resources.
     *
     * <p>This method synchronizes access to ensure that checks and operations related to resource availability
     * are thread-safe.
     *
     * @param configId      The identifier for the configuration being checked. Used in error messages.
     * @param trcf          The ThresholdedRandomCutForest to estimate the size for.
     * @return              True if the system can allocate the required bytes to host the trcf.
     * @throws LimitExceededException If the required memory for the trcf exceeds the available memory.
     *
     * <p>Usage example:
     * <pre>{@code
     * boolean canHost = isHostingAllowed("config123", myTRCF);
     * }</pre>
     */
    public synchronized boolean isHostingAllowed(String configId, ThresholdedRandomCutForest trcf) {
        long requiredBytes = estimateTRCFModelSize(trcf);
        if (canAllocateReserved(requiredBytes)) {
            return true;
        } else {
            throw new LimitExceededException(
                configId,
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
     * Gets the estimated size (bytes) of a TRCF model.
     *
     * @param trcf ThresholdedRandomCutForest object
     * @return estimated model size in bytes
     */
    public long estimateTRCFModelSize(ThresholdedRandomCutForest trcf) {
        RandomCutForest forest = trcf.getForest();
        return estimateTRCFModelSize(
            forest.getDimensions(),
            forest.getNumberOfTrees(),
            forest.getBoundingBoxCacheFraction(),
            forest.getShingleSize(),
            forest.getSampleSize()
        );
    }

    /**
     * Gets the estimated size (bytes) of a RCFCaster model.
     *
     * @param caster RCFCaster object
     * @return estimated model size in bytes
     */
    public long estimateCasterModelSize(RCFCaster caster) {
        RandomCutForest forest = caster.getForest();
        return estimateCasterModelSize(
            forest.getDimensions(),
            forest.getNumberOfTrees(),
            forest.getBoundingBoxCacheFraction(),
            forest.getShingleSize(),
            forest.getSampleSize(),
            caster.getForecastHorizon()
        );
    }
}
