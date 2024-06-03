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

package org.opensearch.timeseries.feature;

import static java.util.Arrays.copyOfRange;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ThreadedActionListener;
import org.opensearch.ad.model.AnomalyDetector;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.AnalysisType;
import org.opensearch.timeseries.TimeSeriesAnalyticsPlugin;
import org.opensearch.timeseries.common.exception.EndRunException;
import org.opensearch.timeseries.constant.CommonMessages;
import org.opensearch.timeseries.dataprocessor.Imputer;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.Entity;

/**
 * A facade managing feature data operations and buffers.
 */
public class FeatureManager {

    private static final Logger logger = LogManager.getLogger(FeatureManager.class);

    private final SearchFeatureDao searchFeatureDao;
    public final Imputer imputer;

    private final int trainSampleTimeRangeInHours;
    private final int minTrainSamples;
    private final double maxMissingPointsRate;
    private final int maxNeighborDistance;
    private final double previewSampleRate;
    private final int maxPreviewSamples;
    private final ThreadPool threadPool;

    /**
     * Constructor with dependencies and configuration.
     *
     * @param searchFeatureDao DAO of features from search
     * @param imputer imputer of samples
     * @param trainSampleTimeRangeInHours time range in hours for collect train samples
     * @param minTrainSamples min number of train samples
     * @param maxMissingPointsRate max proportion of shingle with missing points allowed to generate a shingle
     * @param maxNeighborDistance max distance (number of intervals) between a missing point and a replacement neighbor
     * @param previewSampleRate number of samples to number of all the data points in the preview time range
     * @param maxPreviewSamples max number of samples from search for preview features
     * @param threadPool object through which we can invoke different threadpool using different names
     */
    public FeatureManager(
        SearchFeatureDao searchFeatureDao,
        Imputer imputer,
        int trainSampleTimeRangeInHours,
        int minTrainSamples,
        double maxMissingPointsRate,
        int maxNeighborDistance,
        double previewSampleRate,
        int maxPreviewSamples,
        ThreadPool threadPool
    ) {
        this.searchFeatureDao = searchFeatureDao;
        this.imputer = imputer;
        this.trainSampleTimeRangeInHours = trainSampleTimeRangeInHours;
        this.minTrainSamples = minTrainSamples;
        this.maxMissingPointsRate = maxMissingPointsRate;
        this.maxNeighborDistance = maxNeighborDistance;
        this.previewSampleRate = previewSampleRate;
        this.maxPreviewSamples = maxPreviewSamples;

        this.threadPool = threadPool;
    }

    /**
     * Get feature array within one interval
     * @param config Config accessor
     * @param startTime data start time in milliseconds
     * @param endTime data end time in milliseconds
     * @param context Whether the config is AnomalyDetector or Forecaster
     * @param listener return back the data point
     */
    public void getCurrentFeatures(
        Config config,
        long startTime,
        long endTime,
        AnalysisType context,
        ActionListener<Optional<double[]>> listener
    ) {
        List<Entry<Long, Long>> missingRanges = Collections.singletonList(new SimpleImmutableEntry<>(startTime, endTime));
        try {
            searchFeatureDao.getFeatureSamplesForPeriods(config, missingRanges, context, ActionListener.wrap(points -> {
                // we only have one point
                if (points.size() == 1) {
                    Optional<double[]> point = points.get(0);
                    listener.onResponse(point);
                } else {
                    listener.onResponse(Optional.empty());
                }
            }, listener::onFailure));
        } catch (IOException e) {
            listener.onFailure(new EndRunException(config.getId(), CommonMessages.INVALID_SEARCH_QUERY_MSG, e, true));
        }
    }

    /**
     * Returns to listener data for cold-start training. Used in AD single-stream.
     *
     * Training data starts with getting samples from (costly) search.
     * Samples are increased in dimension via shingling.
     *
     * @param detector contains data info (indices, documents, etc)
     * @param listener onResponse is called with data for cold-start training, or empty if unavailable
     *                 onFailure is called with EndRunException on feature query creation errors
     */
    public void getColdStartData(AnomalyDetector detector, ActionListener<Optional<double[][]>> listener) {
        ActionListener<Optional<Long>> latestTimeListener = ActionListener
            .wrap(latest -> getColdStartSamples(latest, detector, AnalysisType.AD, listener), listener::onFailure);
        searchFeatureDao
            .getLatestDataTime(
                detector,
                Optional.empty(),
                AnalysisType.AD,
                new ThreadedActionListener<>(logger, threadPool, TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME, latestTimeListener, false)
            );
    }

    private void getColdStartSamples(
        Optional<Long> latest,
        Config config,
        AnalysisType context,
        ActionListener<Optional<double[][]>> listener
    ) {
        int shingleSize = config.getShingleSize();
        if (latest.isPresent()) {
            List<Entry<Long, Long>> sampleRanges = getColdStartSampleRanges(config, latest.get());
            try {
                ActionListener<List<Optional<double[]>>> getFeaturesListener = ActionListener
                    .wrap(samples -> processColdStartSamples(samples, shingleSize, listener), listener::onFailure);
                searchFeatureDao
                    .getFeatureSamplesForPeriods(
                        config,
                        sampleRanges,
                        context,
                        new ThreadedActionListener<>(
                            logger,
                            threadPool,
                            TimeSeriesAnalyticsPlugin.AD_THREAD_POOL_NAME,
                            getFeaturesListener,
                            false
                        )
                    );
            } catch (IOException e) {
                listener.onFailure(new EndRunException(config.getId(), CommonMessages.INVALID_SEARCH_QUERY_MSG, e, true));
            }
        } else {
            listener.onResponse(Optional.empty());
        }
    }

    private void processColdStartSamples(List<Optional<double[]>> samples, int shingleSize, ActionListener<Optional<double[][]>> listener) {
        List<double[]> shingles = new ArrayList<>();
        LinkedList<Optional<double[]>> currentShingle = new LinkedList<>();
        for (Optional<double[]> sample : samples) {
            currentShingle.addLast(sample);
            if (currentShingle.size() == shingleSize) {
                sample.ifPresent(s -> fillAndShingle(currentShingle, shingleSize).ifPresent(shingles::add));
                currentShingle.remove();
            }
        }
        listener.onResponse(Optional.of(shingles.toArray(new double[0][0])).filter(results -> results.length > 0));
    }

    private Optional<double[]> fillAndShingle(LinkedList<Optional<double[]>> shingle, int shingleSize) {
        Optional<double[]> result = null;
        if (shingle.stream().filter(s -> s.isPresent()).count() >= shingleSize - getMaxMissingPoints(shingleSize)) {
            TreeMap<Integer, double[]> search = new TreeMap<>(
                IntStream
                    .range(0, shingleSize)
                    .filter(i -> shingle.get(i).isPresent())
                    .boxed()
                    .collect(Collectors.toMap(i -> i, i -> shingle.get(i).get()))
            );
            result = Optional.of(IntStream.range(0, shingleSize).mapToObj(i -> {
                Optional<Entry<Integer, double[]>> after = Optional.ofNullable(search.ceilingEntry(i));
                Optional<Entry<Integer, double[]>> before = Optional.ofNullable(search.floorEntry(i));
                return after
                    .filter(a -> Math.abs(i - a.getKey()) <= before.map(b -> Math.abs(i - b.getKey())).orElse(Integer.MAX_VALUE))
                    .map(Optional::of)
                    .orElse(before)
                    .filter(e -> Math.abs(i - e.getKey()) <= maxNeighborDistance)
                    .map(Entry::getValue)
                    .orElse(null);
            }).filter(d -> d != null).toArray(double[][]::new))
                .filter(d -> d.length == shingleSize)
                .map(d -> batchShingle(d, shingleSize)[0]);
        } else {
            result = Optional.empty();
        }
        return result;
    }

    private List<Entry<Long, Long>> getColdStartSampleRanges(Config detector, long endMillis) {
        long interval = detector.getIntervalInMilliseconds();
        int numSamples = Math.max((int) (Duration.ofHours(this.trainSampleTimeRangeInHours).toMillis() / interval), this.minTrainSamples);
        return IntStream
            .rangeClosed(1, numSamples)
            .mapToObj(i -> new SimpleImmutableEntry<>(endMillis - (numSamples - i + 1) * interval, endMillis - (numSamples - i) * interval))
            .collect(Collectors.toList());
    }

    /**
     * Shingles a batch of data points by concatenating neighboring data points.
     *
     * @param points M, N where M is the number of data points and N is the dimension of a point
     * @param shingleSize the size of a shingle
     * @return P, Q where P = M - {@code shingleSize} + 1 and Q = N * {@code shingleSize}
     * @throws IllegalArgumentException when input is invalid
     */
    public double[][] batchShingle(double[][] points, int shingleSize) {
        if (points.length == 0 || points[0].length == 0 || points.length < shingleSize || shingleSize < 1) {
            throw new IllegalArgumentException("Invalid data for shingling.");
        }
        int numPoints = points.length;
        int dimPoint = points[0].length;
        int numShingles = numPoints - shingleSize + 1;
        int dimShingle = dimPoint * shingleSize;
        double[][] shingles = new double[numShingles][dimShingle];
        for (int i = 0; i < numShingles; i++) {
            for (int j = 0; j < shingleSize; j++) {
                System.arraycopy(points[i + j], 0, shingles[i], j * dimPoint, dimPoint);
            }
        }
        return shingles;
    }

    /**
     * Returns the entities for preview to listener
     * @param detector detector config
     * @param startTime start of the range in epoch milliseconds
     * @param endTime end of the range in epoch milliseconds
     * @param listener onResponse is called when entities are found
     */
    public void getPreviewEntities(AnomalyDetector detector, long startTime, long endTime, ActionListener<List<Entity>> listener) {
        searchFeatureDao.getHighestCountEntities(detector, startTime, endTime, listener);
    }

    /**
     * Returns to listener feature data points (unprocessed and processed) from the period for preview purpose for specific entity.
     *
     * Due to the constraints (workload, latency) from preview, a small number of data samples are from actual
     * query results and the remaining are from interpolation. The results are approximate to the actual features.
     *
     * @param detector detector info containing indices, features, interval, etc
     * @param entity entity specified
     * @param startMilli start of the range in epoch milliseconds
     * @param endMilli end of the range in epoch milliseconds
     * @param listener onResponse is called with time ranges, unprocessed features,
     *                                      and processed features of the data points from the period
     *                 onFailure is called with IllegalArgumentException when there is no data to preview
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    public void getPreviewFeaturesForEntity(
        AnomalyDetector detector,
        Entity entity,
        long startMilli,
        long endMilli,
        ActionListener<Features> listener
    ) throws IOException {
        // TODO refactor this common lines so that these code can be run for 1 time for all entities
        Entry<List<Entry<Long, Long>>, Integer> sampleRangeResults = getSampleRanges(detector, startMilli, endMilli);
        List<Entry<Long, Long>> sampleRanges = sampleRangeResults.getKey();
        int stride = sampleRangeResults.getValue();
        int shingleSize = detector.getShingleSize();

        getPreviewSamplesInRangesForEntity(detector, sampleRanges, entity, getFeatureSamplesListener(stride, shingleSize, listener));
    }

    private ActionListener<Entry<List<Entry<Long, Long>>, double[][]>> getFeatureSamplesListener(
        int stride,
        int shingleSize,
        ActionListener<Features> listener
    ) {
        return ActionListener.wrap(samples -> {
            List<Entry<Long, Long>> searchTimeRange = samples.getKey();
            if (searchTimeRange.size() == 0) {
                listener.onFailure(new IllegalArgumentException("No data to preview anomaly detection."));
                return;
            }
            double[][] sampleFeatures = samples.getValue();
            List<Entry<Long, Long>> previewRanges = getPreviewRanges(searchTimeRange, stride);
            double[][] previewFeatures = getPreviewFeatures(sampleFeatures, stride);
            listener.onResponse(new Features(previewRanges, previewFeatures));
        }, listener::onFailure);
    }

    /**
     * Returns to listener feature data points (unprocessed and processed) from the period for preview purpose.
     *
     * Due to the constraints (workload, latency) from preview, a small number of data samples are from actual
     * query results and the remaining are from interpolation. The results are approximate to the actual features.
     *
     * @param detector detector info containing indices, features, interval, etc
     * @param startMilli start of the range in epoch milliseconds
     * @param endMilli end of the range in epoch milliseconds
     * @param listener onResponse is called with time ranges, unprocessed features,
     *                                      and processed features of the data points from the period
     *                 onFailure is called with IllegalArgumentException when there is no data to preview
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    public void getPreviewFeatures(AnomalyDetector detector, long startMilli, long endMilli, ActionListener<Features> listener)
        throws IOException {
        Entry<List<Entry<Long, Long>>, Integer> sampleRangeResults = getSampleRanges(detector, startMilli, endMilli);
        List<Entry<Long, Long>> sampleRanges = sampleRangeResults.getKey();
        int stride = sampleRangeResults.getValue();
        int shingleSize = detector.getShingleSize();

        getSamplesForRanges(detector, sampleRanges, getFeatureSamplesListener(stride, shingleSize, listener));
    }

    /**
     * Gets time ranges of sampled data points.
     *
     * To reduce workload/latency from search, most data points in the preview time ranges are not from search results.
     * This implementation selects up to maxPreviewSamples evenly spaced points from the entire time range.
     *
     * @return key is a list of sampled time ranges, value is the stride between samples
     */
    private Entry<List<Entry<Long, Long>>, Integer> getSampleRanges(AnomalyDetector detector, long startMilli, long endMilli) {
        long start = truncateToMinute(startMilli);
        long end = truncateToMinute(endMilli);
        long bucketSize = detector.getIntervalInMilliseconds();
        int numBuckets = (int) Math.floor((end - start) / (double) bucketSize);
        int numSamples = (int) Math.max(Math.min(numBuckets * previewSampleRate, maxPreviewSamples), 1);
        int stride = (int) Math.max(1, Math.floor((double) numBuckets / numSamples));
        int numStrides = (int) Math.ceil(numBuckets / (double) stride);
        List<Entry<Long, Long>> sampleRanges = Stream
            .iterate(start, i -> i + stride * bucketSize)
            .limit(numStrides)
            .map(time -> new SimpleImmutableEntry<>(time, time + bucketSize))
            .collect(Collectors.toList());
        return new SimpleImmutableEntry<>(sampleRanges, stride);
    }

    /**
     * Gets search results in the sampled time ranges for specified entity.
     *
     * @param entity specified entity
     * @param listener handle search results map: key is time ranges, value is corresponding search results
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    void getPreviewSamplesInRangesForEntity(
        AnomalyDetector detector,
        List<Entry<Long, Long>> sampleRanges,
        Entity entity,
        ActionListener<Entry<List<Entry<Long, Long>>, double[][]>> listener
    ) throws IOException {
        searchFeatureDao
            .getColdStartSamplesForPeriods(
                detector,
                sampleRanges,
                Optional.ofNullable(entity),
                true,
                AnalysisType.AD,
                getSamplesRangesListener(sampleRanges, listener)
            );
    }

    private ActionListener<List<Optional<double[]>>> getSamplesRangesListener(
        List<Entry<Long, Long>> sampleRanges,
        ActionListener<Entry<List<Entry<Long, Long>>, double[][]>> listener
    ) {
        return ActionListener.wrap(featureSamples -> {
            List<Entry<Long, Long>> ranges = new ArrayList<>(featureSamples.size());
            List<double[]> samples = new ArrayList<>(featureSamples.size());
            for (int i = 0; i < featureSamples.size(); i++) {
                Entry<Long, Long> currentRange = sampleRanges.get(i);
                featureSamples.get(i).ifPresent(sample -> {
                    ranges.add(currentRange);
                    samples.add(sample);
                });
            }
            listener.onResponse(new SimpleImmutableEntry<>(ranges, samples.toArray(new double[0][0])));
        }, listener::onFailure);
    }

    /**
     * Gets search results for the sampled time ranges.
     *
     * @param listener handle search results map: key is time ranges, value is corresponding search results
     * @throws IOException if a user gives wrong query input when defining a detector
     */
    void getSamplesForRanges(
        AnomalyDetector detector,
        List<Entry<Long, Long>> sampleRanges,
        ActionListener<Entry<List<Entry<Long, Long>>, double[][]>> listener
    ) throws IOException {
        searchFeatureDao
            .getFeatureSamplesForPeriods(detector, sampleRanges, AnalysisType.AD, getSamplesRangesListener(sampleRanges, listener));
    }

    /**
     * Gets time ranges for the data points in the preview range that begins with the first
     * sample time range and ends with the last.
     *
     * @param ranges time ranges of samples
     * @param stride the number of data points between samples
     * @return time ranges for all data points
     */
    private List<Entry<Long, Long>> getPreviewRanges(List<Entry<Long, Long>> ranges, int stride) {
        double[] rangeStarts = ranges.stream().mapToDouble(Entry::getKey).toArray();
        double[] rangeEnds = ranges.stream().mapToDouble(Entry::getValue).toArray();
        double[] previewRangeStarts = imputer.singleFeatureImpute(rangeStarts, stride * (ranges.size() - 1) + 1);
        double[] previewRangeEnds = imputer.singleFeatureImpute(rangeEnds, stride * (ranges.size() - 1) + 1);
        List<Entry<Long, Long>> previewRanges = IntStream
            .range(0, previewRangeStarts.length)
            .mapToObj(i -> new SimpleImmutableEntry<>((long) previewRangeStarts[i], (long) previewRangeEnds[i]))
            .collect(Collectors.toList());
        return previewRanges;
    }

    /**
     * Gets unprocessed and processed features for the data points in the preview range.
     *
     * To reduce workload on search, the data points within the preview range are interpolated based on
     * sample query results. Unprocessed features are interpolated query results.
     * Processed features are inputs to models, transformed (such as shingle) from unprocessed features.
     *
     * @return unprocessed and processed features
     */
    private double[][] getPreviewFeatures(double[][] samples, int stride) {
        double[][] unprocessed = Optional
            .of(samples)
            .map(m -> imputer.impute(m, stride * (samples.length - 1) + 1))
            .map(m -> copyOfRange(m, 0, m.length))
            .get();
        return unprocessed;
    }

    private long truncateToMinute(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).truncatedTo(ChronoUnit.MINUTES).toEpochMilli();
    }

    /**
     * @return max number of missing points allowed to generate a shingle
     */
    private int getMaxMissingPoints(int shingleSize) {
        return (int) Math.floor(shingleSize * maxMissingPointsRate);
    }

    public void getFeatureDataPointsByBatch(
        AnomalyDetector detector,
        Entity entity,
        long startTime,
        long endTime,
        ActionListener<Map<Long, Optional<double[]>>> listener
    ) {
        try {
            searchFeatureDao.getFeaturesForPeriodByBatch(detector, entity, startTime, endTime, ActionListener.wrap(points -> {
                logger.debug("features size: {}", points.size());
                listener.onResponse(points);
            }, listener::onFailure));
        } catch (Exception e) {
            logger.error("Failed to get features for detector: " + detector.getId());
            listener.onFailure(e);
        }
    }

    /**
     *
     * @param endTime End time of the stream
     * @param intervalMilli interval between returned time
     * @param startTime Start time of the stream
     * @return a list of epoch timestamps from endTime with interval intervalMilli. The stream should stop when the number is earlier than startTime.
     */
    private List<Long> getFullTrainingDataEndTimes(long endTime, long intervalMilli, long startTime) {
        return LongStream
            .iterate(startTime, i -> i + intervalMilli)
            .takeWhile(i -> i <= endTime)
            .boxed() // Convert LongStream to Stream<Long>
            .collect(Collectors.toList()); // Collect to List
    }
}
