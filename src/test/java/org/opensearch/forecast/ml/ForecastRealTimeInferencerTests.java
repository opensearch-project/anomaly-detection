/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.forecast.ml;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.forecast.caching.ForecastCacheProvider;
import org.opensearch.forecast.caching.ForecastPriorityCache;
import org.opensearch.forecast.model.Forecaster;
import org.opensearch.forecast.ratelimit.ForecastColdStartWorker;
import org.opensearch.forecast.ratelimit.ForecastSaveResultStrategy;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.ml.ModelState;
import org.opensearch.timeseries.ml.Sample;
import org.opensearch.timeseries.settings.TimeSeriesSettings;
import org.opensearch.timeseries.stats.Stats;
import org.opensearch.timeseries.stats.TimeSeriesStat;
import org.opensearch.timeseries.util.ExpiringValue;

import com.amazon.randomcutforest.parkservices.RCFCaster;

public class ForecastRealTimeInferencerTests extends OpenSearchTestCase {
    private ForecastRealTimeInferencer inferencer;
    private Clock clock;
    private Forecaster config;
    private ModelState<RCFCaster> modelState;
    private Sample sample;
    private ThreadPool threadPool;
    private ForecastModelManager modelManager;
    private Stats stats;
    private TimeSeriesStat timeSeriesStat;
    private ForecastCacheProvider cacheProvider;
    private ForecastPriorityCache cache;

    @SuppressWarnings("unchecked")
    @Before
    public void setup() {
        // Initialize clock to fixed instant
        clock = mock(Clock.class);

        threadPool = mock(ThreadPool.class);
        modelManager = mock(ForecastModelManager.class);

        // Mock the stats.getStat to return a mock TimeSeriesStat
        stats = mock(Stats.class);
        timeSeriesStat = mock(TimeSeriesStat.class);
        when(stats.getStat(anyString())).thenReturn(timeSeriesStat);

        cacheProvider = mock(ForecastCacheProvider.class);
        cache = mock(ForecastPriorityCache.class);
        when(cacheProvider.get()).thenReturn(cache);

        // Initialize inferencer with mocks or minimal implementations
        inferencer = new ForecastRealTimeInferencer(
            modelManager,
            stats,
            mock(ForecastCheckpointDao.class),
            mock(ForecastColdStartWorker.class),
            mock(ForecastSaveResultStrategy.class),
            cacheProvider,
            threadPool,
            clock,
            mock(SearchFeatureDao.class)
        );

        // Set up the Config object with an interval duration
        config = mock(Forecaster.class);
        when(config.getIntervalDuration()).thenReturn(Duration.ofSeconds(60)); // 60 seconds

        modelState = mock(ModelState.class);
        sample = mock(Sample.class);
    }

    public void testMaintenanceWithNonExpiredEntries() {
        long expirationTimeInMillis = config
            .getIntervalDuration()
            .multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ)
            .toMillis();

        String modelId = "testModelId";

        // Add entries to sampleQueues and modelLocks
        Map<String, ExpiringValue<TreeSet<Sample>>> sampleQueues = inferencer.getSampleQueues();
        Map<String, ExpiringValue<Lock>> modelLocks = inferencer.getModelLocks();

        // Create a sample queue and add to sampleQueues
        TreeSet<Sample> sampleQueue = new TreeSet<>();
        ExpiringValue<TreeSet<Sample>> expiringSampleQueue = new ExpiringValue<>(sampleQueue, expirationTimeInMillis, clock);

        sampleQueues.put(modelId, expiringSampleQueue);

        // Create a model lock and add to modelLocks
        ReentrantLock lock = new ReentrantLock();
        ExpiringValue<Lock> expiringLock = new ExpiringValue<>(lock, expirationTimeInMillis, clock);

        modelLocks.put(modelId, expiringLock);

        // Verify that entries are present before maintenance
        assertTrue(sampleQueues.containsKey(modelId));
        assertTrue(modelLocks.containsKey(modelId));

        // Call maintenance()
        inferencer.maintenance();

        // Verify that entries are still present after maintenance
        assertTrue(sampleQueues.containsKey(modelId));
        assertTrue(modelLocks.containsKey(modelId));
    }

    public void testMaintenanceWithExpiredEntries() {
        long expirationTimeInMillis = config
            .getIntervalDuration()
            .multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ)
            .toMillis();

        String modelId = "testModelId";

        // Add entries to sampleQueues and modelLocks
        Map<String, ExpiringValue<TreeSet<Sample>>> sampleQueues = inferencer.getSampleQueues();
        Map<String, ExpiringValue<Lock>> modelLocks = inferencer.getModelLocks();

        // Create a sample queue and add to sampleQueues
        TreeSet<Sample> sampleQueue = new TreeSet<>();
        ExpiringValue<TreeSet<Sample>> expiringSampleQueue = new ExpiringValue<>(sampleQueue, expirationTimeInMillis, clock);

        sampleQueues.put(modelId, expiringSampleQueue);

        // Create a model lock and add to modelLocks
        ReentrantLock lock = new ReentrantLock();
        ExpiringValue<Lock> expiringLock = new ExpiringValue<>(lock, expirationTimeInMillis, clock);

        modelLocks.put(modelId, expiringLock);

        // Verify that entries are present before maintenance
        assertTrue(sampleQueues.containsKey(modelId));
        assertTrue(modelLocks.containsKey(modelId));

        // Advance clock beyond expiration time
        when(clock.millis()).thenReturn(expirationTimeInMillis + 1);

        // Call maintenance()
        inferencer.maintenance();

        // Verify that entries have been removed after maintenance
        assertFalse(sampleQueues.containsKey(modelId));
        assertFalse(modelLocks.containsKey(modelId));
    }

    public void testProcessWithTimeout_LockNotAcquired_TimeoutReached() throws InterruptedException {
        // Set up the Config object
        when(config.getIntervalInMilliseconds()).thenReturn(60000L); // 60 seconds in milliseconds
        when(config.getWindowDelay()).thenReturn(null);

        String modelId = "testModelId";

        // Mock modelState to return the modelId
        when(modelState.getModelId()).thenReturn(modelId);

        // Mock sample to return data end time
        when(sample.getDataEndTime()).thenReturn(Instant.ofEpochMilli(1000L));

        // Create a lock that always returns false on tryLock()
        Lock lock = mock(ReentrantLock.class);
        when(lock.tryLock()).thenReturn(false);

        // Add the lock to modelLocks
        Map<String, ExpiringValue<Lock>> modelLocks = inferencer.getModelLocks();
        ExpiringValue<Lock> expiringLock = new ExpiringValue<>(
            lock,
            config.getIntervalDuration().multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ).toMillis(),
            clock
        );
        modelLocks.put(modelId, expiringLock);

        // Set clock time to simulate timeout reached
        long windowDelayMillis = 0L; // Since getWindowDelay() returns null
        long curExecutionEnd = 1000L + windowDelayMillis; // sample data end time + window delay
        long nextExecutionEnd = curExecutionEnd + config.getIntervalInMilliseconds(); // Should be 1000 + 60000 = 61000
        when(clock.millis()).thenReturn(nextExecutionEnd + 1); // Set clock.millis() to 61001 to simulate timeout

        // Call processWithTimeout
        final CountDownLatch inprogress = new CountDownLatch(1);
        AtomicBoolean result = new AtomicBoolean(true);
        inferencer.processWithTimeout(modelState, config, "taskId", sample, ActionListener.wrap(response -> {
            result.set(response);
            inprogress.countDown();
        }, exception -> {
            inprogress.countDown();
            fail("should not have exception");
        }));

        // Verify that the method returns false
        assertTrue(inprogress.await(100, TimeUnit.SECONDS));
        assertFalse(result.get());

        // Verify that threadPool.schedule is NOT called
        verify(threadPool, never()).schedule(any(Runnable.class), any(TimeValue.class), anyString());
    }

    public void testProcessWithTimeout_LockNotAcquired_ScheduleRetry() throws InterruptedException {
        // Set up the Config object
        when(config.getIntervalInMilliseconds()).thenReturn(60000L); // 60 seconds in milliseconds
        when(config.getWindowDelay()).thenReturn(null);

        String modelId = "testModelId";

        // Mock modelState to return the modelId
        when(modelState.getModelId()).thenReturn(modelId);

        // Mock sample to return data end time
        when(sample.getDataEndTime()).thenReturn(Instant.ofEpochMilli(1000L));

        // Create a lock that always returns false on tryLock()
        Lock lock = mock(ReentrantLock.class);
        when(lock.tryLock()).thenReturn(false);

        // Add the lock to modelLocks
        Map<String, ExpiringValue<Lock>> modelLocks = inferencer.getModelLocks();
        ExpiringValue<Lock> expiringLock = new ExpiringValue<>(
            lock,
            config.getIntervalDuration().multipliedBy(TimeSeriesSettings.EXPIRING_VALUE_MAINTENANCE_FREQ).toMillis(),
            clock
        );
        modelLocks.put(modelId, expiringLock);

        // Set clock time to simulate timeout not reached
        long windowDelayMillis = 0L; // Since getWindowDelay() returns null
        long curExecutionEnd = 1000L + windowDelayMillis; // sample data end time + window delay
        long nextExecutionEnd = curExecutionEnd + config.getIntervalInMilliseconds(); // Should be 1000 + 60000 = 61000
        // when(clock.millis()).thenReturn(nextExecutionEnd - 1); // Set clock.millis() to 60999 to simulate timeout not reached
        when(clock.millis()).thenReturn(
            0L,                // ExpiringValue ctor
            nextExecutionEnd - 1, // first attempt (if condition + log)
            nextExecutionEnd - 1,
            nextExecutionEnd + 1, // second attempt hits timeout branch
            nextExecutionEnd + 1
        );

        // Mock the threadPool.schedule method to capture the Runnable
        ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<TimeValue> timeValueCaptor = ArgumentCaptor.forClass(TimeValue.class);
        ArgumentCaptor<String> threadPoolNameCaptor = ArgumentCaptor.forClass(String.class);

        when(threadPool.schedule(runnableCaptor.capture(), timeValueCaptor.capture(), threadPoolNameCaptor.capture()))
            .thenReturn(mock(ScheduledCancellable.class));

        // Call processWithTimeout
        final CountDownLatch inprogress = new CountDownLatch(1);
        AtomicBoolean result = new AtomicBoolean(true);
        inferencer.processWithTimeout(modelState, config, "taskId", sample, ActionListener.wrap(response -> {
            result.set(response);
            inprogress.countDown();
        }, exception -> {
            inprogress.countDown();
            fail("should not have exception");
        }));

        // Verify that the method returns false
        runnableCaptor.getValue().run();
        assertTrue(inprogress.await(100, TimeUnit.SECONDS));
        // timeout reached, not retrying
        assertFalse(result.get());

        // Verify that threadPool.schedule is called
        verify(threadPool, times(1)).schedule(any(Runnable.class), any(TimeValue.class), anyString());

        // Verify that the scheduled Runnable is correct
        Runnable scheduledRunnable = runnableCaptor.getValue();
        assertNotNull(scheduledRunnable);

        // Verify that the scheduled time is 1 second
        TimeValue scheduledTimeValue = timeValueCaptor.getValue();
        assertEquals(1, scheduledTimeValue.seconds());
    }
}
