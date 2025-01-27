/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.rest.handler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.opensearch.commons.authuser.User;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.timeseries.TestHelpers;
import org.opensearch.timeseries.feature.SearchFeatureDao;
import org.opensearch.timeseries.model.Config;
import org.opensearch.timeseries.model.IntervalTimeConfiguration;
import org.opensearch.timeseries.transport.SuggestConfigParamResponse;

/**
 * Covers every public-method branch of HistorySuggest#suggestHistory.
 */
public class HistorySuggestTests extends OpenSearchTestCase {

    private Config mockConfig;
    private User mockUser;
    private SearchFeatureDao mockDao;
    private IntervalTimeConfiguration mockIntervalCfg;
    private Clock fixedClock;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockConfig = mock(Config.class);
        mockUser = TestHelpers.randomUser();
        mockDao = mock(SearchFeatureDao.class);
        mockIntervalCfg = mock(IntervalTimeConfiguration.class);

        // deterministic "now"
        fixedClock = Clock.fixed(Instant.now(), ZoneOffset.UTC);
    }

    /* ------------------------------------------------------------------
       intervalRecommended == null  →  early return w/ defaultHistory
       ------------------------------------------------------------------ */
    public void testEarlyReturn_withDefaultHistory() {
        when(mockConfig.getDefaultHistory()).thenReturn(17);   // arbitrary

        HistorySuggest hs = new HistorySuggest(
            mockConfig, mockUser, mockDao,
            /*intervalRecommended*/ null,                      // <-- key
            Map.of(), fixedClock
        );

        @SuppressWarnings("unchecked")
        ActionListener<SuggestConfigParamResponse> listener = mock(ActionListener.class);

        hs.suggestHistory(listener);

        var cap = org.mockito.ArgumentCaptor.forClass(SuggestConfigParamResponse.class);
        verify(listener).onResponse(cap.capture());
        assertEquals(17, cap.getValue().getHistory().intValue());
    }

    /* ------------------------------------------------------------------
       DAO returns a *future* maxEpoch  →  code clamps it to clock.millis()
       ------------------------------------------------------------------ */
    public void testFutureMaxEpoch_isClampedToNow() {
        // Arrange the DAO so maxEpoch is 1 min in the future.
        long now = fixedClock.millis();
        long start = now - 120_000;       // 2 min ago
        long future = now + 60_000;        // 1 min ahead

        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Pair<Long, Long>> l = (ActionListener<Pair<Long, Long>>) inv.getArgument(3);
            l.onResponse(Pair.of(start, future));
            return null;
        }).when(mockDao).getDateRangeOfSourceData(any(), any(), any(), any());

        // 1-minute interval
        when(mockIntervalCfg.toDuration()).thenReturn(Duration.ofMinutes(1));
        when(mockConfig.getDefaultHistory()).thenReturn(0);

        HistorySuggest hs = new HistorySuggest(
            mockConfig,
            mockUser,
            mockDao,
            mockIntervalCfg,                      // intervalRecommendation present
            Map.of(),
            fixedClock
        );

        @SuppressWarnings("unchecked")
        ActionListener<SuggestConfigParamResponse> listener = mock(ActionListener.class);

        hs.suggestHistory(listener);

        var cap = org.mockito.ArgumentCaptor.forClass(SuggestConfigParamResponse.class);
        verify(listener).onResponse(cap.capture());

        // (now - start) == 120 000 ms → 2 full one-minute intervals
        assertEquals(2, cap.getValue().getHistory().intValue());
    }

    /* ------------------------------------------------------------------
       Range == 0  →  history == 0
       ------------------------------------------------------------------ */
    public void testZeroOrNegativeRange_returnsZeroHistory() {
        long epoch = fixedClock.millis();
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Pair<Long, Long>> l = (ActionListener<Pair<Long, Long>>) inv.getArgument(3);
            l.onResponse(Pair.of(epoch, epoch));
            return null;
        }).when(mockDao).getDateRangeOfSourceData(any(), any(), any(), any());

        when(mockIntervalCfg.toDuration()).thenReturn(Duration.ofMinutes(1));
        when(mockConfig.getDefaultHistory()).thenReturn(0);

        HistorySuggest hs = new HistorySuggest(mockConfig, mockUser, mockDao, mockIntervalCfg, Map.of(), fixedClock);

        @SuppressWarnings("unchecked")
        ActionListener<SuggestConfigParamResponse> listener = mock(ActionListener.class);

        hs.suggestHistory(listener);

        var cap = org.mockito.ArgumentCaptor.forClass(SuggestConfigParamResponse.class);
        verify(listener).onResponse(cap.capture());
        assertEquals(0, cap.getValue().getHistory().intValue());
    }

    /* ------------------------------------------------------------------
       Zero-duration interval  →  IllegalArgumentException via onFailure
       ------------------------------------------------------------------ */
    public void testNonPositiveInterval_raisesIllegalArgumentException() {
        long start = fixedClock.millis() - 10_000;
        long end = fixedClock.millis();

        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<Pair<Long, Long>> l = (ActionListener<Pair<Long, Long>>) inv.getArgument(3);
            l.onResponse(Pair.of(start, end));
            return null;
        }).when(mockDao).getDateRangeOfSourceData(any(), any(), any(), any());

        when(mockIntervalCfg.toDuration()).thenReturn(Duration.ZERO);
        when(mockConfig.getDefaultHistory()).thenReturn(1);

        HistorySuggest hs = new HistorySuggest(mockConfig, mockUser, mockDao, mockIntervalCfg, Map.of(), fixedClock);

        @SuppressWarnings("unchecked")
        ActionListener<SuggestConfigParamResponse> listener = mock(ActionListener.class);

        hs.suggestHistory(listener);

        var cap = org.mockito.ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(cap.capture());
        assertTrue(cap.getValue() instanceof IllegalArgumentException);
        assertEquals("Interval duration must be positive", cap.getValue().getMessage());
    }
}
