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

package test.org.opensearch.ad.util;

import static java.lang.Math.PI;

import java.util.Random;

import org.joda.time.Instant;

public class LabelledAnomalyGenerator {
    /**
     * Generate labbelled multi-dimensional data
     * @param num the number of data points
     * @param period cosine periods
     * @param amplitude cosine amplitude
     * @param noise noise amplitude
     * @param seed random seed
     * @param baseDimension input dimension
     * @param useSlope whether to use slope in cosine data
     * @param historicalData the number of historical points relative to now
     * @param delta point interval
     * @return the labelled data
     */
    public static MultiDimDataWithTime getMultiDimData(
        int num,
        int period,
        double amplitude,
        double noise,
        long seed,
        int baseDimension,
        boolean useSlope,
        int historicalData,
        int delta
    ) {
        double[][] data = new double[num][];
        long[] timestamps = new long[num];
        double[][] changes = new double[num][];
        long[] changedTimestamps = new long[num];
        Random prg = new Random(seed);
        Random noiseprg = new Random(prg.nextLong());
        double[] phase = new double[baseDimension];
        double[] amp = new double[baseDimension];
        double[] slope = new double[baseDimension];

        for (int i = 0; i < baseDimension; i++) {
            phase[i] = prg.nextInt(period);
            amp[i] = (1 + 0.2 * prg.nextDouble()) * amplitude;
            if (useSlope) {
                slope[i] = (0.25 - prg.nextDouble() * 0.5) * amplitude / period;
            }
        }

        long startEpochMs = Instant.now().getMillis() - historicalData * delta;
        for (int i = 0; i < num; i++) {
            timestamps[i] = startEpochMs;
            startEpochMs += delta;
            data[i] = new double[baseDimension];
            double[] newChange = new double[baseDimension];
            for (int j = 0; j < baseDimension; j++) {
                data[i][j] = amp[j] * Math.cos(2 * PI * (i + phase[j]) / period) + slope[j] * i + noise * noiseprg.nextDouble();
                if (noiseprg.nextDouble() < 0.01 && noiseprg.nextDouble() < 0.3) {
                    double factor = 5 * (1 + noiseprg.nextDouble());
                    double change = noiseprg.nextDouble() < 0.5 ? factor * noise : -factor * noise;
                    data[i][j] += newChange[j] = change;
                    changedTimestamps[i] = timestamps[i];
                    changes[i] = newChange;
                }
            }
        }

        return new MultiDimDataWithTime(data, changedTimestamps, changes, timestamps);
    }
}
