/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataUtil {
    /**
     * Removes leading rows in a 2D array that contain Double.NaN values.
     *
     * This method iterates over the rows of the provided 2D array. If a row is found
     * where all elements are not Double.NaN, it removes this row and all rows before it
     * from the array. The modified array, which may be smaller than the original, is then returned.
     *
     * Note: If all rows contain at least one Double.NaN, the method will return an empty array.
     *
     * @param arr The 2D array from which leading rows containing Double.NaN are to be removed.
     * @return A possibly smaller 2D array with leading rows containing Double.NaN removed.
     */
    public static double[][] ltrim(double[][] arr) {
        int numRows = arr.length;
        if (numRows == 0) {
            return new double[0][0];
        }

        int numCols = arr[0].length;
        int startIndex = numRows; // Initialized to numRows
        for (int i = 0; i < numRows; i++) {
            boolean hasNaN = false;
            for (int j = 0; j < numCols; j++) {
                if (Double.isNaN(arr[i][j])) {
                    hasNaN = true;
                    break;
                }
            }
            if (!hasNaN) {
                startIndex = i;
                break; // Stop the loop as soon as a row without NaN is found
            }
        }

        return Arrays.copyOfRange(arr, startIndex, arr.length);
    }

    public static int[] generateMissingIndicesArray(double[] point) {
        List<Integer> intArray = new ArrayList<>();
        for (int i = 0; i < point.length; i++) {
            if (Double.isNaN(point[i])) {
                intArray.add(i);
            }
        }
        // Return null if the array is empty
        if (intArray.size() == 0) {
            return null;
        }
        return intArray.stream().mapToInt(Integer::intValue).toArray();
    }

    public static boolean areAnyElementsNaN(double[] array) {
        return Arrays.stream(array).anyMatch(Double::isNaN);
    }

    /**
     * Rounds the given double value to the specified number of decimal places.
     *
     * This method uses BigDecimal for precise rounding. It rounds using the
     * HALF_UP rounding mode, which means it rounds towards the "nearest neighbor"
     * unless both neighbors are equidistant, in which case it rounds up.
     *
     * @param value the double value to be rounded
     * @param places the number of decimal places to round to
     * @return the rounded double value
     * @throws IllegalArgumentException if the specified number of decimal places is negative
     */
    public static double roundDouble(double value, int places) {
        if (places < 0) {
            throw new IllegalArgumentException();
        }

        BigDecimal bd = new BigDecimal(Double.toString(value));
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }
}
