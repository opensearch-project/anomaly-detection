/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.timeseries.util;

import java.util.Arrays;

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

}
