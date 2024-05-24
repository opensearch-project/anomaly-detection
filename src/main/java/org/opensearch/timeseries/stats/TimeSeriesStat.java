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

package org.opensearch.timeseries.stats;

import java.util.function.Supplier;

import org.opensearch.timeseries.stats.suppliers.CounterSupplier;
import org.opensearch.timeseries.stats.suppliers.SettableSupplier;

/**
 * Class represents a stat the plugin keeps track of
 */
public class TimeSeriesStat<T> {
    private Boolean clusterLevel;
    private Supplier<T> supplier;

    /**
     * Constructor
     *
     * @param clusterLevel whether the stat has clusterLevel scope or nodeLevel scope
     * @param supplier supplier that returns the stat's value
     */
    public TimeSeriesStat(Boolean clusterLevel, Supplier<T> supplier) {
        this.clusterLevel = clusterLevel;
        this.supplier = supplier;
    }

    /**
     * Determines whether the stat is cluster specific or node specific
     *
     * @return true is stat is cluster level; false otherwise
     */
    public Boolean isClusterLevel() {
        return clusterLevel;
    }

    /**
     * Get the value of the statistic
     *
     * @return T value of the stat
     */
    public T getValue() {
        return supplier.get();
    }

    /**
     * Set the value of the statistic
     *
     * @param value set value
     */
    public void setValue(Long value) {
        if (supplier instanceof SettableSupplier) {
            ((SettableSupplier) supplier).set(value);
        }
    }

    /**
     * Increments the supplier if it can be incremented
     */
    public void increment() {
        if (supplier instanceof CounterSupplier) {
            ((CounterSupplier) supplier).increment();
        }
    }

    /**
     * Decrease the supplier if it can be decreased.
     */
    public void decrement() {
        if (supplier instanceof CounterSupplier) {
            ((CounterSupplier) supplier).decrement();
        }
    }
}
