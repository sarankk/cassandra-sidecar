/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.metrics;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.NoopMetricRegistry;
import com.codahale.metrics.Timer;

/**
 * Allows filtering of metrics based on configured allow list. Metrics are filtered out before registering them.
 */
public class FilteringMetricRegistry extends MetricRegistry
{
    private static final NoopMetricRegistry NO_OP_METRIC_REGISTRY = new NoopMetricRegistry(); // supplies no-op metrics
    private final Predicate<String> isAllowed;
    // all metrics including the allowed and disallowed
    private final ConcurrentMap<String, Metric> allMetrics = new ConcurrentHashMap<>();

    public FilteringMetricRegistry(Predicate<String> isAllowed)
    {
        this.isAllowed = isAllowed;
    }

    @Override
    public Counter counter(String name)
    {
        // allMetrics is populated in order to let vertx internal know that the metric has been registered and
        // to avoid registration loop
        return (Counter) allMetrics.computeIfAbsent(name,
                                                    key -> isAllowed.test(key) ? super.counter(key) : NO_OP_METRIC_REGISTRY.counter(key));
    }

    @Override
    public Counter counter(String name, MetricSupplier<Counter> supplier)
    {
        return (Counter) allMetrics.computeIfAbsent(name,
                                                    key -> isAllowed.test(key) ? super.counter(key, supplier) : NO_OP_METRIC_REGISTRY.counter(key));
    }

    @Override
    public Histogram histogram(String name)
    {
        return (Histogram) allMetrics.computeIfAbsent(name,
                                                      key -> isAllowed.test(key) ? super.histogram(key) : NO_OP_METRIC_REGISTRY.histogram(key));
    }

    @Override
    public Histogram histogram(String name, MetricSupplier<Histogram> supplier)
    {
        return (Histogram) allMetrics.computeIfAbsent(name,
                                                      key -> isAllowed.test(key) ? super.histogram(key, supplier) : NO_OP_METRIC_REGISTRY.histogram(key));
    }

    @Override
    public Meter meter(String name)
    {
        return (Meter) allMetrics.computeIfAbsent(name,
                                                  key -> isAllowed.test(key) ? super.meter(key) : NO_OP_METRIC_REGISTRY.meter(key));
    }

    @Override
    public Meter meter(String name, MetricSupplier<Meter> supplier)
    {
        return (Meter) allMetrics.computeIfAbsent(name,
                                                  key -> isAllowed.test(key) ? super.meter(key, supplier) : NO_OP_METRIC_REGISTRY.meter(key));
    }

    @Override
    public Timer timer(String name)
    {
        return (Timer) allMetrics.computeIfAbsent(name,
                                                  key -> isAllowed.test(key) ? super.timer(key) : NO_OP_METRIC_REGISTRY.timer(key));
    }

    @Override
    public Timer timer(String name, MetricSupplier<Timer> supplier)
    {
        return (Timer) allMetrics.computeIfAbsent(name,
                                                  key -> isAllowed.test(key) ? super.timer(key, supplier) : NO_OP_METRIC_REGISTRY.timer(key));
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Gauge> T gauge(String name)
    {
        return (T) allMetrics.computeIfAbsent(name,
                                              key -> isAllowed.test(key) ? super.gauge(key) : NO_OP_METRIC_REGISTRY.gauge(key));
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Gauge> T gauge(String name, MetricSupplier<T> supplier)
    {
        return (T) allMetrics.computeIfAbsent(name,
                                              key -> isAllowed.test(key) ? super.gauge(name, supplier) : supplier.newMetric() /* unregistered metric */);
    }

    /**
     * @return all the metrics including the allowed and disallowed metrics
     */
    @Override
    public Map<String, Metric> getMetrics()
    {
        return Collections.unmodifiableMap(allMetrics);
    }

    /**
     * @return metrics registered with register. This will be useful for testing purposes to check
     * what metrics are captured
     */
    public Map<String, Metric> getIncludedMetrics()
    {
        return super.getMetrics();
    }

    /**
     * Metric specific retrieve methods such as {@code counter(name)} retrieve a noop instance if metric is filtered.
     * Prefer calling those over register method, register method returns an unregistered metric if the metric is
     * filtered. In some cases Noop metric instance has a performance advantage.
     */
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public <T extends Metric> T register(String name, T metric) throws IllegalArgumentException
    {
        if (metric == null)
        {
            throw new IllegalArgumentException("Metric can not be null");
        }
        return (T) allMetrics.computeIfAbsent(name,
                                              key -> isAllowed.test(key) ? super.register(key, metric) : metric);
    }
}
