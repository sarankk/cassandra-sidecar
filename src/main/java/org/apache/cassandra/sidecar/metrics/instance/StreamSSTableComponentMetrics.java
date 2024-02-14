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

package org.apache.cassandra.sidecar.metrics.instance;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.DefaultSettableGauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.cassandra.sidecar.metrics.MetricName;

import static org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics.INSTANCE_PREFIX;

/**
 * {@link StreamSSTableComponentMetrics} tracks metrics captured during streaming of SSTable component from Sidecar.
 */
public class StreamSSTableComponentMetrics
{
    public static final String FEATURE = INSTANCE_PREFIX + ".stream";
    protected final InstanceMetricRegistry instanceMetricRegistry;
    protected final String sstableComponent;
    protected final Meter rateLimitedCalls;
    protected final DefaultSettableGauge<Long> bytesStreamed;
    protected final Timer timeTakenForSendFile;
    protected final Timer waitTimeSent;
    protected final DefaultSettableGauge<Long> totalBytesStreamed;

    public StreamSSTableComponentMetrics(InstanceMetricRegistry instanceMetricRegistry,
                                         String sstableComponent)
    {
        this.instanceMetricRegistry = Objects.requireNonNull(instanceMetricRegistry, "Metric registry can not be null");
        this.sstableComponent
        = Objects.requireNonNull(sstableComponent, "SSTable component required for component specific metrics capture");
        if (sstableComponent.isEmpty())
        {
            throw new IllegalArgumentException("SSTableComponent required for component specific metrics capture");
        }

        rateLimitedCalls
        = instanceMetricRegistry.meter(new MetricName(FEATURE,
                                                      "rate_limited_calls_429",
                                                      "component=" + sstableComponent).toString());
        bytesStreamed = instanceMetricRegistry.gauge(new MetricName(FEATURE,
                                                                    "bytes_streamed",
                                                                    "component=" + sstableComponent).toString(),
                                                     () -> new DefaultSettableGauge<>(0L));
        timeTakenForSendFile = instanceMetricRegistry.timer(new MetricName(FEATURE,
                                                                           "time_taken_for_sendfile",
                                                                           "component=" + sstableComponent).toString());
        waitTimeSent = instanceMetricRegistry.timer(new MetricName(FEATURE,
                                                                   "429_wait_time",
                                                                   "component=" + sstableComponent).toString());
        totalBytesStreamed = instanceMetricRegistry.gauge(new MetricName(FEATURE,
                                                                         "total_bytes_streamed").toString(),
                                                          () -> new DefaultSettableGauge<>(0L));
    }

    public String sstableComponent()
    {
        return sstableComponent;
    }

    public void recordRateLimitedCall()
    {
        rateLimitedCalls.mark();
    }

    public void recordBytesStreamed(long bytes)
    {
        bytesStreamed.setValue(bytes);
        totalBytesStreamed.setValue(bytes);
    }

    public void recordTimeTakenForSendFile(long duration, TimeUnit unit)
    {
        timeTakenForSendFile.update(duration, unit);
    }

    public void recordWaitTimeSent(long duration, TimeUnit unit)
    {
        waitTimeSent.update(duration, unit);
    }
}
