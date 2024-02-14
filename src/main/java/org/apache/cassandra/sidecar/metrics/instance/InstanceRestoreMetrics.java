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
 * {@link InstanceRestoreMetrics} contains metrics to track restore task for a Cassandra instance maintained by Sidecar.
 */
public class InstanceRestoreMetrics
{
    public static final String FEATURE = INSTANCE_PREFIX + ".restore";
    protected final InstanceMetricRegistry instanceMetricRegistry;
    protected final Timer sliceCompletionTime;
    protected final Timer sliceImportTime;
    protected final Timer sliceUnzipTime;
    protected final Timer sliceValidationTime;
    protected final Meter sliceDownloadTimeouts;
    protected final Meter sliceDownloadRetries;
    protected final Meter sliceChecksumMismatches;
    protected final DefaultSettableGauge<Long> sliceImportQueueLength;
    protected final DefaultSettableGauge<Long> pendingSliceCount;
    protected final DefaultSettableGauge<Long> dataSSTableComponentSize;
    protected final Timer sliceDownloadTime;
    protected final DefaultSettableGauge<Long> sliceCompressedSizeInBytes;
    protected final DefaultSettableGauge<Long> sliceUncompressedSizeInBytes;

    public InstanceRestoreMetrics(InstanceMetricRegistry instanceMetricRegistry)
    {
        this.instanceMetricRegistry = Objects.requireNonNull(instanceMetricRegistry, "Metric registry can not be null");

        sliceCompletionTime = instanceMetricRegistry.timer(new MetricName(FEATURE,
                                                                          "slice_completion_time").toString());
        sliceImportTime = instanceMetricRegistry.timer(new MetricName(FEATURE,
                                                                      "slice_import_time").toString());
        sliceUnzipTime = instanceMetricRegistry.timer(new MetricName(FEATURE,
                                                                     "slice_unzip_time").toString());
        sliceValidationTime = instanceMetricRegistry.timer(new MetricName(FEATURE,
                                                                          "slice_validation_time").toString());
        sliceDownloadTimeouts = instanceMetricRegistry.meter(new MetricName(FEATURE,
                                                                            "slice_download_timeouts").toString());
        sliceDownloadRetries = instanceMetricRegistry.meter(new MetricName(FEATURE,
                                                                           "slice_completion_retries").toString());
        sliceChecksumMismatches = instanceMetricRegistry.meter(new MetricName(FEATURE,
                                                                              "slice_checksum_mismatches").toString());
        sliceImportQueueLength
        = instanceMetricRegistry.gauge(new MetricName(FEATURE, "slice_import_queue_length").toString(),
                                       () -> new DefaultSettableGauge<>(0L));
        pendingSliceCount
        = instanceMetricRegistry.gauge(new MetricName(FEATURE, "pending_slice_count").toString(),
                                       () -> new DefaultSettableGauge<>(0L));
        dataSSTableComponentSize
        = instanceMetricRegistry.gauge(new MetricName(FEATURE, "restore_size", "component=data").toString(),
                                       () -> new DefaultSettableGauge<>(0L));
        sliceDownloadTime = instanceMetricRegistry.timer(new MetricName(FEATURE, "slice_download_time").toString());
        sliceCompressedSizeInBytes
        = instanceMetricRegistry.gauge(new MetricName(FEATURE, "slice_compressed_size_bytes").toString(),
                                       () -> new DefaultSettableGauge<>(0L));
        sliceUncompressedSizeInBytes
        = instanceMetricRegistry.gauge(new MetricName(FEATURE, "slice_uncompressed_size_bytes").toString(),
                                       () -> new DefaultSettableGauge<>(0L));
    }

    public void recordSliceCompletionTime(long duration, TimeUnit unit)
    {
        sliceCompletionTime.update(duration, unit);
    }

    public void recordSliceImportTime(long duration, TimeUnit unit)
    {
        sliceImportTime.update(duration, unit);
    }

    public void recordSliceUnzipTime(long duration, TimeUnit unit)
    {
        sliceUnzipTime.update(duration, unit);
    }

    public void recordSliceValidationTime(long duration, TimeUnit unit)
    {
        sliceValidationTime.update(duration, unit);
    }

    public void recordSliceDownloadTimeout()
    {
        sliceDownloadTimeouts.mark();
    }

    public void recordSliceDownloadRetry()
    {
        sliceDownloadRetries.mark();
    }

    public void recordSliceChecksumMismatch()
    {
        sliceChecksumMismatches.mark();
    }

    public void recordSliceImportQueueLength(long length)
    {
        sliceImportQueueLength.setValue(length);
    }

    public void recordPendingSliceCount(long count)
    {
        pendingSliceCount.setValue(count);
    }

    public void recordDataSSTableComponentSize(long bytes)
    {
        dataSSTableComponentSize.setValue(bytes);
    }

    public void recordSliceDownloadTime(long duration, TimeUnit unit)
    {
        sliceDownloadTime.update(duration, unit);
    }

    public void recordSliceCompressedSize(long bytes)
    {
        sliceCompressedSizeInBytes.setValue(bytes);
    }

    public void recordSliceUncompressedSize(long bytes)
    {
        sliceUncompressedSizeInBytes.setValue(bytes);
    }
}
