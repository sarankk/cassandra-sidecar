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

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.DefaultSettableGauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * Tracks metrics related to restore functionality provided by Sidecar.
 */
@Singleton
public class RestoreMetrics
{
    public static final String FEATURE = "sidecar.restore";
    protected final MetricRegistry metricRegistry;
    protected final Timer sliceReplicationTime;
    protected final Timer jobCompletionTime;
    protected final Meter successfulJobs;
    protected final Meter failedJobs;
    protected final DefaultSettableGauge<Integer> activeJobs;
    protected final Meter tokenRefreshes;
    protected final Meter tokenUnauthorized;
    protected final Meter tokenExpired;

    @Inject
    public RestoreMetrics(MetricRegistry metricRegistry)
    {
        this.metricRegistry = Objects.requireNonNull(metricRegistry, "Metric registry can not be null");

        sliceReplicationTime
        = metricRegistry.timer(new MetricName(FEATURE, "slice_replication_time").toString());
        jobCompletionTime
        = metricRegistry.timer(new MetricName(FEATURE, "job_completion_time").toString());
        successfulJobs = metricRegistry.meter(new MetricName(FEATURE, "successful_jobs").toString());
        failedJobs = metricRegistry.meter(new MetricName(FEATURE, "failed_jobs").toString());
        activeJobs = metricRegistry.gauge(new MetricName(FEATURE, "active_jobs").toString(),
                                          () -> new DefaultSettableGauge<>(0));
        tokenRefreshes = metricRegistry.meter(new MetricName(FEATURE, "token_refreshes").toString());
        tokenUnauthorized = metricRegistry.meter(new MetricName(FEATURE, "token_unauthorized").toString());
        tokenExpired = metricRegistry.meter(new MetricName(FEATURE, "token_expired").toString());
    }

    public void recordSliceReplicationTime(long duration, TimeUnit unit)
    {
        sliceReplicationTime.update(duration, unit);
    }

    public void recordJobCompletionTime(long duration, TimeUnit unit)
    {
        jobCompletionTime.update(duration, unit);
    }

    public void recordSuccessfulJob()
    {
        successfulJobs.mark();
    }

    public void recordFailedJob()
    {
        failedJobs.mark();
    }

    public void recordActiveJobs(int count)
    {
        activeJobs.setValue(count);
    }

    public void recordTokenRefresh()
    {
        tokenRefreshes.mark();
    }

    public void recordTokenUnauthorized()
    {
        tokenUnauthorized.mark();
    }

    public void recordTokenExpired()
    {
        tokenExpired.mark();
    }
}
