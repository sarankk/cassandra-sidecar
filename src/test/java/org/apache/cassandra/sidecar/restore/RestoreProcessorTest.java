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

package org.apache.cassandra.sidecar.restore;

import java.util.concurrent.CountDownLatch;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.schema.SidecarSchema;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricProvider;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.mockito.Mockito;

import static org.apache.cassandra.sidecar.AssertionUtils.loopAssert;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.getMetric;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class RestoreProcessorTest
{
    private RestoreProcessor processor;
    private SidecarSchema sidecarSchema;
    private PeriodicTaskExecutor periodicTaskExecutor;
    private InstancesConfig instancesConfig;

    @BeforeEach
    void setup()
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        instancesConfig = injector.getInstance(InstancesConfig.class);
        sidecarSchema = mock(SidecarSchema.class);
        RestoreProcessor delegate = injector.getInstance(RestoreProcessor.class);
        processor = spy(delegate);
        when(processor.delay()).thenReturn(100L);
        when(processor.sidecarSchema()).thenReturn(sidecarSchema);
        periodicTaskExecutor = injector.getInstance(PeriodicTaskExecutor.class);

        InstanceMetricProvider instanceMetricProvider = injector.getInstance(InstanceMetricProvider.class);
        instancesConfig.instances().forEach(instance -> {
            instanceMetricProvider.metrics(instance.id());
        });
    }

    @AfterEach
    void cleanUp()
    {
        registry().removeMatching((name, metric) -> true);
        instancesConfig.instances().forEach(instance -> {
            registry(instance.id()).removeMatching((name, metric) -> true);
        });
    }

    @Test
    void testMaxProcessConcurrency()
    {
        // SidecarSchema is initialized
        when(sidecarSchema.isInitialized()).thenReturn(true);

        int concurrency = TestModule.RESTORE_MAX_CONCURRENCY;
        periodicTaskExecutor.schedule(processor);

        assertThat(processor.activeSlices()).isZero();

        CountDownLatch latch = new CountDownLatch(1);

        int total = concurrency * 3;
        for (int i = 0; i < total; i++)
        {
            processor.submit(mockSlowSlice(latch));
        }

        // assert before any slice can be completed
        loopAssert(3, () -> {
            // expect slice import queue has the size of concurrency
            long lastImportQueueLengthBeforeSliceCompletion = lastImportQueueLength();
            assertThat(lastImportQueueLengthBeforeSliceCompletion).isPositive();
            assertThat(lastImportQueueLengthBeforeSliceCompletion).isLessThanOrEqualTo(concurrency);

            // expect the pending slices count equals to "total - concurrency"
            long lastPendingSliceCountBeforeSliceCompletion = lastPendingSliceCount();
            assertThat(lastPendingSliceCountBeforeSliceCompletion).isPositive();
            assertThat(lastPendingSliceCountBeforeSliceCompletion).isLessThanOrEqualTo(total - concurrency);

            assertThat(processor.activeSlices()).isEqualTo(concurrency);
        });

        // slices start to succeed
        latch.countDown();

        // it never grows beyond `concurrency`
        loopAssert(3, () -> {
            assertThat(processor.activeSlices())
            .describedAs("Active slice count should be in the range of (0, concurrency]")
            .isLessThanOrEqualTo(concurrency)
            .isPositive();
        });

        // the active slices should be back to 0
        // and the pending slices should be back to 0
        loopAssert(3, () -> {
            assertThat(processor.activeSlices()).isZero();
            long lastImportQueueLengthAfterSliceCompletion = lastImportQueueLength();
            assertThat(lastImportQueueLengthAfterSliceCompletion).isZero();
            long lastPendingSliceCountAfterSliceCompletion = lastPendingSliceCount();
            assertThat(lastPendingSliceCountAfterSliceCompletion).isZero();
        });

        // all slices complete successfully
        long[] sliceCompletionTimes = sliceCompletionTimes();
        assertThat(sliceCompletionTimes).hasSize(total);
        for (long sliceCompleteDuration : sliceCompletionTimes)
        {
            assertThat(sliceCompleteDuration).isPositive();
        }
    }

    @Test
    void testSkipExecuteWhenSidecarSchemaIsNotInitialized()
    {
        when(sidecarSchema.isInitialized()).thenReturn(false);

        assertThat(processor.shouldSkip()).isTrue();
        assertThat(processor.activeSlices()).isZero();

        CountDownLatch latch = new CountDownLatch(1);
        processor.submit(mockSlowSlice(latch));
        assertThat(processor.activeSlices())
        .describedAs("No slice should be active because executions are skipped")
        .isZero();

        // Make slice completable. But since all executions are skipped, the active slice should remain as 1
        latch.countDown();
        loopAssert(3, () -> {
            assertThat(processor.pendingStartSlices()).isOne();
            assertThat(processor.activeSlices()).isZero();
        });
    }

    private RestoreSlice mockSlowSlice(CountDownLatch latch)
    {
        RestoreSlice slice = mock(RestoreSlice.class, Mockito.RETURNS_DEEP_STUBS);
        when(slice.jobId()).thenReturn(UUIDs.timeBased());
        InstanceMetadata instanceMetadata = mock(InstanceMetadata.class);
        when(instanceMetadata.id()).thenReturn(1);
        when(slice.owner()).thenReturn(instanceMetadata);
        when(slice.toAsyncTask(any(), any(), any(), anyDouble(), any(), any(), any())).thenReturn(promise -> {
            Uninterruptibles.awaitUninterruptibly(latch);
            promise.complete(slice);
        });
        when(slice.hasImported()).thenReturn(true);
        return slice;
    }

    private Long lastImportQueueLength()
    {
        return (Long) getMetric(1, "sidecar.instance.restore.slice_import_queue_length", Gauge.class)
                      .getValue();
    }

    private Long lastPendingSliceCount()
    {
        return (Long) getMetric(1, "sidecar.instance.restore.pending_slice_count", Gauge.class)
                      .getValue();
    }

    private long[] sliceCompletionTimes()
    {
        return getMetric(1, "sidecar.instance.restore.slice_completion_time", Timer.class)
               .getSnapshot()
               .getValues();
    }
}
