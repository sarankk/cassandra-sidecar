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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.utils.UUIDs;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.data.RestoreJobStatus;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools.TaskExecutorPool;
import org.apache.cassandra.sidecar.db.RestoreJob;
import org.apache.cassandra.sidecar.db.RestoreJobTest;
import org.apache.cassandra.sidecar.db.RestoreSlice;
import org.apache.cassandra.sidecar.db.RestoreSliceDatabaseAccessor;
import org.apache.cassandra.sidecar.exceptions.RestoreJobException;
import org.apache.cassandra.sidecar.exceptions.RestoreJobFatalException;
import org.apache.cassandra.sidecar.metrics.RestoreMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetricsImpl;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.utils.SSTableImporter;
import org.mockito.Mockito;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import static org.apache.cassandra.sidecar.AssertionUtils.getBlocking;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.getMetric;
import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RestoreSliceTaskTest
{
    private RestoreSlice restoreSlice;
    private StorageClient storageClient;
    private TaskExecutorPool executorPool;
    private SSTableImporter importer;
    private RestoreSliceTask task;
    private RestoreSliceDatabaseAccessor sliceDatabaseAccessor;
    private InstanceMetrics instanceMetrics;
    private RestoreMetrics restoreMetrics;

    @BeforeEach
    void setup()
    {
        restoreSlice = mock(RestoreSlice.class, Mockito.RETURNS_DEEP_STUBS);
        when(restoreSlice.stageDirectory()).thenReturn(Paths.get("."));
        when(restoreSlice.sliceId()).thenReturn("testing-slice");
        when(restoreSlice.key()).thenReturn("storage-key");
        when(restoreSlice.owner().id()).thenReturn(1);
        storageClient = mock(StorageClient.class);
        importer = mock(SSTableImporter.class);
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        executorPool = injector.getInstance(ExecutorPools.class).internal();
        sliceDatabaseAccessor = mock(RestoreSliceDatabaseAccessor.class);
        instanceMetrics = new InstanceMetricsImpl(registry(1));
        restoreMetrics = new RestoreMetrics(registry());
        task = new TestRestoreSliceTask(restoreSlice, storageClient,
                                        executorPool, importer, 0,
                                        sliceDatabaseAccessor, instanceMetrics, restoreMetrics);
    }

    @AfterEach
    void cleanup()
    {
        registry().removeMatching((name, metric) -> true);
        registry(1).removeMatching((name, metric) -> true);
    }

    @Test
    void testRestoreSucceeds()
    {
        when(storageClient.objectExists(restoreSlice)).thenReturn(CompletableFuture.completedFuture(null));
        when(storageClient.downloadObjectIfAbsent(restoreSlice))
        .thenReturn(CompletableFuture.completedFuture(new File(".")));
        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        // assert on the stats collected
        assertThat(getMetric("sidecar.restore.slice_replication_time", Timer.class)
                   .getSnapshot()
                   .size()).isOne();
        assertThat(getMetric("sidecar.restore.slice_replication_time", Timer.class)
                   .getSnapshot()
                   .getValues()[0]).isPositive();
        assertThat(getMetric(1, "sidecar.instance.restore.slice_download_time", Timer.class)
                   .getSnapshot()
                   .size()).isOne();
        assertThat(getMetric(1, "sidecar.instance.restore.slice_download_time", Timer.class)
                   .getSnapshot()
                   .getValues()[0]).isPositive();
        assertThat(getMetric(1, "sidecar.instance.restore.slice_unzip_time", Timer.class)
                   .getSnapshot()
                   .size()).isOne();
        assertThat(getMetric(1, "sidecar.instance.restore.slice_unzip_time", Timer.class)
                   .getSnapshot()
                   .getValues()[0]).isPositive();
        assertThat(getMetric(1, "sidecar.instance.restore.slice_validation_time", Timer.class)
                   .getSnapshot()
                   .size()).isOne();
        assertThat(getMetric(1, "sidecar.instance.restore.slice_validation_time", Timer.class)
                   .getSnapshot()
                   .getValues()[0]).isPositive();
        assertThat(getMetric(1, "sidecar.instance.restore.slice_import_time", Timer.class)
                   .getSnapshot()
                   .size()).isOne();
        assertThat(getMetric(1, "sidecar.instance.restore.slice_import_time", Timer.class)
                   .getSnapshot()
                   .getValues()[0]).isPositive();
    }

    @Test
    void testCaptureSliceReplicationTimeOnlyOnce()
    {
        // the existence of the slice is already confirmed by the s3 client
        when(restoreSlice.existsOnS3()).thenReturn(true);
        when(storageClient.downloadObjectIfAbsent(restoreSlice))
        .thenReturn(CompletableFuture.completedFuture(new File(".")));

        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        assertThat(getMetric("sidecar.restore.slice_replication_time", Timer.class)
                   .getCount())
        .describedAs("The replication time of the slice has been captured when confirming the existence." +
                     "It should not be captured again in this run.")
        .isZero();
    }

    @Test
    void testStopProcessingCancelledSlice()
    {
        when(restoreSlice.isCancelled()).thenReturn(true);

        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise);

        assertThatThrownBy(() -> getBlocking(promise.future()))
        .hasRootCauseExactlyInstanceOf(RestoreJobFatalException.class)
        .hasMessageContaining("Restore slice is cancelled");
    }

    @Test
    void testThrowRetryableExceptionOnS3ObjectNotFound()
    {
        CompletableFuture<HeadObjectResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(mock(NoSuchKeyException.class));
        when(storageClient.objectExists(restoreSlice)).thenReturn(failedFuture);

        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise);
        assertThatThrownBy(() -> getBlocking(promise.future()))
        .hasRootCauseExactlyInstanceOf(RestoreJobException.class) // NOT a fatal exception
        .hasMessageContaining("Object not found");
    }

    @Test
    void testSliceStaging()
    {
        // test specific setup
        RestoreJob job = spy(RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED));
        doReturn(true).when(job).isManagedBySidecar();
        when(restoreSlice.job()).thenReturn(job);
        when(restoreSlice.stagedObjectPath()).thenReturn(Paths.get("nonexist"));
        when(storageClient.objectExists(restoreSlice)).thenReturn(CompletableFuture.completedFuture(null));
        when(storageClient.downloadObjectIfAbsent(restoreSlice))
        .thenReturn(CompletableFuture.completedFuture(new File(".")));

        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        verify(restoreSlice, times(1)).completeStagePhase();
        verify(restoreSlice, times(0)).completeImportPhase(); // should not be called in this phase
        verify(sliceDatabaseAccessor, times(1)).updateStatus(restoreSlice);
    }

    @Test
    void testSliceStagingWithExistingObject(@TempDir Path testFolder) throws IOException
    {
        // test specific setup
        RestoreJob job = spy(RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.CREATED));
        doReturn(true).when(job).isManagedBySidecar();
        when(restoreSlice.job()).thenReturn(job);
        Path stagedPath = testFolder.resolve("slice.zip");
        Files.createFile(stagedPath);
        when(restoreSlice.stagedObjectPath()).thenReturn(stagedPath);
        when(storageClient.objectExists(restoreSlice))
        .thenThrow(new RuntimeException("Should not call this method"));
        when(storageClient.downloadObjectIfAbsent(restoreSlice))
        .thenThrow(new RuntimeException("Should not call this method"));

        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        verify(restoreSlice, times(1)).completeStagePhase();
        verify(restoreSlice, times(0)).completeImportPhase(); // should not be called in this phase
        verify(sliceDatabaseAccessor, times(1)).updateStatus(restoreSlice);
    }

    @Test
    void testSliceImport()
    {
        // test specific setup
        RestoreJob job = spy(RestoreJobTest.createTestingJob(UUIDs.timeBased(), RestoreJobStatus.STAGED));
        doReturn(true).when(job).isManagedBySidecar();
        when(restoreSlice.job()).thenReturn(job);

        Promise<RestoreSlice> promise = Promise.promise();
        task.handle(promise);
        getBlocking(promise.future()); // no error is thrown

        verify(restoreSlice, times(0)).completeStagePhase(); // should not be called in the phase
        verify(restoreSlice, times(1)).completeImportPhase();
        verify(sliceDatabaseAccessor, times(1)).updateStatus(restoreSlice);
    }


    static class TestRestoreSliceTask extends RestoreSliceTask
    {
        private final RestoreSlice slice;
        private final InstanceMetrics instanceMetrics;

        public TestRestoreSliceTask(RestoreSlice slice, StorageClient s3Client, TaskExecutorPool executorPool,
                                    SSTableImporter importer, double requiredUsableSpacePercentage,
                                    RestoreSliceDatabaseAccessor sliceDatabaseAccessor, InstanceMetrics instanceMetrics,
                                    RestoreMetrics restoreMetrics)
        {
            super(slice, s3Client, executorPool, importer, requiredUsableSpacePercentage,
                  sliceDatabaseAccessor, instanceMetrics, restoreMetrics);
            this.slice = slice;
            this.instanceMetrics = instanceMetrics;
        }

        @Override
        void unzipAndImport(Promise<RestoreSlice> event, File file, Runnable onSuccessCommit)
        {
            instanceMetrics.restore().recordSliceUnzipTime(123, TimeUnit.MILLISECONDS);
            instanceMetrics.restore().recordSliceValidationTime(123, TimeUnit.MILLISECONDS);
            instanceMetrics.restore().recordSliceImportTime(123, TimeUnit.MILLISECONDS);
            slice.completeImportPhase();
            event.tryComplete(slice);
            if (onSuccessCommit != null)
            {
                onSuccessCommit.run();
            }
        }

        @Override
        void unzipAndImport(Promise<RestoreSlice> event, File file)
        {
            unzipAndImport(event, file, null);
        }
    }
}
