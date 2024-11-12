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

package org.apache.cassandra.sidecar.acl.authorization;

import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.CacheConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;

import static org.apache.cassandra.sidecar.ExecutorPoolsHelper.createdSharedTestPool;
import static org.apache.cassandra.sidecar.acl.authorization.CassandraRoleAuthorizationsCache.UNIQUE_CACHE_ENTRY;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_SIDECAR_SCHEMA_INITIALIZED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link CassandraRoleAuthorizationsCache}
 */
class CassandraRoleAuthorizationsCacheTest
{
    Vertx vertx;
    ExecutorPools executorPools;

    @BeforeEach
    void setup()
    {
        vertx = Vertx.vertx();
        executorPools = createdSharedTestPool(vertx);
    }

    @Test
    void testCacheSizeAlwaysOne() throws InterruptedException
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.getAllRolesAndPermissions())
        .thenReturn(Collections.singletonMap("test_role1", Collections.singleton(SidecarActions.CREATE_SNAPSHOT.toAuthorization())));
        SidecarConfiguration mockConfig = mockConfig();
        CassandraRoleAuthorizationsCache cache = new CassandraRoleAuthorizationsCache(vertx,
                                                                                      executorPools,
                                                                                      mockConfig,
                                                                                      mockDbAccessor);
        assertThat(cache.getAll().size()).isZero();
        assertThat(cache.getAuthorizations("test_role1").size()).isOne();
        assertThat(cache.getAll().size()).isOne();

        when(mockDbAccessor.getAllRolesAndPermissions())
        .thenReturn(ImmutableMap.of("test_role1", Collections.singleton(SidecarActions.CREATE_SNAPSHOT.toAuthorization()),
                                    "test_role2", Collections.singleton(SidecarActions.STREAM_SSTABLE.toAuthorization())));

        // wait for cache entries to be refreshed
        Thread.sleep(3000);

        // New entries fetched during refreshes
        assertThat(cache.getAuthorizations("test_role2").size()).isOne();
        assertThat(cache.getAll().size()).isOne();
    }

    @Test
    void testBulkload() throws InterruptedException
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.getAllRolesAndPermissions())
        .thenReturn(ImmutableMap.of("test_role1", Collections.singleton(SidecarActions.CREATE_SNAPSHOT.toAuthorization()),
                                    "test_role2", Collections.singleton(SidecarActions.STREAM_SSTABLE.toAuthorization())));
        SidecarConfiguration mockConfig = mockConfig();
        CassandraRoleAuthorizationsCache cache = new CassandraRoleAuthorizationsCache(vertx,
                                                                                      executorPools,
                                                                                      mockConfig,
                                                                                      mockDbAccessor);
        assertThat(cache.getAll().size()).isZero();

        // warming cache
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());

        // wait for cache warming. system_auth.role_permissions table bulk loaded against a single key
        Thread.sleep(3000);
        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.get(UNIQUE_CACHE_ENTRY).get("test_role1").size()).isOne();
        assertThat(cache.get(UNIQUE_CACHE_ENTRY).get("test_role2").size()).isOne();
    }

    @Test
    void testCacheDisabled()
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.getAllRolesAndPermissions())
        .thenReturn(ImmutableMap.of("test_role1", Collections.singleton(SidecarActions.CREATE_SNAPSHOT.toAuthorization()),
                                    "test_role2", Collections.singleton(SidecarActions.STREAM_SSTABLE.toAuthorization())));
        SidecarConfiguration mockConfig = mockConfig();
        when(mockConfig.accessControlConfiguration().permissionCacheConfiguration().enabled()).thenReturn(false);
        CassandraRoleAuthorizationsCache cache = new CassandraRoleAuthorizationsCache(vertx,
                                                                                      executorPools,
                                                                                      mockConfig,
                                                                                      mockDbAccessor);
        assertThat(cache.getAuthorizations("test_role1").size()).isOne();
        assertThat(cache.getAuthorizations("test_role2").size()).isOne();
    }

    @Test
    void testEmptyEntriesFromSystemAuthDatabaseAccessor() throws InterruptedException
    {
        SystemAuthDatabaseAccessor mockDbAccessor = mock(SystemAuthDatabaseAccessor.class);
        when(mockDbAccessor.getAllRolesAndPermissions()).thenReturn(Collections.emptyMap());
        SidecarConfiguration mockConfig = mockConfig();
        CassandraRoleAuthorizationsCache cache = new CassandraRoleAuthorizationsCache(vertx,
                                                                                      executorPools,
                                                                                      mockConfig,
                                                                                      mockDbAccessor);
        assertThat(cache.getAll().size()).isZero();

        // warming cache
        vertx.eventBus().publish(ON_SIDECAR_SCHEMA_INITIALIZED.address(), new JsonObject());

        // wait for cache warming. system_auth.role_permissions table bulk loaded against a single key
        Thread.sleep(3000);
        assertThat(cache.getAll().size()).isOne();
        assertThat(cache.get(UNIQUE_CACHE_ENTRY).size()).isZero();
    }

    private SidecarConfiguration mockConfig()
    {
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAccessControlConfig = mock(AccessControlConfiguration.class);
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAccessControlConfig);
        CacheConfiguration mockCacheConfig = mock(CacheConfiguration.class);
        when(mockCacheConfig.enabled()).thenReturn(true);
        when(mockCacheConfig.expireAfterAccessMillis()).thenReturn(3000L);
        when(mockCacheConfig.maximumSize()).thenReturn(10L);
        when(mockCacheConfig.warmupRetries()).thenReturn(5);
        when(mockCacheConfig.warmupRetryIntervalMillis()).thenReturn(1000L);
        when(mockAccessControlConfig.permissionCacheConfiguration()).thenReturn(mockCacheConfig);
        return mockConfig;
    }
}
