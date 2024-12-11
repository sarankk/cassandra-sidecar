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

import java.security.Permissions;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.acl.AuthCache;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.db.SystemAuthDatabaseAccessor;

/**
 * Caches entries from system_auth.role_permissions table. The table maps valid certificate identities to Cassandra
 * roles. identity_to_role table is available since Cassandra versions 5.0
 */
@Singleton
public class RolePermissionsCache extends AuthCache<Pair<String, String>, Set<CassandraPermission>>
{
    private static final String NAME = "role_permissions_cache";

    @Inject
    protected RolePermissionsCache(String name,
                                   Vertx vertx,
                                   ExecutorPools executorPools,
                                   SidecarConfiguration sidecarConfiguration,
                                   SystemAuthDatabaseAccessor systemAuthDatabaseAccessor)
    {
        super(NAME,
              vertx,
              executorPools,
              systemAuthDatabaseAccessor::listPermissionsOfRoleOnResource,
              systemAuthDatabaseAccessor::getAllRolesAndPermissions,
              sidecarConfiguration.accessControlConfiguration().permissionCacheConfiguration());
    }
}
