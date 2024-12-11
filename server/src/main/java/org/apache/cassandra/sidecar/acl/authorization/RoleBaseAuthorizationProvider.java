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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.auth.authorization.PermissionBasedAuthorization;
import io.vertx.ext.auth.authorization.impl.PermissionBasedAuthorizationImpl;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;

/**
 * Provides authorizations based on user's role. Extracts permissions user holds from either Cassandra's
 * system_auth.role_permissions table or from permissions configured in sidecar and retrieves sidecar permissions
 * from SidecarPermissionsProvider.
 */
public class RoleBaseAuthorizationProvider implements AuthorizationProvider
{
    private final IdentityToRoleCache identityToRoleCache;
    private final RolePermissionsCache rolePermissionsCache;
    private final SidecarPermissionsProvider sidecarPermissionsProvider;

    public RoleBaseAuthorizationProvider(IdentityToRoleCache identityToRoleCache,
                                         RolePermissionsCache rolePermissionsCache,
                                         SidecarPermissionsProvider sidecarPermissionsProvider)
    {
        this.identityToRoleCache = identityToRoleCache;
        this.rolePermissionsCache = rolePermissionsCache;
        this.sidecarPermissionsProvider = sidecarPermissionsProvider;
    }

    public String getId()
    {
        return "RoleBasedAccessControl";
    }

    @Override
    public void getAuthorizations(User user, Handler<AsyncResult<Void>> handler)
    {
        getAuthorizations(user).onComplete(handler);
    }

    @Override
    public Future<Void> getAuthorizations(User user)
    {
        List<String> identities = Optional.ofNullable(user.principal().getString("identity"))
                                          .map(Collections::singletonList)
                                          .orElseGet(() -> Arrays.asList(user.principal()
                                                                             .getString("identities")
                                                                             .split(",")));

        String expectedRole = identityToRoleCache.get(identities.get(0));

        for (Map.Entry<Pair<String, String>, Set<CassandraPermission>> entry : rolePermissionsCache.getAll().entrySet())
        {
            String role = entry.getKey().getLeft();
            String resource = entry.getKey().getRight();
            if (!role.equals(expectedRole))
            {
                continue;
            }
            entry.getValue().forEach(permission -> {
                PermissionBasedAuthorization authorization = new PermissionBasedAuthorizationImpl(permission.name());
                authorization.setResource(resource);
                user.authorizations().add(getId(), authorization);
            });
        }

        for (Map.Entry<Pair<String, Resource>, Set<SidecarPermission>> entry : sidecarPermissionsProvider.userPermissions().entrySet())
        {
            String role = entry.getKey().getLeft();
            Resource resource = entry.getKey().getRight();
            if (!role.equals(expectedRole))
            {
                continue;
            }
            entry.getValue().forEach(permission -> {
                PermissionBasedAuthorization authorization = new PermissionBasedAuthorizationImpl(permission.name());
                authorization.setResource(resource.getName());
                user.authorizations().add(getId(), authorization);
            });
        }
        return Future.succeededFuture();
    }
}
