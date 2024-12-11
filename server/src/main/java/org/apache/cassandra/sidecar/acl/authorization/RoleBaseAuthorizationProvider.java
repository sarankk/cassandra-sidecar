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
import java.util.Optional;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.auth.authorization.PermissionBasedAuthorization;
import io.vertx.ext.auth.authorization.impl.PermissionBasedAuthorizationImpl;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;

public class RoleBaseAuthorizationProvider implements AuthorizationProvider
{
    private final IdentityToRoleCache identityToRoleCache;
    private final RolePermissionsCache rolePermissionsCache;

    public RoleBaseAuthorizationProvider(IdentityToRoleCache identityToRoleCache,
                                         RolePermissionsCache rolePermissionsCache,
                                         SidecarPermissionsProvider sidecarPermissionsProvider)
    {
        this.identityToRoleCache = identityToRoleCache;
        this.rolePermissionsCache = rolePermissionsCache;
    }

    public String getId()
    {
        return "RBAC";
    }

    @Override
    public void getAuthorizations(User user, Handler<AsyncResult<Void>> handler)
    {
        getAuthorizations(user).onComplete(handler);
    }

    @Override
    public Future<Void> getAuthorizations(User user)
    {
//        return AuthorizationProvider.super.getAuthorizations(user);


        List<String> identities = Optional.ofNullable(user.principal().getString("identity"))
                                          .map(Collections::singletonList)
                                          .orElseGet(() -> Arrays.asList(user.principal()
                                                                             .getString("identities")
                                                                             .split(",")));

//        String cassandraRole = identityToRoleCache.get(identities.get(0));
        String cassandraRole = "cassandra";

        rolePermissionsCache.getAll().forEach((key, permissions) -> {
            if (key.getLeft().equals(cassandraRole))
            {
                permissions.forEach(permission -> {
                    PermissionBasedAuthorization authorization = new PermissionBasedAuthorizationImpl(permission.toString());
                    authorization.setResource(key.getRight());
                    user.authorizations().add(getId(), authorization);
                });
            }
        });

        // for super users should grant AllowAllAuthorization

        return Future.succeededFuture();
    }
}
