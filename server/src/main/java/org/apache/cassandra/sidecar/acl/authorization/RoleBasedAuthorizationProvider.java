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
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.AuthorizationProvider;
import io.vertx.ext.web.handler.HttpException;
import org.apache.cassandra.sidecar.acl.IdentityToRoleCache;

import static org.apache.cassandra.sidecar.utils.AuthUtils.extractIdentities;

/**
 * Provides authorizations based on user's role. Extracts permissions user holds from Cassandra's
 * system_auth.role_permissions table and from permissions configured in sidecar.
 */
public class RoleBasedAuthorizationProvider implements AuthorizationProvider
{
    private final IdentityToRoleCache identityToRoleCache;
    private final CassandraRoleAuthorizationsCache cassandraRoleAuthorizationsCache;
    private final SidecarRoleAuthorizationsProvider sidecarRoleAuthorizationsProvider;

    public RoleBasedAuthorizationProvider(IdentityToRoleCache identityToRoleCache,
                                          CassandraRoleAuthorizationsCache cassandraRoleAuthorizationsCache,
                                          SidecarRoleAuthorizationsProvider sidecarRoleAuthorizationsProvider)
    {
        this.identityToRoleCache = identityToRoleCache;
        this.cassandraRoleAuthorizationsCache = cassandraRoleAuthorizationsCache;
        this.sidecarRoleAuthorizationsProvider = sidecarRoleAuthorizationsProvider;
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
        List<String> identities = extractIdentities(user);

        if (identities.isEmpty())
        {
            throw new HttpException(HttpResponseStatus.UNAUTHORIZED.code(), "Missing client identities");
        }

        // First identity in chain is usually client identity
        String role = identityToRoleCache.get(identities.get(0));
        if (role == null)
        {
            throw new HttpException(HttpResponseStatus.UNAUTHORIZED.code(), "No matching Cassandra role found");
        }

        // when entries in cache are not found, null is returned. We can not add null in user.authorizations()
        Set<Authorization> cassandraAuthorizations
        = Optional.ofNullable(cassandraRoleAuthorizationsCache.getAuthorizations(role)).orElse(Collections.emptySet());
        Set<Authorization> sidecarAuthorizations
        = Optional.ofNullable(sidecarRoleAuthorizationsProvider.getAuthorizations(role)).orElse(Collections.emptySet());
        user.authorizations().add(getId(), cassandraAuthorizations);
        user.authorizations().add(getId(), sidecarAuthorizations);
        return Future.succeededFuture();
    }
}
