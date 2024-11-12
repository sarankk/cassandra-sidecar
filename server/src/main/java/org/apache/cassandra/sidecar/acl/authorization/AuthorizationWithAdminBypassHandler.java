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

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.impl.AuthorizationHandlerImpl;

/**
 * Verifies user has required authorizations. Allows admin identities to bypass authorization checks.
 */
public class AuthorizationWithAdminBypassHandler extends AuthorizationHandlerImpl
{
    private final AdminIdentityResolver adminIdentityResolver;

    public AuthorizationWithAdminBypassHandler(AdminIdentityResolver adminIdentityResolver,
                                               Authorization authorization)
    {
        super(authorization);
        this.adminIdentityResolver = adminIdentityResolver;
    }

    @Override
    public void handle(RoutingContext ctx)
    {
        User user = ctx.user();
        validatePrincipal(user);

        List<String> identities = Optional.ofNullable(user.principal().getString("identity"))
                                          .map(Collections::singletonList)
                                          .orElseGet(() -> Arrays.asList(user.principal()
                                                                             .getString("identities")
                                                                             .split(",")));

        // Admin identities bypass route specific authorization checks
        if (identities.stream().anyMatch(adminIdentityResolver::isAdmin))
        {
            ctx.next();
            return;
        }

        super.handle(ctx);
    }

    private void validatePrincipal(User user)
    {
        if (user.principal() == null)
        {
            throw new HttpException(HttpResponseStatus.UNAUTHORIZED.code(), "User principal empty");
        }

        if (!user.principal().containsKey("identity") && !user.principal().containsKey("identities"))
        {
            throw new HttpException(HttpResponseStatus.UNAUTHORIZED.code(), "No valid identity found for authorizing");
        }
    }
}
