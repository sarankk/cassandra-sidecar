package org.apache.cassandra.sidecar.accesscontrol.authorization;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.authorization.AuthorizationProvider;

public class RoleBaseAuthorizationProvider implements AuthorizationProvider
{
    public String getId()
    {
        return "";
    }

    public void getAuthorizations(User user, Handler<AsyncResult<Void>> handler)
    {

    }

    public Future<Void> getAuthorizations(User user)
    {
        return AuthorizationProvider.super.getAuthorizations(user);
    }
}
