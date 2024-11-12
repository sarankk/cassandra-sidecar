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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.vertx.ext.auth.authorization.Authorization;
import org.apache.cassandra.sidecar.config.ResourceActionsConfiguration;
import org.apache.cassandra.sidecar.config.RolePermissionsConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

/**
 * Provides sidecar related permissions, configured in Sidecar.
 */
@Singleton
public class SidecarRoleAuthorizationsProvider
{
    // Stores set of authorizations against a user role.
    private final Map<String, Set<Authorization>> userAuthorizations;

    @Inject
    public SidecarRoleAuthorizationsProvider(SidecarConfiguration sidecarConfiguration)
    {
        userAuthorizations
        = Collections.unmodifiableMap(parsePermissions(sidecarConfiguration.accessControlConfiguration()
                                                                           .rolePermissionsConfigurations()));
    }

    public Set<Authorization> getAuthorizations(String role)
    {
        return userAuthorizations.get(role);
    }

    private Map<String, Set<Authorization>> parsePermissions(List<RolePermissionsConfiguration> rolePermissionsConfigurations)
    {
        Map<String, Set<Authorization>> userAuthorizations = new HashMap<>();
        for (RolePermissionsConfiguration userPermission : rolePermissionsConfigurations)
        {
            for (ResourceActionsConfiguration resourceActions : userPermission.permissionConfigurations())
            {
                Set<Authorization> authorizations = resourceActions
                                                    .actions()
                                                    .stream()
                                                    .map(name -> Action.fromName(name).toAuthorization(resourceActions.resource()))
                                                    .collect(Collectors.toSet());
                userAuthorizations.computeIfAbsent(userPermission.role(), k -> new HashSet<>()).addAll(authorizations);
            }
        }
        return userAuthorizations;
    }
}
