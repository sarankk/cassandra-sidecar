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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Pair;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.config.ResourcePermissionConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.UserPermissionConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

/**
 * Provider for sidecar related permissions.
 */
@Singleton
public class SidecarPermissionsProvider
{
    private final Map<Pair<String, Resource>, Set<SidecarPermission>> userPermissions = new ConcurrentHashMap<>();

    @Inject
    public SidecarPermissionsProvider(SidecarConfiguration sidecarConfiguration)
    {
        parsePermissions(sidecarConfiguration.accessControlConfiguration().userPermissionConfigurations());
    }

    public Set<SidecarPermission> permissionForResource(String role, String resource)
    {
        return userPermissions.get(Pair.of(role, resource));
    }

    public Map<Pair<String, Resource>, Set<SidecarPermission>> userPermissions()
    {
        return userPermissions;
    }

    private void parsePermissions(List<UserPermissionConfiguration> userPermissionConfigurations)
    {
        for (UserPermissionConfiguration userPermission : userPermissionConfigurations)
        {
            for (ResourcePermissionConfiguration resourcePermission : userPermission.permissionConfigurations())
            {
                Resource resource = parseResource(resourcePermission.resource());
                Pair<String, Resource> key = Pair.of(userPermission.role(), resource);
                SidecarPermission permission = parsePermission(resourcePermission.permission());
                userPermissions.computeIfAbsent(key, k -> new HashSet<>()).add(permission);
            }
        }
    }

    private Resource parseResource(String resource)
    {
        if (resource == null)
        {
            throw new ConfigurationException("Resource can not be null");
        }
        if (resource.startsWith(DataResource.root().getName()))
        {
            return DataResource.fromName(resource);
        }
        throw new ConfigurationException("Invalid resource set " + resource + " expected resource type is data");
    }

    private SidecarPermission parsePermission(String permission)
    {
        if (permission == null)
        {
            throw new ConfigurationException("Permission can not be null");
        }
        String[] parts = permission.split("_");
        if (parts.length <= 1)
        {
            throw new ConfigurationException("Permission set in unexpected format. Expected format <action_allowed>_<permission_name");
        }
        return new SidecarPermission(parts[1], parts[0]);
    }
}
