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

package org.apache.cassandra.sidecar.config.yaml;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.config.ResourcePermissionConfiguration;
import org.apache.cassandra.sidecar.config.UserPermissionConfiguration;

/**
 * {@inheritDoc}
 */
public class UserPermissionConfigurationImpl implements UserPermissionConfiguration
{
    public static final String DEFAULT_ROLE = null;
    public static final List<ResourcePermissionConfiguration> DEFAULT_PERMISSION_CONFIGURATIONS = Collections.emptyList();

    @JsonProperty("role")
    private final String role;

    @JsonProperty("permissions")
    private final List<ResourcePermissionConfiguration> permissionConfigurations;

    public UserPermissionConfigurationImpl()
    {
        this(DEFAULT_ROLE, DEFAULT_PERMISSION_CONFIGURATIONS);
    }

    public UserPermissionConfigurationImpl(String role, List<ResourcePermissionConfiguration> permissionConfigurations)
    {
        this.role = role;
        this.permissionConfigurations = permissionConfigurations;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("role")
    public String role()
    {
        return role;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("permissions")
    public List<ResourcePermissionConfiguration> permissionConfigurations()
    {
        return permissionConfigurations;
    }
}
