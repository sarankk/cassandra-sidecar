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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.cassandra.sidecar.config.AccessControlConfiguration;
import org.apache.cassandra.sidecar.config.ResourceActionsConfiguration;
import org.apache.cassandra.sidecar.config.RolePermissionsConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ResourceActionsConfigurationImpl;
import org.apache.cassandra.sidecar.config.yaml.RolePermissionsConfigurationImpl;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for {@link SidecarRoleAuthorizationsProvider}
 */
public class SidecarRoleAuthorizationsProviderTest
{
    @Test
    void testAuthorizationsFetched()
    {
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAclConfig = mock(AccessControlConfiguration.class);
        List<RolePermissionsConfiguration> rolePermissionsConfiguration = new ArrayList<>();
        ResourceActionsConfiguration resourceActionsConfiguration = new ResourceActionsConfigurationImpl("resource",
                                                                                                         Collections.singletonList("CREATE"));
        rolePermissionsConfiguration.add(new RolePermissionsConfigurationImpl("test_role", Collections.singletonList(resourceActionsConfiguration)));
        when(mockAclConfig.rolePermissionsConfigurations()).thenReturn(rolePermissionsConfiguration);
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAclConfig);
        SidecarRoleAuthorizationsProvider authorizationsProvider = new SidecarRoleAuthorizationsProvider(mockConfig);
        assertThat(authorizationsProvider.getAuthorizations("test_role").size()).isOne();
    }

    @Test
    void testNotFoundUser()
    {
        SidecarConfiguration mockConfig = mock(SidecarConfiguration.class);
        AccessControlConfiguration mockAclConfig = mock(AccessControlConfiguration.class);
        List<RolePermissionsConfiguration> rolePermissionsConfiguration = new ArrayList<>();
        ResourceActionsConfiguration resourceActionsConfiguration = new ResourceActionsConfigurationImpl("resource",
                                                                                                         Collections.singletonList("CREATE"));
        rolePermissionsConfiguration.add(new RolePermissionsConfigurationImpl("test_role", Collections.singletonList(resourceActionsConfiguration)));
        when(mockAclConfig.rolePermissionsConfigurations()).thenReturn(rolePermissionsConfiguration);
        when(mockConfig.accessControlConfiguration()).thenReturn(mockAclConfig);
        SidecarRoleAuthorizationsProvider authorizationsProvider = new SidecarRoleAuthorizationsProvider(mockConfig);
        assertThat(authorizationsProvider.getAuthorizations("user_not_found")).isNull();
    }
}
