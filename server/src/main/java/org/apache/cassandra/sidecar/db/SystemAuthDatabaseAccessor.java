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

package org.apache.cassandra.sidecar.db;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.cassandra.sidecar.acl.authorization.CassandraPermission;
import org.apache.cassandra.sidecar.acl.authorization.Resource;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.db.schema.SystemAuthSchema;
import org.apache.cassandra.sidecar.exceptions.SchemaUnavailableException;

/**
 * Database Accessor that queries cassandra to get information maintained under system_auth keyspace.
 */
@Singleton
public class SystemAuthDatabaseAccessor extends DatabaseAccessor<SystemAuthSchema>
{
    @Inject
    public SystemAuthDatabaseAccessor(SystemAuthSchema systemAuthSchema,
                                      CQLSessionProvider sessionProvider)
    {
        super(systemAuthSchema, sessionProvider);
    }

    /**
     * Queries Cassandra for the role associated with given identity.
     *
     * @param identity  Identity of user extracted
     * @return the role associated with the given identity in Cassandra
     */
    public String findRoleFromIdentity(String identity)
    {
        ensureIdentityToRoleTableAccess();
        BoundStatement statement = tableSchema.selectRoleFromIdentity()
                                              .bind(identity);
        ResultSet result = execute(statement);
        Row row = result.one();
        return row != null ? row.getString("role") : null;
    }

    /**
     * Queries Cassandra for all rows in identity_to_role table
     *
     * @return - {@code List<Row>} containing each row in the identity to roles table
     */
    public Map<String, String> findAllIdentityToRoles()
    {
        ensureIdentityToRoleTableAccess();
        BoundStatement statement = tableSchema.getAllRolesAndIdentities().bind();

        ResultSet resultSet = execute(statement);
        Map<String, String> results = new HashMap<>();
        for (Row row : resultSet)
        {
            results.put(row.getString("identity"), row.getString("role"));
        }
        return results;
    }

    public Set<CassandraPermission> listPermissionsOfRoleOnResource(Pair<String, String> roleResource)
    {
        BoundStatement statement = tableSchema.listPermissionsOfRoleOnResource()
                                              .bind(roleResource.getLeft());
        ResultSet result = execute(statement);
        for (Row row : result.all())
        {

        }
        return Collections.emptySet();
    }

    public Map<Pair<String, String>, Set<CassandraPermission>> getAllRolesAndPermissions()
    {
        BoundStatement statement = tableSchema.getAllRolesAndPermissions().bind();
        ResultSet result = execute(statement);
        Map<Pair<String, String>, Set<CassandraPermission>> rolePermissions = new HashMap<>();
        for (Row row : result)
        {
            String role = row.getString("role");
            String resource = row.getString("resource");
            Set<CassandraPermission> permissions = row.getSet("permissions", String.class)
                                                      .stream()
                                                      .map(CassandraPermission::new)
                                                      .collect(Collectors.toSet());
            Pair<String, String> key = Pair.of(role, resource);
            if (rolePermissions.containsKey(key))
            {
                rolePermissions.get(key).addAll(permissions);
            }
            else
            {
                rolePermissions.put(key, permissions);
            }
        }
        return rolePermissions;
    }

    private void ensureIdentityToRoleTableAccess()
    {
        if (tableSchema.selectRoleFromIdentity() == null || tableSchema.getAllRolesAndIdentities() == null)
        {
            throw new SchemaUnavailableException("SystemAuthSchema was not prepared, values cannot be retrieved from table");
        }
    }
}
