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

import java.util.Set;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;

/**
 * This class is adapted from Cassandra, it represents data resource from system_auth.role_permissions table
 */
public class DataResource implements Resource
{
    enum Level
    {
        ROOT, KEYSPACE, ALL_TABLES, TABLE
    }

    // permissions which may be granted on tables
    private static final Set<Permission> TABLE_LEVEL_PERMISSIONS = ImmutableSet.of(CassandraPermission.ALTER,
                                                                                   CassandraPermission.DROP,
                                                                                   CassandraPermission.SELECT,
                                                                                   CassandraPermission.MODIFY,
                                                                                   CassandraPermission.AUTHORIZE,
                                                                                   CassandraPermission.UNMASK,
                                                                                   CassandraPermission.SELECT_MASKED);

    // permissions which may be granted on all tables of a given keyspace
    private static final Set<Permission> ALL_TABLES_LEVEL_PERMISSIONS = ImmutableSet.of(CassandraPermission.CREATE,
                                                                                        CassandraPermission.ALTER,
                                                                                        CassandraPermission.DROP,
                                                                                        CassandraPermission.SELECT,
                                                                                        CassandraPermission.MODIFY,
                                                                                        CassandraPermission.AUTHORIZE,
                                                                                        CassandraPermission.UNMASK,
                                                                                        CassandraPermission.SELECT_MASKED);

    // permissions which may be granted on one or all keyspaces
    private static final Set<Permission> KEYSPACE_LEVEL_PERMISSIONS = ImmutableSet.of(CassandraPermission.CREATE,
                                                                                      CassandraPermission.ALTER,
                                                                                      CassandraPermission.DROP,
                                                                                      CassandraPermission.SELECT,
                                                                                      CassandraPermission.MODIFY,
                                                                                      CassandraPermission.AUTHORIZE,
                                                                                      CassandraPermission.UNMASK,
                                                                                      CassandraPermission.SELECT_MASKED);

    private static final String ROOT_NAME = "data";
    private static final DataResource ROOT_RESOURCE = new DataResource(Level.ROOT, null, null);

    private final Level level;
    private final String keyspace;
    private final String table;

    // memoized hashcode since DataRessource is immutable and used in hashmaps often
    private final transient int hash;

    private DataResource(Level level, String keyspace, String table)
    {
        this.level = level;
        this.keyspace = keyspace;
        this.table = table;

        this.hash = Objects.hashCode(level, keyspace, table);
    }

    /**
     * @return Printable name of the resource.
     */
    @Override
    public String getName()
    {
        switch (level)
        {
            case KEYSPACE:
                return String.format("%s/%s", ROOT_NAME, keyspace);
            case ALL_TABLES:
                return String.format("%s/%s/*", ROOT_NAME, keyspace);
            case TABLE:
                return String.format("%s/%s/%s", ROOT_NAME, keyspace, table);
        }
        throw new IllegalStateException("Unexpected level " + level + " found for data resource");
    }

    @Override
    public Set<Permission> applicablePermissions()
    {
        switch (level)
        {
            case ROOT:
            case KEYSPACE:
                return KEYSPACE_LEVEL_PERMISSIONS;
            case ALL_TABLES:
                return ALL_TABLES_LEVEL_PERMISSIONS;
            case TABLE:
                return TABLE_LEVEL_PERMISSIONS;
        }
        throw new AssertionError();
    }

    /**
     * @return the root-level resource.
     */
    public static DataResource root()
    {
        return ROOT_RESOURCE;
    }

    /**
     * Creates a DataResource representing a keyspace.
     */
    public static DataResource keyspace(String keyspace)
    {
        return new DataResource(Level.KEYSPACE, keyspace, null);
    }

    /**
     * Creates a DataResource representing all tables of a keyspace.
     */
    public static DataResource allTables(String keyspace)
    {
        return new DataResource(Level.ALL_TABLES, keyspace, null);
    }

    /**
     * Creates a DataResource instance representing a table.
     */
    public static DataResource table(String keyspace, String table)
    {
        return new DataResource(Level.TABLE, keyspace, table);
    }

    /**
     * Parses a data resource name into a DataResource instance.
     *
     * @param name Name of the data resource.
     * @return DataResource instance matching the name.
     */
    public static DataResource fromName(String name)
    {
        String[] parts = StringUtils.split(name, '/');

        if (!parts[0].equals(ROOT_NAME) || parts.length > 3)
            throw new IllegalArgumentException(String.format("%s is not a valid data resource name", name));

        if (parts.length == 1)
            return root();

        if (parts.length == 2)
            return keyspace(parts[1]);

        if ("*".equals(parts[2]))
            return allTables(parts[1]);

        return table(parts[1], parts[2]);
    }
}
