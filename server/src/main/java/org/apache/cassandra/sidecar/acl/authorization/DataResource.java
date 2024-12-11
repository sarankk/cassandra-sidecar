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
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

/**
 * This class is adapted from Cassandra, since it is used to read resource from system_auth.role_permissions table
 */
public class DataResource implements Resource
{
    private static final String ROOT_NAME = "data";

    private final Level level;
    private final String keyspace;
    private final String table;

    private DataResource(Level level, String keyspace, String table)
    {
        this.level = level;
        this.keyspace = keyspace;
        this.table = table;
    }

    /**
     * @return Printable name of the resource.
     */
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

    public Set<Permission> applicablePermissions()
    {
        return Collections.emptySet();
    }

//    /**
//     * Parses a data resource name into a DataResource instance.
//     *
//     * @param name Name of the data resource.
//     * @return DataResource instance matching the name.
//     */
//    public static DataResource fromName(String name)
//    {
//        String[] parts = StringUtils.split(name, '/');
//
//        if (!parts[0].equals(ROOT_NAME) || parts.length > 3)
//            throw new IllegalArgumentException(String.format("%s is not a valid data resource name", name));
//
//        if (parts.length == 1)
//            return root();
//
//        if (parts.length == 2)
//            return keyspace(parts[1]);
//
//        if ("*".equals(parts[2]))
//            return allTables(parts[1]);
//
//        return table(parts[1], parts[2]);
//    }

    enum Level
    {
        KEYSPACE, ALL_TABLES, TABLE
    }
}
