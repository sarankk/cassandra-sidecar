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

public class CassandraPermission extends Permission
{
    public static final CassandraPermission CREATE = new CassandraPermission("CREATE");
    public static final CassandraPermission ALTER = new CassandraPermission("ALTER");
    public static final CassandraPermission DROP = new CassandraPermission("DROP");
    public static final CassandraPermission SELECT = new CassandraPermission("SELECT");
    public static final CassandraPermission MODIFY = new CassandraPermission("MODIFY");
    public static final CassandraPermission DESCRIBE = new CassandraPermission("DESCRIBE");
    public static final CassandraPermission UNMASK = new CassandraPermission("UNMASK");

    /**
     * For Cassandra related permissions, target is not relevant.
     */
    public CassandraPermission(String action)
    {
        super(action);
    }

    @Override
    public String toString()
    {
        return action;
    }
}
