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

/**
 * This class is adapted from Cassandra.
 */
public interface Resource
{
    /**
     * @return Name of the resource.
     */
    String getName();

    /**
     * Returns the set of Permissions that may be applied to this resource
     *
     * Certain permissions are not applicable to particular types of resources. For e.g. CREATE permission on table,
     * or SELECT on a Role. This is necessary because the CQL syntax supports ALL as wildcard, but the set of
     * permissions that should resolve to varies by Resource.
     *
     * @return the permissions that may be granted on the specific resource
     */
    Set<Permission> applicablePermissions();
}
