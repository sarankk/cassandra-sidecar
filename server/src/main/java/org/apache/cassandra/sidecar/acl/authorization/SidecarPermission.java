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

import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.jetbrains.annotations.NotNull;

public class SidecarPermission extends Permission
{
    public static final SidecarPermission CREATE_SNAPSHOT = new SidecarPermission("SNAPSHOT", "CREATE");
    public static final SidecarPermission READ_SNAPSHOT = new SidecarPermission("SNAPSHOT", "READ");
    public static final SidecarPermission DELETE_SNAPSHOT = new SidecarPermission("SNAPSHOT", "DELETE");
    public static final SidecarPermission UPLOAD_SSTABLE = new SidecarPermission("SSTABLE", "UPLOAD");
    public static final SidecarPermission STREAM_SSTABLE = new SidecarPermission("SSTABLE", "STREAM");
    public static final SidecarPermission DELETE_SSTABLE = new SidecarPermission("SSTABLE", "STREAM");
    public static final SidecarPermission READ_INFO = new SidecarPermission("INFO", "READ");

    public SidecarPermission(String target, String action)
    {
        super(target, action);
    }

    @Override
    public String toString()
    {
        return String.format("%s_%s", action, target);
    }

    public static SidecarPermission from(@NotNull String permission)
    {
        String[] parts = permission.split("_");
        if (parts.length <= 1)
        {
            throw new ConfigurationException("Unexpected format found for sidecar permission set. " +
                                             "Expected format is <action_allowed>_<target>");
        }

        String action = parts[0];
        String target = parts[1];
        return new SidecarPermission(target, action);
    }
}
