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

package org.apache.cassandra.sidecar.config;

/**
 * Configuration that stores resource and its permission
 */
public interface ResourcePermissionConfiguration
{
    /**
     * @return resource associated with permission. Resource starts with resource type, with format
     * <resource_type>/...<resource_parts>. Currently, sidecar recognizes only data resource type
     */
    String resource();

    /**
     * @return permission associated with resource. Permission is represented through
     * <action_allowed>_<action_target>. CREATE_SNAPSHOT permission grants CREATE action on SNAPSHOT target.
     * List of possible actions are CREATE, READ, EDIT, DELETE, STREAM, UPLOAD, EXECUTE, ALL. Action ALL allows
     * all possible actions for a target, for e.g. ALL_SNAPSHOT allows both CREATE_SNAPSHOT, DELETE_SNAPSHOT
     */
    String permission();
}
