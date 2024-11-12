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

import java.util.List;

/**
 * Configuration that stores resource and actions allowed on a resource
 */
public interface ResourceActionsConfiguration
{
    /**
     * @return resource associated with permission. Resource starts with resource type, with format
     * <resource_type>/...<resource_parts>. If resource is not set, actions are allowed across resources
     * for e.g. for cluster wide actions.
     */
    String resource();

    /**
     * @return {@code List} of actions allowed for the resource. Wildcard actions are supported. For
     * wildcard action ':' is used to divide parts and '*' is used to represent each wildcard part. Majority of
     * required sidecar permissions are represented in format <action_allowed>:<action_target>.
     */
    List<String> actions();
}
