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
import org.apache.cassandra.sidecar.config.ResourceActionsConfiguration;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

/**
 * {@inheritDoc}
 */
public class ResourceActionsConfigurationImpl implements ResourceActionsConfiguration
{
    @JsonProperty("resource")
    private final String resource;

    @JsonProperty("actions")
    private final List<String> actions;

    public ResourceActionsConfigurationImpl()
    {
        this(null, Collections.emptyList());
    }

    public ResourceActionsConfigurationImpl(String resource, List<String> actions)
    {
        this.resource = resource;
        this.actions = actions;
        validate(actions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("resource")
    public String resource()
    {
        return resource;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @JsonProperty("actions")
    public List<String> actions()
    {
        return actions;
    }

    private void validate(List<String> actions)
    {
        for (String action : actions)
        {
            if (action == null || action.isEmpty())
            {
                throw new ConfigurationException("Action can not be empty");
            }
        }
    }
}
