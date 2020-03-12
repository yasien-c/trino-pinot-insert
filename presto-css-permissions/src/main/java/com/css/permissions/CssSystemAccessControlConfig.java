/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.css.permissions;

import io.airlift.configuration.Config;

import static java.util.Objects.requireNonNull;

public class CssSystemAccessControlConfig
{
    public static final int DEFAULT_PORT = 50051;

    private String identityServiceHost;
    private int identityServicePort = DEFAULT_PORT;
    private String iamServiceHost;
    private int iamServicePort = DEFAULT_PORT;
    private String projectId = "datawarehouse";

    public String getIdentityServiceHost()
    {
        return identityServiceHost;
    }

    @Config("permissions.identity-service-host")
    public CssSystemAccessControlConfig setIdentityServiceHost(String identityServiceHost)
    {
        this.identityServiceHost = requireNonNull(identityServiceHost, "address is null");
        return this;
    }

    public int getIdentityServicePort()
    {
        return identityServicePort;
    }

    @Config("permissions.identity-service-port")
    public CssSystemAccessControlConfig setIdentityServicePort(int identityServicePort)
    {
        this.identityServicePort = identityServicePort;
        return this;
    }

    public String getIamServiceHost()
    {
        return iamServiceHost;
    }

    @Config("permissions.iam-service-host")
    public CssSystemAccessControlConfig setIamServiceHost(String address)
    {
        this.iamServiceHost = requireNonNull(address, "CSS directory address is null");
        return this;
    }

    public int getIamServicePort()
    {
        return iamServicePort;
    }

    @Config("permissions.iam-service-port")
    public CssSystemAccessControlConfig setIamServicePort(int port)
    {
        this.iamServicePort = requireNonNull(port, "CSS directory address is null");
        return this;
    }

    public String getProjectId()
    {
        return projectId;
    }

    @Config("permissions.project-id")
    public CssSystemAccessControlConfig setProjectId(String projectId)
    {
        this.projectId = projectId;
        return this;
    }
}
