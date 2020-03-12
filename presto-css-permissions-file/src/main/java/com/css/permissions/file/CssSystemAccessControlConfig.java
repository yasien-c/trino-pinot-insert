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
package com.css.permissions.file;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CssSystemAccessControlConfig
{
    public static final int DEFAULT_PORT = 50051;
    public static final int DEFAULT_CACHE_SIZE = 10000;
    public static final int DEFAULT_PRINCIPAL_CACHE_EXPIRATION_SECONDS = 60;
    public static final int DEFAULT_ROLES_CACHE_EXPIRATION_SECONDS = 60;

    private String identityServiceHost;
    private int identityServicePort = DEFAULT_PORT;
    private String cssDirectoryServiceHost;
    private int cssDirectoryServicePort = DEFAULT_PORT;
    private String permissionsFile;
    private Optional<String> extraRoleBindingFile = Optional.empty();

    private int principalsCacheSize = DEFAULT_CACHE_SIZE;
    private int principalsCacheExpirationSeconds = DEFAULT_PRINCIPAL_CACHE_EXPIRATION_SECONDS;
    private int rolesCacheExpirationSeconds = DEFAULT_ROLES_CACHE_EXPIRATION_SECONDS;

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

    public String getCssDirectoryServiceHost()
    {
        return cssDirectoryServiceHost;
    }

    @Config("permissions.css-directory-service-host")
    public CssSystemAccessControlConfig setCssDirectoryServiceHost(String address)
    {
        this.cssDirectoryServiceHost = requireNonNull(address, "CSS directory address is null");
        return this;
    }

    public int getCssDirectoryServicePort()
    {
        return cssDirectoryServicePort;
    }

    @Config("permissions.css-directory-service-port")
    public CssSystemAccessControlConfig setCssDirectoryServicePort(int port)
    {
        this.cssDirectoryServicePort = requireNonNull(port, "CSS directory address is null");
        return this;
    }

    public String getPermissionsFile()
    {
        return permissionsFile;
    }

    @Config("permissions.css-permissions-file")
    @ConfigDescription("Path to a file that holds permission mappings.")
    public CssSystemAccessControlConfig setPermissionsFile(String permissionsFile)
    {
        this.permissionsFile = requireNonNull(permissionsFile,
                "CSS permissions file is null");
        return this;
    }

    public Optional<String> getExtraRoleBindingFile()
    {
        return extraRoleBindingFile;
    }

    @Config("permissions.css-extra-role-bindings-file")
    @ConfigDescription("Path to a file that extra user to role bindings for users (or service accounts) that don't have LDAP roles.")
    public CssSystemAccessControlConfig setExtraRoleBindingFile(String extraRoleBindingFile)
    {
        this.extraRoleBindingFile = Optional.ofNullable(extraRoleBindingFile);
        return this;
    }

    public int getPrincipalsCacheSize()
    {
        return principalsCacheSize;
    }

    @Config("permissions.principals-cache-size")
    @ConfigDescription("Number of CssPrincipal objects to cache in memory.")
    public CssSystemAccessControlConfig setPrincipalsCacheSize(int principalsCacheSize)
    {
        this.principalsCacheSize = principalsCacheSize;
        return this;
    }

    public int getPrincipalsCacheExpirationSeconds()
    {
        return principalsCacheExpirationSeconds;
    }

    @Config("permissions.principals-cache-expiration-seconds")
    @ConfigDescription("Time CssPrincipal objects live in memory. More time means less stress on Identity service but longer times to revoke a permission.")
    public CssSystemAccessControlConfig setPrincipalsCacheExpirationSeconds(int principalsCacheExpirationSeconds)
    {
        this.principalsCacheExpirationSeconds = principalsCacheExpirationSeconds;
        return this;
    }

    public int getRolesCacheExpirationSeconds()
    {
        return rolesCacheExpirationSeconds;
    }

    @Config("permissions.roles-cache-expiration-seconds")
    @ConfigDescription("Time roles objects live in memory. More time means less stress on DirectoryProxy service but longer times to revoke a permission.")
    public CssSystemAccessControlConfig setRolesCacheExpirationSeconds(int rolesCacheExpirationSeconds)
    {
        this.rolesCacheExpirationSeconds = rolesCacheExpirationSeconds;
        return this;
    }
}
