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

import com.css.directoryproxy.api.DirectoryProxyServerGrpc.DirectoryProxyServerBlockingStub;
import com.css.directoryproxy.api.GetRolesRequest;
import com.css.directoryproxy.api.GetRolesResponse;
import com.css.directoryproxy.api.GetUserRequest;
import com.css.directoryproxy.api.GetUserResponse;
import com.css.identity.server.api.DescribeSessionRequest;
import com.css.identity.server.api.DescribeSessionResponse;
import com.css.identity.server.api.PublicIdentityServiceGrpc.PublicIdentityServiceBlockingStub;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ProtocolStringList;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.prestosql.spi.security.AccessDeniedException;

import javax.inject.Inject;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.css.permissions.file.Utils.parseJson;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DefaultCssDirectoryClient
        implements CssDirectoryClient
{
    public static final String EMAIL_SEPARATOR = "@";
    private static final String ROLES_CACHE_KEY = "roles";

    private final DirectoryProxyServerBlockingStub directoryProxyServer;
    private final PublicIdentityServiceBlockingStub identityService;
    private final LoadingCache<String, CssPrincipal> principals;
    private final LoadingCache<String, Map<String, String>> rolesCache;
    private final Map<String, Set<String>> emailToRoles;

    @Inject
    public DefaultCssDirectoryClient(
            CssSystemAccessControlConfig config,
            DirectoryProxyServerBlockingStub directoryProxyServer,
            PublicIdentityServiceBlockingStub identityService)
    {
        requireNonNull(config);
        this.directoryProxyServer = requireNonNull(directoryProxyServer);
        this.identityService = requireNonNull(identityService);
        emailToRoles = buildExtraRoleBindings(config);

        principals = CacheBuilder.newBuilder()
                .maximumSize(config.getPrincipalsCacheSize())
                .expireAfterWrite(config.getPrincipalsCacheExpirationSeconds(), TimeUnit.SECONDS)
                .build(
                        new CacheLoader<String, CssPrincipal>()
                        {
                            @Override
                            public CssPrincipal load(String key)
                            {
                                return getUserPrincipal(key);
                            }
                        });

        rolesCache = CacheBuilder.newBuilder()
                .maximumSize(1)
                .expireAfterWrite(config.getRolesCacheExpirationSeconds(), TimeUnit.SECONDS)
                .build(
                        new CacheLoader<String, Map<String, String>>()
                        {
                            @Override
                            public Map<String, String> load(String key)
                            {
                                return getRoles();
                            }
                        });
    }

    private static void validateUserName(String user, String email)
    {
        if (!email.startsWith(format("%s@", user))) {
            throw new AccessDeniedException(format("User '%s' does not match credential email '%s'",
                    user,
                    email));
        }
    }

    private Map<String, Set<String>> buildExtraRoleBindings(CssSystemAccessControlConfig config)
    {
        Map<String, Set<String>> emailToRoles = new HashMap<>();
        Optional<String> extraRoleBindingsFile = config.getExtraRoleBindingFile();
        if (extraRoleBindingsFile.isPresent()) {
            RoleBindings roleBindings = parseJson(
                    Paths.get(extraRoleBindingsFile.get()), RoleBindings.class);

            for (RoleBinding roleBinding : roleBindings.getRoleBindings()) {
                roleBinding.getEmails().forEach(email -> {
                    emailToRoles.putIfAbsent(email, new HashSet<>());
                    emailToRoles.get(email).add(roleBinding.getRoleName());
                });
            }
        }

        return emailToRoles;
    }

    @Override
    public CssPrincipal getUserPrincipalAndValidateUser(String accessToken, String user)
    {
        try {
            if (Strings.isNullOrEmpty(accessToken)) {
                throw new CssFileAuthenticationException("Access token is null or empty");
            }
            CssPrincipal principal = principals.get(accessToken);
            validateUserName(user, principal.getEmail());
            return principal;
        }
        catch (ExecutionException e) {
            throw new CssFileAuthenticationException(e.getMessage());
        }
    }

    private CssPrincipal getUserPrincipal(String accessToken)
    {
        CssPrincipal.Builder builder = CssPrincipal.Builder.builder();
        DescribeSessionResponse response = identityService.describeSession(DescribeSessionRequest.newBuilder()
                .setToken(accessToken)
                .build());

        String accountName = getAccountName(response);
        builder.withName(accountName)
                .withEmail(response.getAccount().getEmail());

        try {
            GetUserResponse getUserResponse = directoryProxyServer.getUser(GetUserRequest.newBuilder()
                    .setUsername(accountName)
                    .build());

            builder.withRoles(getRolesFromIds(getUserResponse.getUser().getRoleIdList()));
        }
        catch (StatusRuntimeException e) {
            if (Status.NOT_FOUND.getCode().equals(e.getStatus().getCode())) {
                // User is valid but doesn't present in LDAP. Will try to fetch groups from extra role bindings
                if (emailToRoles.containsKey(response.getAccount().getEmail())) {
                    builder.withRoles(emailToRoles.get(response.getAccount().getEmail()));
                }
                else {
                    throw new CssFileAuthenticationException(String.format("User {%s} doesn't have LDAP or extra roles.",
                            response.getAccount().getEmail()));
                }
            }
        }

        return builder.build();
    }

    private String getAccountName(DescribeSessionResponse response)
    {
        String email = response.getAccount().getEmail();
        return email.substring(0, email.lastIndexOf(EMAIL_SEPARATOR));
    }

    protected List<String> getRolesFromIds(ProtocolStringList roleIdList)
    {
        List<String> roleNames = new ArrayList<>(roleIdList.size());
        Map<String, String> roles = getCachedRoles();
        roleIdList.forEach(roleId -> {
            String roleName = roles.get(roleId);
            if (roleName != null) {
                roleNames.add(roleName);
            }
            else {
                throw new CssFileAuthenticationException(
                        String.format("User has a role with id {%s} but couldn't load it from directory service. " +
                                "List of current roles: {%s}", roleId, roles));
            }
        });

        return roleNames;
    }

    private Map<String, String> getCachedRoles()
    {
        try {
            return rolesCache.get(ROLES_CACHE_KEY);
        }
        catch (ExecutionException e) {
            throw new CssFileAuthenticationException(e.getMessage());
        }
    }

    protected Map<String, String> getRoles()
    {
        GetRolesResponse response = directoryProxyServer.getRoles(GetRolesRequest.newBuilder()
                .build());

        Map<String, String> roles = new HashMap<>(response.getRoleCount());
        response.getRoleList().forEach(role -> roles.put(String.valueOf(role.getId()), role.getName()));

        return roles;
    }
}
