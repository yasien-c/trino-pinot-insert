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

import com.css.iam.serviceapi.HasPermissionBatchRequest;
import com.css.iam.serviceapi.HasPermissionRequest;
import com.css.iam.serviceapi.PermissionCheck;
import com.css.identity.server.api.DescribeSessionRequest;
import com.css.identity.server.api.PublicIdentityServiceGrpc.PublicIdentityServiceBlockingStub;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.spi.security.AccessDeniedException;
import io.prestosql.spi.security.SystemSecurityContext;

import javax.inject.Inject;

import java.util.Collection;
import java.util.List;

import static com.css.iam.serviceapi.IamServiceGrpc.IamServiceBlockingStub;
import static com.css.permissions.ResourceType.CATALOG;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AuthorizationClient
{
    public static final String ACCESS_TOKEN = "access-token";
    public static final String EMAIL_SEPARATOR = "@";
    /*
     * Needed for selecting from views, Statement analyzer will replace the identity
     * with this user.
     * Unauthenticated access by this user will fail for all other actions.
     */
    private static final String CSS_VIEW_OWNER = "";

    private static final Logger log = Logger.get(AuthorizationClient.class);

    private final PublicIdentityServiceBlockingStub identityService;
    private final IamServiceBlockingStub iamService;
    private final String project;

    @Inject
    public AuthorizationClient(CssSystemAccessControlConfig config,
            PublicIdentityServiceBlockingStub identityService, IamServiceBlockingStub iamService)
    {
        requireNonNull(config, "config is null");
        this.identityService = requireNonNull(identityService, "identityServiceStub is null");
        this.iamService = requireNonNull(iamService, "iamServiceBlockingStub is null");
        this.project = config.getProjectId();
    }

    /**
     * validates User's credentials by checking whether the provided name matches the access token and whether the
     * access token is valid.
     *
     * @param securityContext
     */
    public void validateUser(SystemSecurityContext securityContext)
    {
        if (securityContext.getIdentity().getUser().equals(CSS_VIEW_OWNER)) {
            return;
        }

        String email;
        try {
            email = identityService.describeSession(DescribeSessionRequest.newBuilder()
                    .setToken(extractAccessToken(securityContext))
                    .build())
                    .getAccount().getEmail();
        }
        catch (Exception e) {
            throw new AccessDeniedException(format("Cannot get session information from identity service: %s", e));
        }

        if (!email.toLowerCase(ENGLISH).startsWith(format("%s@", securityContext.getIdentity().getUser().toLowerCase(ENGLISH)))) {
            throw new AccessDeniedException(format("User '%s' does not match credential email '%s'",
                    securityContext.getIdentity().getUser(),
                    email));
        }
    }

    /**
     * Based on the action and the list of desired resources returns a list of allowed resources (so action can be performed
     * on them).
     *
     * @param securityContext Presto security context.
     * @param action          Action to validate
     * @param resources       Resource to validate.
     * @return List of resources that action is allowed to be performed om.
     */
    public List<String> getAllowedResources(SystemSecurityContext securityContext, Actions action, Collection<String> resources)
    {
        log.info("AUTHORIZATION ALLOWED RESOURCES: action '%s' resources '%s'", action.name(), resources);
        // Only allow for selecting from views.
        if (securityContext.getIdentity().getUser().equals(CSS_VIEW_OWNER) && action == Actions.SELECT_AS_OWNER) {
            return ImmutableList.copyOf(resources);
        }

        return iamService.hasPermissionBatch(HasPermissionBatchRequest.newBuilder()
                .setAccessToken(extractAccessToken(securityContext))
                .setProjectId(project)
                .addAllPermission(resources.stream().map(resource -> createPermissionCheck(action, resource))
                        .collect(toImmutableList()))
                .build())
                .getResultList()
                .stream()
                .filter(result -> result.getHasPermission())
                .map(result -> result.getCheck().getResource())
                .collect(toImmutableList());
    }

    /**
     * Validates whether the action can be performed on the resource.
     *
     * @param securityContext
     * @param action
     * @param resource
     * @return
     */
    public boolean validatePermission(SystemSecurityContext securityContext, Actions action, String resource)
    {
        log.info("AUTHORIZATION VALIDATE: action '%s' resources '%s'", action.name(), resource);
        // Only allow for selecting from views.
        if (securityContext.getIdentity().getUser().equals(CSS_VIEW_OWNER) && action == Actions.USE && resource.startsWith(CATALOG.name())) {
            return true;
        }

        return iamService.hasPermission(HasPermissionRequest.newBuilder()
                .setAccessToken(extractAccessToken(securityContext))
                .setProjectId(project)
                .setAction(action.getAction())
                .setResource(resource)
                .build())
                .getHasPermission();
    }

    private PermissionCheck createPermissionCheck(Actions action, String resource)
    {
        return PermissionCheck.newBuilder()
                .setAction(action.getAction())
                .setResource(resource)
                .build();
    }

    private String extractAccessToken(SystemSecurityContext context)
    {
        String accessToken = context.getIdentity().getExtraCredentials().get(ACCESS_TOKEN);
        if (isNullOrEmpty(accessToken)) {
            throw new CssAuthenticationException("Access token is null or empty");
        }
        return accessToken;
    }
}
