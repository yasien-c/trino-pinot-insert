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

import com.css.identity.server.api.PublicIdentityServiceGrpc.PublicIdentityServiceBlockingStub;
import com.css.identity.server.api.SignInRequest;
import com.css.identity.server.api.SignInResponse;
import com.google.common.base.Splitter;
import io.prestosql.spi.security.PasswordAuthenticator;

import javax.inject.Inject;

import java.security.Principal;
import java.util.List;
import java.util.Optional;

import static com.css.permissions.AuthorizationClient.ACCESS_TOKEN;
import static java.util.Objects.requireNonNull;

public class CssPasswordAuthenticator
        implements PasswordAuthenticator
{
    private final PublicIdentityServiceBlockingStub identityServiceStub;

    @Inject
    public CssPasswordAuthenticator(PublicIdentityServiceBlockingStub identityServiceStub)
    {
        requireNonNull(identityServiceStub, "identityServiceStub is null");
        this.identityServiceStub = identityServiceStub;
    }

    /*
     * If password contains encoded access token then extract it otherwise use password authentication
     */
    @Override
    public Principal createAuthenticatedPrincipal(String user, String password)
    {
        Optional<String> accessToken = extractAccessToken(password);
        if (accessToken.isPresent()) {
            return new CssPrincipal(user, accessToken.get());
        }
        SignInResponse response = identityServiceStub.signIn(SignInRequest.newBuilder()
                .setEmail(user)
                .setPassword(password)
                .build());
        return new CssPrincipal(user, response.getTokens().getAccessToken());
    }

    private static Optional<String> extractAccessToken(String credentials)
    {
        if (credentials.startsWith(ACCESS_TOKEN)) {
            List<String> parts = Splitter.on('=').limit(2).splitToList(credentials);
            if (parts.size() != 2 || parts.stream().anyMatch(String::isEmpty)) {
                throw new CssAuthenticationException("Malformed decoded credentials");
            }
            return Optional.of(parts.get(1));
        }
        else {
            return Optional.empty();
        }
    }
}
