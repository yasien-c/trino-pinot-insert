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

import java.security.Principal;
import java.util.Collection;

import static java.util.Objects.requireNonNull;

public class CssPrincipal
        implements Principal
{
    private final String name;
    private final String email;
    private final Collection<String> roles;

    public CssPrincipal(String name, String email, Collection<String> roles)
    {
        this.name = requireNonNull(name, "name is null");
        this.email = requireNonNull(email, "email is null");
        this.roles = requireNonNull(roles, "roles are null");
    }

    @Override
    public String getName()
    {
        return name;
    }

    public String getEmail()
    {
        return email;
    }

    public Collection<String> getRoles()
    {
        return roles;
    }

    public static final class Builder
    {
        private String name;
        private String email;
        private Collection<String> roles;

        private Builder()
        {
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public Builder withName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder withEmail(String email)
        {
            this.email = email;
            return this;
        }

        public Builder withRoles(Collection<String> roles)
        {
            this.roles = roles;
            return this;
        }

        public CssPrincipal build()
        {
            return new CssPrincipal(name, email, roles);
        }
    }
}
