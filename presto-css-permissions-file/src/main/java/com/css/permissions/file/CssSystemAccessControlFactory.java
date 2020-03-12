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
import com.css.identity.server.api.PublicIdentityServiceGrpc.PublicIdentityServiceBlockingStub;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.prestosql.spi.security.SystemAccessControl;
import io.prestosql.spi.security.SystemAccessControlFactory;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class CssSystemAccessControlFactory
        implements SystemAccessControlFactory
{
    @Override
    public String getName()
    {
        return "css-access-control-file";
    }

    @Override
    public SystemAccessControl create(Map<String, String> config)
    {
        Bootstrap app = new Bootstrap(
                binder -> {
                    configBinder(binder).bindConfig(CssSystemAccessControlConfig.class);
                    binder.bind(CssDirectorySystemAccessControl.class).in(Scopes.SINGLETON);
                    binder.bind(PublicIdentityServiceBlockingStub.class).toProvider(
                            IdentityServiceProvider.class).in(Scopes.SINGLETON);
                    binder.bind(DirectoryProxyServerBlockingStub.class).toProvider(
                            DirectoryProxyServerProvider.class).in(Scopes.SINGLETON);
                    binder.bind(CssDirectoryClient.class).to(DefaultCssDirectoryClient.class).in(Scopes.SINGLETON);
                });

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();
        return injector.getInstance(CssDirectorySystemAccessControl.class);
    }
}
