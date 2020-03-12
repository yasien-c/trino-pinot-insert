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
package com.css.eventlistener;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.spi.eventlistener.EventListener;
import io.prestosql.spi.eventlistener.EventListenerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.css.eventlistener.KafkaEventLogger.NAME;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class KafkaEventListenerFactory
        implements EventListenerFactory
{
    private final Map<String, String> defaultProperties;

    public KafkaEventListenerFactory()
    {
        this(ImmutableMap.of());
    }

    @VisibleForTesting
    public KafkaEventListenerFactory(Map<String, String> defaultProperties)
    {
        requireNonNull(defaultProperties, "defaultProperties is null");
        this.defaultProperties = ImmutableMap.copyOf(defaultProperties);
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        ImmutableList.Builder<Module> moduleBuilder = ImmutableList.builder();
        moduleBuilder
                .add(new JsonModule())
                .add(binder -> {
                    binder.bind(ClassLoader.class).toInstance(KafkaEventListenerFactory.class.getClassLoader());
                    configBinder(binder).bindConfig(KafkaEventLoggerConfig.class);
                    binder.bind(EventLogger.class).to(KafkaEventLogger.class).in(Scopes.SINGLETON);
                    binder.bind(KafkaEventLogger.class).in(Scopes.SINGLETON);
                    binder.bind(EventListener.class).to(KafkaEventListener.class).in(Scopes.SINGLETON);
                    binder.bind(KafkaEventListener.class).in(Scopes.SINGLETON);
                });
        try {
            Bootstrap app = new Bootstrap(moduleBuilder.build());
            Map<String, String> mergedConfig = new HashMap<>(defaultProperties);
            mergedConfig.putAll(config);
            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(mergedConfig)
                    .initialize();
            return injector.getInstance(EventListener.class);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
