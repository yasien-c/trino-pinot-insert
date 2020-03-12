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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.Session;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.eventlistener.EventListenerFactory;
import io.prestosql.testing.DistributedQueryRunner;

import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public class TestingCssEventListenerQueryRunner
{
    private static final String CATALOG = "tpch";
    private static final String SCHEMA = "tiny";

    private TestingCssEventListenerQueryRunner() {}

    public static DistributedQueryRunner createQueryRunner(Map<String, String> extraProperties, Map<String, String> eventListenerProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(CATALOG)
                .setSchema(SCHEMA)
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(2)
                .setExtraProperties(extraProperties)
                .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog(CATALOG, CATALOG);
            queryRunner.installPlugin(new TestingCssEventListenerPlugin(eventListenerProperties));
            return queryRunner;
        }
        catch (Exception e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    static class TestingCssEventListenerPlugin
            implements Plugin
    {
        private final Map<String, String> properties;

        public TestingCssEventListenerPlugin(Map<String, String> properties)
        {
            requireNonNull(properties, "properties is null");
            this.properties = ImmutableMap.copyOf(properties);
        }

        @Override
        public Iterable<EventListenerFactory> getEventListenerFactories()
        {
            return ImmutableList.of(new KafkaEventListenerFactory(properties));
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        //ImmutableMap.of("http-server.http.port", "8080"),
        DistributedQueryRunner queryRunner = createQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.<String, String>builder()
                        .put("kafka-event-listener.query-log-topic", "query_logs")
                .put("kafka-event-listener.kafka-nodes", "localhost:32820")
                .put("kafka-event-listener.schema-registry-url", "http://localhost:32822")
                .build());

        Thread.sleep(10);
        Logger log = Logger.get(TestingCssEventListenerQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
