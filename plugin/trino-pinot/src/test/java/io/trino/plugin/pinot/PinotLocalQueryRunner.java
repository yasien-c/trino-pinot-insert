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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.connector.CatalogServiceProvider;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.pinot.client.PinotHostMapper;
import io.trino.plugin.postgresql.PostgreSqlPlugin;
import io.trino.spi.session.PropertyMetadata;
import io.trino.testing.DistributedQueryRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.testing.TestingHandles.createTestCatalogHandle;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class PinotLocalQueryRunner
{
    public static final String PINOT_CATALOG = "pinot";

    private PinotLocalQueryRunner() {}

    public static DistributedQueryRunner createPinotQueryRunner(Map<String, String> extraProperties, Map<String, String> extraPinotProperties, Optional<Module> extension)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession("default"))
                .setNodeCount(2)
                .setExtraProperties(extraProperties)
                .build();
        queryRunner.installPlugin(new PinotPlugin(extension));
        queryRunner.createCatalog(PINOT_CATALOG, "pinot", extraPinotProperties);
        return queryRunner;
    }

    public static Session createSession(String schema)
    {
        return createSessionBuilder(schema, new PinotConfig()).build();
    }

    public static Session.SessionBuilder createSessionBuilder(String schema, PinotConfig config)
    {
        PinotSessionProperties pinotSessionProperties = new PinotSessionProperties(config);
        SessionPropertyManager sessionPropertyManager = new SessionPropertyManager(
                ImmutableSet.of(new SystemSessionProperties()),
                CatalogServiceProvider.singleton(createTestCatalogHandle(PINOT_CATALOG), Maps.uniqueIndex(pinotSessionProperties.getSessionProperties(), PropertyMetadata::getName)));
        return testSessionBuilder(sessionPropertyManager)
                .setCatalog(PINOT_CATALOG)
                .setSchema(schema);
    }

    public static void main(String[] args)
            throws Exception
    {
        int minioPort = 58478;
        int brokerPort = 58496;
        String controllerUrl = "localhost:58490";
        Map<String, Integer> serverPortMap = Map.of("b6dca12575ea", 58506, "71657fd54bbd", 58516, "f41a0dd8f851", 58511, "fe1b962e3b6c", 58500);
        Map<String, Integer> grpcPortMap = Map.of("b6dca12575ea", 58504, "71657fd54bbd", 58517, "f41a0dd8f851", 58512, "fe1b962e3b6c", 58498);
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        Map<String, String> pinotProperties = ImmutableMap.<String, String>builder()
                .put("pinot.controller-urls", controllerUrl)
                .put("pinot.segments-per-split", "10")
                .put("pinot.deep-store-provider", "S3")
                .put("pinot.s3-accesskey-file", "/Users/elon.azoulay/tmp/etc/accesskey")
                .put("pinot.s3-secretkey-file", "/Users/elon.azoulay/tmp/etc/secretkey")
                .put("pinot.s3-endpoint", "http://localhost:%s".formatted(minioPort))
                .put("pinot.s3-region", "centralus")
                .put("pinot.deep-store-uri", "s3://scratch/pinot_testing_1")
                .put("pinot.segment-creation-base-directory", "/Users/elon.azoulay/tmp/data")
                .buildOrThrow();
        DistributedQueryRunner queryRunner = createPinotQueryRunner(properties,
                pinotProperties,
                Optional.of(binder -> newOptionalBinder(binder, PinotHostMapper.class).setBinding()
                        .toInstance(new TestingMultiServerPinotHostMapper(
                                HostAndPort.fromParts("localhost", brokerPort),
                                serverPortMap,
                                grpcPortMap))));
        Map<String, String> postgresqlProperties = new HashMap<>();
        postgresqlProperties.put("connection-url", "jdbc:postgresql://localhost:5432/postgres");
        postgresqlProperties.put("connection-user", "elon.azoulay");
        postgresqlProperties.put("decimal-mapping", "ALLOW_OVERFLOW");
        postgresqlProperties.put("decimal-rounding-mode", "HALF_UP");
        postgresqlProperties.put("decimal-default-scale", "5");
        queryRunner.installPlugin(new PostgreSqlPlugin());
        queryRunner.createCatalog("postgres", "postgresql", postgresqlProperties);
        Thread.sleep(10);
        Logger log = Logger.get(PinotLocalQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
