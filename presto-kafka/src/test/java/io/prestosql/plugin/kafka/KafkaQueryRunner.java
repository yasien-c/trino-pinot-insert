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
package io.prestosql.plugin.kafka;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Level;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.kafka.lookup.MapBasedTopicDescriptionLookup;
import io.prestosql.plugin.kafka.util.CodecSupplier;
import io.prestosql.plugin.kafka.util.TestUtils;
import io.prestosql.plugin.kafka.util.TestingKafka;
import io.prestosql.plugin.kafka.util.TestingKafkaWithSchemaRegistry;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.TestingPrestoClient;
import io.prestosql.tpch.TpchTable;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.plugin.kafka.util.AvroSchemaRegistryTestUtils.DEFAULT_SCHEMA;
import static io.prestosql.plugin.kafka.util.AvroSchemaRegistryTestUtils.createTopicDescriptions;
import static io.prestosql.plugin.kafka.util.TestUtils.loadTpchTopicDescription;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class KafkaQueryRunner
{
    private KafkaQueryRunner() {}

    private static final Logger log = Logger.get(KafkaQueryRunner.class);
    private static final String TPCH_SCHEMA = "tpch";

    static DistributedQueryRunner createKafkaQueryRunner(TestingKafka testingKafka, TpchTable<?>... tables)
            throws Exception
    {
        return createKafkaQueryRunner(testingKafka, ImmutableList.copyOf(tables));
    }

    static DistributedQueryRunner createKafkaQueryRunner(TestingKafka testingKafka, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createKafkaQueryRunner(testingKafka, tables, ImmutableMap.of());
    }

    static DistributedQueryRunner createKafkaQueryRunner(TestingKafka testingKafka, Iterable<TpchTable<?>> tables, Map<SchemaTableName, KafkaTopicDescription> topicDescription)
            throws Exception
    {
        return createKafkaQueryRunner(testingKafka, ImmutableMap.of(), tables, topicDescription);
    }

    static DistributedQueryRunner createKafkaQueryRunner(
            TestingKafka testingKafka,
            Map<String, String> extraKafkaProperties,
            Iterable<TpchTable<?>> tables,
            Map<SchemaTableName, KafkaTopicDescription> extraTopicDescription)
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.kafka", Level.WARN);

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createSession()).build();

            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            testingKafka.start();

            for (TpchTable<?> table : tables) {
                testingKafka.createTopics(kafkaTopicName(table));
            }

            Map<SchemaTableName, KafkaTopicDescription> tpchTopicDescriptions = createTpchTopicDescriptions(queryRunner.getCoordinator().getMetadata(), tables);

            Map<SchemaTableName, KafkaTopicDescription> topicDescriptions = ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                    .putAll(extraTopicDescription)
                    .putAll(tpchTopicDescriptions)
                    .build();
            KafkaPlugin kafkaPlugin = new KafkaPlugin();
            kafkaPlugin.setTopicDescriptionLookup(new MapBasedTopicDescriptionLookup(topicDescriptions));
            queryRunner.installPlugin(kafkaPlugin);

            Map<String, String> kafkaProperties = new HashMap<>(ImmutableMap.copyOf(extraKafkaProperties));
            kafkaProperties.putIfAbsent("kafka.nodes", testingKafka.getConnectString());
            kafkaProperties.putIfAbsent("kafka.table-names", Joiner.on(",").join(topicDescriptions.keySet()));
            kafkaProperties.putIfAbsent("kafka.connect-timeout", "120s");
            kafkaProperties.putIfAbsent("kafka.default-schema", "default");
            kafkaProperties.putIfAbsent("kafka.messages-per-split", "1000");
            queryRunner.createCatalog("kafka", "kafka", kafkaProperties);

            TestingPrestoClient prestoClient = queryRunner.getClient();

            log.info("Loading data...");
            long startTime = System.nanoTime();
            for (TpchTable<?> table : tables) {
                loadTpchTopic(testingKafka, prestoClient, table);
            }
            log.info("Loading complete in %s", nanosSince(startTime).toString(SECONDS));

            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, testingKafka);
            throw e;
        }
    }

    static DistributedQueryRunner createKafkaQueryRunner(
            TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry,
            Map<String, String> extraProperties,
            Map<String, String> extraKafkaProperties,
            String resourcePath,
            Path destinationPath)
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.kafka", Level.WARN);

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createAvroSchemaRegistrySession())
                    .setNodeCount(2)
                    .setExtraProperties(extraProperties)
                    .build();

            testingKafkaWithSchemaRegistry.start();

            Map<SchemaTableName, KafkaTopicDescription> topicDescriptions = createTopicDescriptions(queryRunner.getCoordinator().getMetadata(), resourcePath, destinationPath);

            topicDescriptions.values().stream()
                    .forEach(topicDescription -> testingKafkaWithSchemaRegistry.createTopics(topicDescription.getTopicName()));

            KafkaPlugin kafkaPlugin = new KafkaPlugin();
            kafkaPlugin.setTopicDescriptionLookup(new MapBasedTopicDescriptionLookup(topicDescriptions));
            queryRunner.installPlugin(kafkaPlugin);
            Map<String, String> extraKafkaPropertiesWithTables = ImmutableMap.<String, String>builder()
                    .putAll(extraKafkaProperties)
                    .put("kafka.table-names", Joiner.on(",").join(topicDescriptions.keySet()))
                    .build();
            return createKafkaQueryRunner(testingKafkaWithSchemaRegistry, queryRunner, extraKafkaPropertiesWithTables);
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, testingKafkaWithSchemaRegistry);
            throw e;
        }
    }

    static DistributedQueryRunner createKafkaQueryRunner(
            TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry,
            Map<String, String> extraProperties,
            Map<String, String> extraKafkaProperties)
            throws Exception
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.kafka", Level.WARN);

        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = DistributedQueryRunner.builder(createAvroSchemaRegistrySession())
                    .setNodeCount(2)
                    .setExtraProperties(extraProperties)
                    .build();
            queryRunner.installPlugin(new KafkaPlugin());
            Map<String, String> extraKafkaPropertiesWithSchemaRegistryLookup = ImmutableMap.<String, String>builder()
                    .putAll(extraKafkaProperties)
                    .put("kafka.topic-description-lookup", "schema-registry")
                    .build();
            testingKafkaWithSchemaRegistry.start();
            return createKafkaQueryRunner(testingKafkaWithSchemaRegistry, queryRunner, extraKafkaPropertiesWithSchemaRegistryLookup);
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner, testingKafkaWithSchemaRegistry);
            throw e;
        }
    }

    private static DistributedQueryRunner createKafkaQueryRunner(
            TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry,
            DistributedQueryRunner queryRunner,
            Map<String, String> extraKafkaProperties)
    {
        Map<String, String> kafkaProperties = new HashMap<>(ImmutableMap.copyOf(extraKafkaProperties));
        kafkaProperties.putIfAbsent("kafka.nodes", testingKafkaWithSchemaRegistry.getConnectString());
        kafkaProperties.putIfAbsent("kafka.connect-timeout", "120s");
        kafkaProperties.putIfAbsent("kafka.default-schema", "default");
        kafkaProperties.putIfAbsent("kafka.messages-per-split", "1000");
        kafkaProperties.putIfAbsent("kafka.schema-registry-url", testingKafkaWithSchemaRegistry.getSchemaRegistryConnectString());
        queryRunner.createCatalog("kafka", "kafka", kafkaProperties);
        return queryRunner;
    }

    private static void loadTpchTopic(TestingKafka testingKafka, TestingPrestoClient prestoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        TestUtils.loadTpchTopic(testingKafka, prestoClient, kafkaTopicName(table), new QualifiedObjectName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH)));
        log.info("Imported %s in %s", 0, table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static String kafkaTopicName(TpchTable<?> table)
    {
        return TPCH_SCHEMA + "." + table.getTableName().toLowerCase(ENGLISH);
    }

    private static Map<SchemaTableName, KafkaTopicDescription> createTpchTopicDescriptions(Metadata metadata, Iterable<TpchTable<?>> tables)
            throws Exception
    {
        JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec = new CodecSupplier<>(KafkaTopicDescription.class, metadata).get();
        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> topicDescriptions = ImmutableMap.builder();
        for (TpchTable<?> table : tables) {
            String tableName = table.getTableName();
            SchemaTableName tpchTable = new SchemaTableName(TPCH_SCHEMA, tableName);

            topicDescriptions.put(loadTpchTopicDescription(topicDescriptionJsonCodec, tpchTable.toString(), tpchTable));
        }
        return topicDescriptions.build();
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("kafka")
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    private static Session createAvroSchemaRegistrySession()
    {
        return testSessionBuilder()
                .setCatalog("kafka")
                .setSchema(DEFAULT_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        DistributedQueryRunner queryRunner = createKafkaQueryRunner(new TestingKafka(), TpchTable.getTables());
        Thread.sleep(10);
        Logger log = Logger.get(KafkaQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
