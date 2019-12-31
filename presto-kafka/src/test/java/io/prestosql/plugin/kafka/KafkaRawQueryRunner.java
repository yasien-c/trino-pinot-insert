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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.Session;
import io.prestosql.testing.DistributedQueryRunner;

import java.util.Map;

import static io.prestosql.testing.TestingSession.testSessionBuilder;

public final class KafkaRawQueryRunner
{
    private KafkaRawQueryRunner() {}

    private static final Logger log = Logger.get(KafkaRawQueryRunner.class);

    static DistributedQueryRunner createKafkaQueryRunner(Map<String, String> extraProperties, Map<String, String> extraKafkaProperties)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setNodeCount(2)
                .setExtraProperties(extraProperties)
                .build();
        queryRunner.installPlugin(new KafkaPlugin());
        queryRunner.createCatalog("kafka", "kafka", extraKafkaProperties);
        return queryRunner;
    }

    private static Session createSession()
    {
        return testSessionBuilder()
                .setCatalog("kafka")
                .setSchema("default")
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        Map<String, String> kafkaProperties = ImmutableMap.<String, String>builder()
                .put("kafka.nodes", "10.252.24.253:9092")
                .put("kafka.schema-registry-url", "http://10.252.27.244:8081")
                .put("kafka.table-names", "order_central_avro_updates")
                //.put("kafka.nodes", "localhost:32888")
                //.put("kafka.schema-registry-url", "http://localhost:8081")
                //.put("kafka.table-names", "foo,bar")
                .put("kafka.hide-internal-columns", "false")
                .put("kafka.table-description-dir", "/Users/elon/repos/tmp/kafka_testing")

                .build();
        DistributedQueryRunner queryRunner = createKafkaQueryRunner(properties, kafkaProperties);

        Thread.sleep(10);
        Logger log = Logger.get(KafkaRawQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
