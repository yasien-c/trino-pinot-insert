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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.Session;
import io.prestosql.plugin.kafka.util.TestingKafkaWithSchemaRegistry;
import io.prestosql.testing.DistributedQueryRunner;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.Test;

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
                .put("kafka.topic-description-lookup", "schema-registry")
                //.put("kafka.table-names", "order_central_avro_updates")
                //.put("kafka.nodes", "localhost:32863")
                //.put("kafka.schema-registry-url", "http://localhost:8081")
                //.put("kafka.table-names", "foo,caz")
                .put("kafka.hide-internal-columns", "false")
                .put("kafka.table-description-dir", "/Users/elon/repos/tmp/kafka_testing/topics_local")
                .build();
        DistributedQueryRunner queryRunner = createKafkaQueryRunner(properties, kafkaProperties);

        Thread.sleep(10);
        Logger log = Logger.get(KafkaRawQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    @Test
    public void test()
            throws Exception
    {
        TestingKafkaWithSchemaRegistry container = new TestingKafkaWithSchemaRegistry();
        container.start();
        container.createTopics("foo");
        KafkaProducer<String, GenericRecord> producer = container.createKafkaAvroProducer();

        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        Schema schema2 = SchemaBuilder.record("myrecord")
                .fields()
                .name("f1").type().stringType().noDefault()
                .name("f2").type().optional().intType()
                .name("f3").type().optional().array().items().longType()
                .name("f4").type().optional().map().values().longType()
                .name("f5").type().optional().record("subRecord").fields()
                .name("field1")
                .type().stringType().noDefault()
                .name("field2").type().longType().noDefault()
                .name("field3").type().array().items().longType().noDefault()
                .endRecord()
                .endRecord();

        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> builder = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            String key = "key0-" + i;
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("f1", "value" + i);
            builder.add(new ProducerRecord<>("foo", key, avroRecord));
        }
        for (int i = 0; i < 10; i++) {
            String key = "key1-" + i;
            GenericRecord avroRecord = new GenericData.Record(schema2);
            avroRecord.put("f1", "value" + i);
            avroRecord.put("f2", i);
            if (i == 8) {
                avroRecord.put("f3", null);
                avroRecord.put("f4", null);
                avroRecord.put("f5", null);
            }
            else {
                avroRecord.put("f3", ImmutableList.of(5L + i, 6L + i));
                avroRecord.put("f4", ImmutableMap.builder().put("mapKey1", 10L + i).put("mapKey2", 100L + i).put("mapKey3", 1000L + i).build());
                GenericRecord subField = new GenericData.Record(schema2.getField("f5").schema().getTypes().get(1));
                subField.put("field1", "subfield" + i);
                subField.put("field2", 100L + i);
                subField.put("field3", ImmutableList.of(55L + i, 56L + i, 57L + i));
                avroRecord.put("f5", subField);
            }
            builder.add(new ProducerRecord<>("foo", key, avroRecord));
        }
        try {
            for (ProducerRecord<String, GenericRecord> record : builder.build()) {
                producer.send(record);
            }
        }
        finally {
            producer.flush();
            producer.close();
        }
        System.err.println("KAFKA CONNECT: " + container.getConnectString());
        System.err.println("SCHEMA REGISTRY CONNECT: " + container.getSchemaRegistryConnectString());
        Thread.sleep(Long.MAX_VALUE);
    }
}
