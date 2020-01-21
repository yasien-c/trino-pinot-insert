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
import io.prestosql.plugin.kafka.util.TestingKafkaWithSchemaRegistry;
import io.prestosql.testing.QueryRunner;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.kafka.KafkaQueryRunner.createKafkaQueryRunner;

@Test(singleThreaded = true)
public class TestKafkaWithSchemaRegistryLookup
        extends AbstractTestKafkaWithSchemaRegistry
{
    private static final String TABLE_NAME = "default.\"bar&raw&&key_col=bigint#dataformat=int&avro-confluent&&\"";

    private TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry;

    @Override
    protected String getSchemaTableName()
    {
        return "default.\"foo&raw&&key_col=varchar&avro-confluent&&\"";
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafkaWithSchemaRegistry = new TestingKafkaWithSchemaRegistry();
        QueryRunner queryRunner = createKafkaQueryRunner(testingKafkaWithSchemaRegistry,
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.<String, String>builder()
                        .put("kafka.hide-internal-columns", "false")
                        .build());
        testingKafkaWithSchemaRegistry.createTopics("foo");
        produceRecords(testingKafkaWithSchemaRegistry);
        testingKafkaWithSchemaRegistry.createTopics("bar");
        produceRecordsWithRawKeyMapping(testingKafkaWithSchemaRegistry);
        Thread.sleep(1000);
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void stopKafkaWithSchemaRegistry()
    {
        testingKafkaWithSchemaRegistry.close();
        testingKafkaWithSchemaRegistry = null;
    }

    @Test
    public void testMessageWithLongKeys()
    {
        assertQuery("SELECT f1 FROM " + TABLE_NAME + " WHERE key_col = 3", "VALUES ('value3')");
        assertQuery("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE key_col > 3", "VALUES (6)");
        assertQuery("SELECT MAX(key_col) FROM " + TABLE_NAME, "VALUES (9)");
        assertQuery("SELECT MAX_BY(f1, key_col) FROM " + TABLE_NAME, "VALUES ('value9')");
    }

    private void produceRecordsWithRawKeyMapping(TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry)
    {
        KafkaProducer<Integer, GenericRecord> producer = testingKafkaWithSchemaRegistry.createKafkaAvroProducer(IntegerSerializer.class, Integer.class);
        Schema schema = SchemaBuilder.record("myrecord")
                .fields()
                .name("f1").type().stringType().noDefault()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<Integer, GenericRecord>> builder = ImmutableList.builder();
        for (int i = 0; i < 10; i++) {
            GenericRecord avroRecord = new GenericData.Record(schema);
            avroRecord.put("f1", "value" + i);
            builder.add(new ProducerRecord<>("bar", i, avroRecord));
        }
        try {
            for (ProducerRecord<Integer, GenericRecord> record : builder.build()) {
                producer.send(record);
            }
        }
        finally {
            producer.flush();
            producer.close();
        }
    }
}
