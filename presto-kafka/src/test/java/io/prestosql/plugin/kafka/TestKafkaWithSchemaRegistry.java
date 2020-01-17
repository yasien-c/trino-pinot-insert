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
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.plugin.kafka.KafkaQueryRunner.createKafkaQueryRunner;

@Test(singleThreaded = true)
public class TestKafkaWithSchemaRegistry
        extends AbstractTestQueryFramework
{
    private TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry;

    @BeforeClass
    public void setup()
            throws Exception
    {
        testingKafkaWithSchemaRegistry = new TestingKafkaWithSchemaRegistry();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafkaWithSchemaRegistry = new TestingKafkaWithSchemaRegistry();
        QueryRunner queryRunner = createKafkaQueryRunner(testingKafkaWithSchemaRegistry, ImmutableMap.of("http-server.http.port", "8080"), ImmutableMap.of(), "/avro");
        produceRecords();
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void stopKafkaWithSchemaRegistry()
    {
        testingKafkaWithSchemaRegistry.close();
        testingKafkaWithSchemaRegistry = null;
    }

    @Test
    public void testRecordsWithEvolvingSchema()
            throws Exception
    {
        Thread.sleep(Long.MAX_VALUE);
    }

    private void produceRecords()
    {
        KafkaProducer<String, GenericRecord> producer = testingKafkaWithSchemaRegistry.createKafkaAvroProducer();
        Schema schemaV1 = SchemaBuilder.record("myrecord")
                .fields()
                .name("f1").type().stringType().noDefault()
                .endRecord();
        Schema schemaV2 = SchemaBuilder.record("myrecord")
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
            GenericRecord avroRecord = new GenericData.Record(schemaV1);
            avroRecord.put("f1", "value" + i);
            builder.add(new ProducerRecord<>("foo", key, avroRecord));
        }
        for (int i = 0; i < 10; i++) {
            String key = "key1-" + i;
            GenericRecord avroRecord = new GenericData.Record(schemaV2);
            avroRecord.put("f1", "value" + i);
            avroRecord.put("f2", i);
            if (i == 8) {
                avroRecord.put("f3", null);
                avroRecord.put("f4", null);
                avroRecord.put("f5", null);
            }  else {
                avroRecord.put("f3", ImmutableList.of(5L + i, 6L + i));
                avroRecord.put("f4", ImmutableMap.builder().put("mapKey1", 10L + i).put("mapKey2", 100L + i).put("mapKey3", 1000L + i).build());
                GenericRecord subField = new GenericData.Record(schemaV2.getField("f5").schema().getTypes().get(1));
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
        } finally {
            producer.flush();
            producer.close();
        }
    }
}
