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
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.Test;

public abstract class AbstractTestKafkaWithSchemaRegistry
        extends AbstractTestQueryFramework
{
    private static final String SCHEMA_TABLE_NAME = "default.foo";
    private static final String SCHEMA_REGISTRY_TABLE_NAME = "default.bar";

    protected abstract String getSchemaTableName();

    protected String getSchemaRegistryTableName()
    {
        return null;
    }

    @Test
    public void testWithTableSchemaReadFromFile()
    {
        runTestQueries(getSchemaTableName());
    }

    @Test
    public void testWithTableSchemaReadFromSchemaRegistry()
    {
        skipTestUnless(getSchemaRegistryTableName() != null);
        runTestQueries(getSchemaRegistryTableName());
    }

    private void runTestQueries(String schemaTableName)
    {
        assertQuery("SELECT COUNT(*) FROM " + schemaTableName, "VALUES 20");
        assertQuery("SELECT COUNT(*) FROM " + schemaTableName + " WHERE f2 IS NULL AND f3 IS NULL AND f4 IS NULL AND f5 IS NULL", "VALUES 10");
        assertQuery("SELECT ELEMENT_AT(f3, 1), ELEMENT_AT(f3, 2) FROM " + schemaTableName + " WHERE f2 = 5", "VALUES (10, 11)");
        assertQuery("SELECT ELEMENT_AT(f4, 'mapKey2'), ELEMENT_AT(f4, 'mapKey1') FROM " + schemaTableName + " WHERE f2 = 7", "VALUES (107, 17)");
        assertQuery("SELECT f5.field1, ELEMENT_AT(f5.field3, 2) FROM " + schemaTableName + " WHERE f2 = 9", "VALUES ('subfield9', 65)");
    }

    static void produceRecords(TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry)
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
            }
            else {
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
        }
        finally {
            producer.flush();
            producer.close();
        }
    }
}
