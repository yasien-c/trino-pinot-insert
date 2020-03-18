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

import com.css.eventlistener.KafkaEventListener.EventType;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.kafka.util.TestingKafkaWithSchemaRegistry;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.EnumSet;

import static com.css.eventlistener.TestingCssEventListenerQueryRunner.createQueryRunner;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCssEventListener
{
    private static final String TOPIC = "presto_query_logs";
    private static final EnumSet<EventType> EVENT_TYPES = EnumSet.allOf(EventType.class);

    private TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry;
    private QueryRunner queryRunner;

    @BeforeClass
    public void setup()
            throws Exception
    {
        testingKafkaWithSchemaRegistry = new TestingKafkaWithSchemaRegistry();
        testingKafkaWithSchemaRegistry.start();
        testingKafkaWithSchemaRegistry.createTopics(TOPIC);
        queryRunner = createQueryRunner(
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("kafka-event-listener.query-log-topic", TOPIC)
                        .put("kafka-event-listener.kafka-nodes", testingKafkaWithSchemaRegistry.getConnectString())
                        .put("kafka-event-listener.schema-registry-url", testingKafkaWithSchemaRegistry.getSchemaRegistryConnectString())
                        .build());
    }

    @AfterClass(alwaysRun = true)
    public void close()
    {
        closeAllRuntimeException(queryRunner, testingKafkaWithSchemaRegistry);
    }

    @Test
    public void testQueryLogging()
            throws Exception
    {
        MaterializedResult result = queryRunner.execute("select now()");
        assertEquals(result.getRowCount(), 1);
        // Wait for event listener to publish events
        Thread.sleep(2000);
        try (KafkaConsumer<String, GenericRecord> consumer = testingKafkaWithSchemaRegistry.createKafkaAvroConsumer(StringDeserializer.class, String.class)) {
            consumer.assign(consumer.partitionsFor(TOPIC).stream()
                    .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))
                    .collect(toImmutableList()));
            ConsumerRecords<String, GenericRecord> records = consumer.poll(1000);
            assertEquals(records.count(), 2);
            for (ConsumerRecord<String, GenericRecord> record : records) {
                assertEquals(record.value().get("query").toString(), "select now()");
                assertTrue(EVENT_TYPES.contains(EventType.valueOf(record.value().get("event_type").toString())));
            }
        }
    }
}
