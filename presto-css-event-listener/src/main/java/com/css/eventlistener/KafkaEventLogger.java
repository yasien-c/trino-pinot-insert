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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.spi.HostAddress;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Set;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class KafkaEventLogger
        implements EventLogger<GenericRecord>
{
    private static final Logger log = Logger.get(KafkaEventLogger.class);
    public static final String NAME = "kafka";

    private final KafkaProducer<String, GenericRecord> kafkaProducer;
    private final String queryLogTopic;
    private final String queryLogEntryKeyField;

    @Inject
    public KafkaEventLogger(KafkaEventLoggerConfig config)
    {
        requireNonNull(config, "config is null");
        kafkaProducer = createKafkaProducer(config.getKafkaNodes(), config.getSchemaRegistryUrl());
        queryLogTopic = config.getQueryLogTopic();
        queryLogEntryKeyField = config.getQueryLogEntryKeyField();
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void log(GenericRecord event)
    {
        try {
            kafkaProducer.send(new ProducerRecord<>(queryLogTopic, event.get(queryLogEntryKeyField).toString(), event));
        }
        catch (Exception e) {
            log.error(e);
        }
    }

    @Override
    public void close()
    {
        try {
            kafkaProducer.flush();
        }
        finally {
            kafkaProducer.close();
        }
    }

    private KafkaProducer<String, GenericRecord> createKafkaProducer(Set<HostAddress> nodes, HostAddress schemaRegistryUrl)
    {
        Properties properties = new Properties();
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl.getHostText());
        properties.put(
                BOOTSTRAP_SERVERS_CONFIG,
                nodes.stream()
                        .map(HostAddress::toString)
                        .collect(joining(",")));
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        return new KafkaProducer<>(properties);
    }
}
