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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.prestosql.spi.HostAddress;

import javax.validation.constraints.Size;

import java.util.Set;
import java.util.stream.StreamSupport;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class KafkaEventLoggerConfig
{
    private static final int KAFKA_DEFAULT_PORT = 9092;

    private Set<HostAddress> kafkaNodes = ImmutableSet.of();
    private HostAddress schemaRegistryUrl;
    private int schemaRegistryCapacity = 1000;
    private String queryLogTopic;
    private String queryLogEntryKeyField = "query_id";
    private int maxQueueLength = 1000;

    public String getQueryLogTopic()
    {
        return queryLogTopic;
    }

    @Config("kafka-event-listener.query-log-topic")
    @ConfigDescription("Kafka topic used for query logs")
    public KafkaEventLoggerConfig setQueryLogTopic(String queryLogTopic)
    {
        this.queryLogTopic = requireNonNull(queryLogTopic, "queryLogTopic is null");
        return this;
    }

    public String getQueryLogEntryKeyField()
    {
        return queryLogEntryKeyField;
    }

    @Config("kafka-event-listener.query-log-entry-key-field")
    @ConfigDescription("Event field to use as key to kafka log entry record")
    public KafkaEventLoggerConfig setQueryLogEntryKeyField(String queryLogEntryKeyField)
    {
        this.queryLogEntryKeyField = queryLogEntryKeyField;
        return this;
    }

    @Size(min = 1)
    public Set<HostAddress> getKafkaNodes()
    {
        return kafkaNodes;
    }

    @Config("kafka-event-listener.kafka-nodes")
    @ConfigDescription("Seed nodes for Kafka cluster. At least one must exist")
    public KafkaEventLoggerConfig setKafkaNodes(String kafkaNodes)
    {
        this.kafkaNodes = (kafkaNodes == null) ? null : parseNodes(kafkaNodes);
        return this;
    }

    public HostAddress getSchemaRegistryUrl()
    {
        return schemaRegistryUrl;
    }

    @Config("kafka-event-listener.schema-registry-url")
    @ConfigDescription("Confluent schema registry url")
    public KafkaEventLoggerConfig setSchemaRegistryUrl(String schemaRegistryUrl)
    {
        this.schemaRegistryUrl = HostAddress.fromString(schemaRegistryUrl);
        return this;
    }

    public int getSchemaRegistryCapacity()
    {
        return schemaRegistryCapacity;
    }

    @Config("kafka-event-listener.schema-registry-capacity")
    @ConfigDescription("Size of schema registry cache")
    public KafkaEventLoggerConfig setSchemaRegistryCapacity(int schemaRegistryCapacity)
    {
        this.schemaRegistryCapacity = schemaRegistryCapacity;
        return this;
    }

    public int getMaxQueueLength()
    {
        return maxQueueLength;
    }

    @Config("kafka-event-listener.max-queue-length")
    @ConfigDescription("Size of message buffer queue. Messages are queued and sent to kafka in a background thread")
    public KafkaEventLoggerConfig setMaxQueueLength(int maxQueueLength)
    {
        this.maxQueueLength = maxQueueLength;
        return this;
    }

    private static ImmutableSet<HostAddress> parseNodes(String nodes)
    {
        Splitter splitter = Splitter.on(',').omitEmptyStrings().trimResults();
        return StreamSupport.stream(splitter.split(nodes).spliterator(), false)
                .map(KafkaEventLoggerConfig::toHostAddress)
                .collect(toImmutableSet());
    }

    private static HostAddress toHostAddress(String value)
    {
        return HostAddress.fromString(value).withDefaultPort(KAFKA_DEFAULT_PORT);
    }
}
