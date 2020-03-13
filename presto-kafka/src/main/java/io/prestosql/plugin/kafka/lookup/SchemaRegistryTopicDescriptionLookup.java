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
package io.prestosql.plugin.kafka.lookup;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.prestosql.plugin.kafka.KafkaConfig;
import io.prestosql.plugin.kafka.KafkaTopicDescription;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.Duration.succinctNanos;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_UNAVAILABLE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class SchemaRegistryTopicDescriptionLookup
        implements TopicDescriptionLookup
{
    public static final String NAME = "schema-registry";

    private static final String DELIMITER = "&";
    private static final String KEY_SUFFIX = "-key";
    private static final String VALUE_SUFFIX = "-value";

    private final SchemaRegistryClient schemaRegistryClient;
    private final String defaultSchema;

    private final ScheduledExecutorService subjectsCacheExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("SchemaRegistryTopicDescriptionLookup"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicLong lastRefresh = new AtomicLong();
    private final Duration maxRefreshInterval;
    private final CounterStat refreshFailures = new CounterStat();
    private final AtomicReference<Map<String, TopicAndSubjects>> topicAndSubjectsCache = new AtomicReference<>(ImmutableMap.of());
    private final TopicInfoDecoder topicInfoDecoder;

    @Inject
    public SchemaRegistryTopicDescriptionLookup(KafkaConfig kafkaConfig, TypeManager typeManager)
    {
        requireNonNull(kafkaConfig, "kafkaConfig is null");
        requireNonNull(typeManager, "typeManager is null");
        checkState(kafkaConfig.getSchemaRegistryUrl().isPresent(), "schema registry url is not present in config");
        schemaRegistryClient = new CachedSchemaRegistryClient(kafkaConfig.getSchemaRegistryUrl().get(), kafkaConfig.getSchemaRegistryCapacity());
        defaultSchema = requireNonNull(kafkaConfig.getDefaultSchema(), "default schema is null");
        this.maxRefreshInterval = new Duration(1, TimeUnit.SECONDS);
        this.topicInfoDecoder = new TopicInfoDecoder(typeManager, schemaRegistryClient);
        refreshSubjects();
    }

    @PostConstruct
    public void start()
    {
        if (started.compareAndSet(false, true)) {
            subjectsCacheExecutor.scheduleWithFixedDelay(this::refreshSubjects, 1, 1, TimeUnit.SECONDS);
        }
    }

    @PreDestroy
    public void destroy()
    {
        subjectsCacheExecutor.shutdownNow();
    }

    private void refreshSubjects()
    {
        try {
            Map<String, Set<String>> topicToSubjects = new HashMap<>();
            for (String subject : schemaRegistryClient.getAllSubjects()) {
                topicToSubjects.computeIfAbsent(extractTopicFromSubject(subject), k -> new HashSet<>()).add(subject);
            }
            ImmutableMap.Builder<String, TopicAndSubjects> topicSubjectsCacheBuilder = ImmutableMap.builder();
            for (Map.Entry<String, Set<String>> entry : topicToSubjects.entrySet()) {
                String topic = entry.getKey();
                TopicAndSubjects topicAndSubjects = new TopicAndSubjects(topic,
                        entry.getValue().contains(getKeySubjectFromTopic(topic)),
                        entry.getValue().contains(getValueSubjectFromTopic(topic)));
                topicSubjectsCacheBuilder.put(topicAndSubjects.getTableName(), topicAndSubjects);
            }
            topicAndSubjectsCache.set(topicSubjectsCacheBuilder.build());
            lastRefresh.set(System.nanoTime());
        }
        catch (Throwable t) {
            if (succinctNanos(System.nanoTime() - lastRefresh.get()).compareTo(maxRefreshInterval) > 0) {
                lastRefresh.set(0);
            }
            refreshFailures.update(1);
        }
    }

    @Override
    public KafkaTopicDescription getTopicDescription(SchemaTableName schemaTableName)
    {
        if (lastRefresh.get() == 0) {
            throw new PrestoException(CONFIGURATION_UNAVAILABLE, "Topic descriptions cannot be fetched from schema registry");
        }
        DecodedTopicInfo decodedTopicInfo = parseDecodedTopicInfo(schemaTableName);
        return topicInfoDecoder.decodeTopicInfo(decodedTopicInfo);
    }

    @Override
    public Collection<SchemaTableName> getAllTables()
    {
        try {
            ImmutableSet.Builder<SchemaTableName> schemaTableNameBuilder = ImmutableSet.builder();
            schemaRegistryClient.getAllSubjects().stream()
                    .forEach(subject -> schemaTableNameBuilder.add(new SchemaTableName(defaultSchema, extractTopicFromSubject(subject))));
            return schemaTableNameBuilder.build();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    private static String extractTopicFromSubject(String subject)
    {
        requireNonNull(subject, "subject is null");
        if (subject.endsWith(VALUE_SUFFIX)) {
            return subject.substring(0, subject.length() - VALUE_SUFFIX.length());
        }
        checkState(subject.endsWith(KEY_SUFFIX), "unexpected subject name %s", subject);
        return subject.substring(0, subject.length() - KEY_SUFFIX.length());
    }

    private static String getKeySubjectFromTopic(String topic)
    {
        return topic + KEY_SUFFIX;
    }

    private static String getValueSubjectFromTopic(String topic)
    {
        return topic + VALUE_SUFFIX;
    }

    private static class TopicAndSubjects
    {
        private final boolean hasKey;
        private final boolean hasValue;
        private final String topic;

        public TopicAndSubjects(String topic, boolean hasKey, boolean hasValue)
        {
            this.topic = requireNonNull(topic, "topic is null");
            this.hasKey = hasKey;
            this.hasValue = hasValue;
        }

        public String getTableName()
        {
            return topic.toLowerCase(ENGLISH);
        }

        public String getTopic()
        {
            return topic;
        }

        public Optional<String> getKeySubject()
        {
            if (hasKey) {
                return Optional.of(getKeySubjectFromTopic(topic));
            }
            return Optional.empty();
        }

        public Optional<String> getValueSubject()
        {
            if (hasValue) {
                return Optional.of(getValueSubjectFromTopic(topic));
            }
            return Optional.empty();
        }
    }

    private DecodedTopicInfo parseDecodedTopicInfo(SchemaTableName encodedSchemaTableName)
    {
        requireNonNull(encodedSchemaTableName, "encodedTableName is null");
        String encodedTableName = encodedSchemaTableName.getTableName();
        List<String> parts = Splitter.on(DELIMITER).trimResults().splitToList(encodedTableName);
        checkState(parts.size() == 7 || parts.size() == 1, "Unexpected format for encodedTableName. Expected format is <tableName>&<keyDataFormat>&<keyDataSchema>&<columnName>=<typeInfo>;...&<valueDataFormat>&<valueDataSchema>&<columnName>=<typeInfo>;...");
        TopicAndSubjects topicAndSubjects = topicAndSubjectsCache.get().get(parts.get(0));
        checkState(topicAndSubjects != null, "Cannot find topic for encoded table name '%s'", encodedSchemaTableName);
        if (parts.size() == 1) {
            return new DecodedTopicInfo(new SchemaTableName(encodedSchemaTableName.getSchemaName(), topicAndSubjects.getTableName()),
                    topicAndSubjects.getTopic(),
                    topicAndSubjects.getKeySubject(),
                    "raw",
                    Optional.empty(),
                    "",
                    topicAndSubjects.getValueSubject(),
                    "avro-confluent",
                    Optional.empty(),
                    "");
        }
        return new DecodedTopicInfo(new SchemaTableName(encodedSchemaTableName.getSchemaName(), topicAndSubjects.getTableName()),
                topicAndSubjects.getTopic(),
                topicAndSubjects.getKeySubject(),
                "raw",
                parseDataSchema(parts.get(2)),
                parts.get(3),
                topicAndSubjects.getValueSubject(),
                parts.get(4),
                parseDataSchema(parts.get(5)),
                parts.get(6));
    }

    private static Optional<String> parseDataSchema(String dataSchema)
    {
        if (isNullOrEmpty(dataSchema)) {
            return Optional.empty();
        }
        return Optional.of(dataSchema);
    }

    static class DecodedTopicInfo
    {
        private final SchemaTableName schemaTableName;
        private final String topicName;
        private final Optional<String> keySubject;
        private final String keyDataFormat;
        private final Optional<String> keyDataSchema;
        private final String keyColumns;
        private final Optional<String> messageSubject;
        private final String messageDataFormat;
        private final Optional<String> messageDataSchema;
        private final String messageColumns;

        public DecodedTopicInfo(SchemaTableName schemaTableName,
                                String topicName,
                                Optional<String> keySubject,
                                String keyDataFormat,
                                Optional<String> keyDataSchema,
                                String keyColumns,
                                Optional<String> messageSubject,
                                String messageDataFormat,
                                Optional<String> messageDataSchema,
                                String messageColumns)
        {
            this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
            this.topicName = requireNonNull(topicName, "tableName is null");
            this.keySubject = requireNonNull(keySubject, "keySubject is null");
            this.keyDataFormat = requireNonNull(keyDataFormat, "keyDataFormat is null");
            this.keyDataSchema = requireNonNull(keyDataSchema, "keyDataSchema is null");
            this.keyColumns = requireNonNull(keyColumns, "keyColumns is null");
            this.messageSubject = requireNonNull(messageSubject, "messageSubject is null");
            this.messageDataFormat = requireNonNull(messageDataFormat, "messageDataFormat is null");
            this.messageDataSchema = requireNonNull(messageDataSchema, "messageDataSchema is null");
            this.messageColumns = requireNonNull(messageColumns, "messageColumns is null");
        }

        public SchemaTableName getSchemaTableName()
        {
            return schemaTableName;
        }

        public String getTopicName()
        {
            return topicName;
        }

        public Optional<String> getKeySubject()
        {
            return keySubject;
        }

        public String getKeyDataFormat()
        {
            return keyDataFormat;
        }

        public Optional<String> getKeyDataSchema()
        {
            return keyDataSchema;
        }

        public String getKeyColumns()
        {
            return keyColumns;
        }

        public Optional<String> getMessageSubject()
        {
            return messageSubject;
        }

        public String getMessageDataFormat()
        {
            return messageDataFormat;
        }

        public Optional<String> getMessageDataSchema()
        {
            return messageDataSchema;
        }

        public String getMessageColumns()
        {
            return messageColumns;
        }
    }

    @Managed
    @Nested
    public CounterStat getRefreshFailures()
    {
        return refreshFailures;
    }
}
