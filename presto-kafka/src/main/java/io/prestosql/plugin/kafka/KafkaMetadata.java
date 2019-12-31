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
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.prestosql.decoder.avro.AvroSchemaConverter;
import io.prestosql.decoder.dummy.DummyRowDecoder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.avro.Schema;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.plugin.kafka.KafkaHandleResolver.convertColumnHandle;
import static io.prestosql.plugin.kafka.KafkaHandleResolver.convertTableHandle;
import static java.util.Objects.requireNonNull;

/**
 * Manages the Kafka connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns. See {@link KafkaInternalFieldDescription} for a list
 * of per-topic additional columns.
 */
public class KafkaMetadata
        implements ConnectorMetadata
{
    private static final String AUTO_RESOLVE = "auto";
    private static final String AVRO = "avro";

    private final boolean hideInternalColumns;
    private final Map<SchemaTableName, KafkaTopicDescription> tableDescriptions;
    private final Optional<SchemaRegistryClient> schemaRegistryClient;
    private final AvroSchemaConverter avroSchemaConverter;

    @Inject
    public KafkaMetadata(
            KafkaConfig kafkaConfig,
            Supplier<Map<SchemaTableName, KafkaTopicDescription>> kafkaTableDescriptionSupplier,
            TypeManager typeManager)
    {
        requireNonNull(kafkaConfig, "kafkaConfig is null");
        this.hideInternalColumns = kafkaConfig.isHideInternalColumns();

        requireNonNull(kafkaTableDescriptionSupplier, "kafkaTableDescriptionSupplier is null");
        this.tableDescriptions = kafkaTableDescriptionSupplier.get();

        requireNonNull(typeManager, "typeManager is null");
        this.schemaRegistryClient = kafkaConfig.getSchemaRegistryUrl().map(schemaRegistryUrl ->
                new CachedSchemaRegistryClient(schemaRegistryUrl, 1000));
        this.avroSchemaConverter = new AvroSchemaConverter(typeManager);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (SchemaTableName tableName : tableDescriptions.keySet()) {
            builder.add(tableName.getSchemaName());
        }
        return ImmutableList.copyOf(builder.build());
    }

    @Override
    public KafkaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        KafkaTopicDescription table = tableDescriptions.get(schemaTableName);
        if (table == null) {
            return null;
        }

        return new KafkaTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                table.getTopicName(),
                getDataFormat(table.getKey()),
                getDataFormat(table.getMessage()),
                table.getKey().flatMap(KafkaTopicFieldGroup::getDataSchema),
                table.getMessage().flatMap(KafkaTopicFieldGroup::getDataSchema));
    }

    private static String getDataFormat(Optional<KafkaTopicFieldGroup> fieldGroup)
    {
        return fieldGroup.map(KafkaTopicFieldGroup::getDataFormat).orElse(DummyRowDecoder.NAME);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(convertTableHandle(tableHandle).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : tableDescriptions.keySet()) {
            if (schemaName.map(tableName.getSchemaName()::equals).orElse(true)) {
                builder.add(tableName);
            }
        }

        return builder.build();
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        KafkaTableHandle kafkaTableHandle = convertTableHandle(tableHandle);

        KafkaTopicDescription kafkaTopicDescription = tableDescriptions.get(kafkaTableHandle.toSchemaTableName());
        if (kafkaTopicDescription == null) {
            throw new TableNotFoundException(kafkaTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        AtomicInteger index = new AtomicInteger(0);

        kafkaTopicDescription.getKey().ifPresent(key -> {
            List<KafkaTopicFieldDescription> fields = key.getFields();
            if (fields.isEmpty()) {
                Optional<KafkaTopicFieldGroup> fieldDescription = getFieldGroup(kafkaTableHandle.toSchemaTableName(), kafkaTopicDescription, true);
                if (fieldDescription.isPresent()) {
                    fields = fieldDescription.get().getFields();
                }
            }
            for (KafkaTopicFieldDescription kafkaTopicFieldDescription : fields) {
                columnHandles.put(kafkaTopicFieldDescription.getName(), kafkaTopicFieldDescription.getColumnHandle(true, index.getAndIncrement()));
            }
        });

        kafkaTopicDescription.getMessage().ifPresent(message -> {
            List<KafkaTopicFieldDescription> fields = message.getFields();
            if (fields.isEmpty()) {
                Optional<KafkaTopicFieldGroup> fieldDescription = getFieldGroup(kafkaTableHandle.toSchemaTableName(), kafkaTopicDescription, false);
                if (fieldDescription.isPresent()) {
                    fields = fieldDescription.get().getFields();
                }
            }
            for (KafkaTopicFieldDescription kafkaTopicFieldDescription : fields) {
                columnHandles.put(kafkaTopicFieldDescription.getName(), kafkaTopicFieldDescription.getColumnHandle(false, index.getAndIncrement()));
            }
        });

        for (KafkaInternalFieldDescription kafkaInternalFieldDescription : KafkaInternalFieldDescription.values()) {
            columnHandles.put(kafkaInternalFieldDescription.getColumnName(), kafkaInternalFieldDescription.getColumnHandle(index.getAndIncrement(), hideInternalColumns));
        }

        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames;
        if (!prefix.getTable().isPresent()) {
            tableNames = listTables(session, prefix.getSchema());
        }
        else {
            tableNames = ImmutableList.of(prefix.toSchemaTableName());
        }

        for (SchemaTableName tableName : tableNames) {
            try {
                columns.put(tableName, getTableMetadata(tableName).getColumns());
            }
            catch (TableNotFoundException e) {
                // information_schema table or a system table
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        convertTableHandle(tableHandle);
        return convertColumnHandle(columnHandle).getColumnMetadata();
    }

    @SuppressWarnings("ValueOfIncrementOrDecrementUsed")
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        KafkaTopicDescription table = tableDescriptions.get(schemaTableName);
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        table.getKey().ifPresent(key -> {
            List<KafkaTopicFieldDescription> fields = key.getFields();
            if (fields.isEmpty()) {
                Optional<KafkaTopicFieldGroup> fieldDescription = getFieldGroup(schemaTableName, table, true);
                if (fieldDescription.isPresent()) {
                    fields = fieldDescription.get().getFields();
                }
            }
            for (KafkaTopicFieldDescription fieldDescription : fields) {
                builder.add(fieldDescription.getColumnMetadata());
            }
        });

        table.getMessage().ifPresent(message -> {
            List<KafkaTopicFieldDescription> fields = message.getFields();
            if (fields.isEmpty()) {
                Optional<KafkaTopicFieldGroup> fieldDescription = getFieldGroup(schemaTableName, table, false);
                if (fieldDescription.isPresent()) {
                    fields = fieldDescription.get().getFields();
                }
            }
            for (KafkaTopicFieldDescription fieldDescription : fields) {
                builder.add(fieldDescription.getColumnMetadata());
            }
        });

        for (KafkaInternalFieldDescription fieldDescription : KafkaInternalFieldDescription.values()) {
            builder.add(fieldDescription.getColumnMetadata(hideInternalColumns));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    static boolean isAutoResolvable(KafkaTableHandle tableHandle, boolean isKey)
    {
        if (isKey) {
            return tableHandle.getKeyDataSchemaLocation().map(KafkaMetadata::shouldAutoResolveSchema).orElse(false)
                    && tableHandle.getKeyDataFormat().equalsIgnoreCase(AVRO);
        }
        else {
            return tableHandle.getMessageDataSchemaLocation().map(KafkaMetadata::shouldAutoResolveSchema).orElse(false)
                    && tableHandle.getMessageDataFormat().equalsIgnoreCase(AVRO);
        }
    }
    private static boolean isAutoResolvable(KafkaTopicDescription topicDescription, boolean isKey)
    {
        requireNonNull(topicDescription, "topicDescription is null");
        if (isKey) {
            return topicDescription.getKey().isPresent()
                    && getDataFormat(topicDescription.getKey()).equals(AVRO)
                    && topicDescription.getKey().get().getDataSchema().map(KafkaMetadata::shouldAutoResolveSchema).orElse(false);
        }
        else {
            return topicDescription.getMessage().isPresent()
                    && getDataFormat(topicDescription.getMessage()).equals(AVRO)
                    && topicDescription.getMessage().get().getDataSchema().map(KafkaMetadata::shouldAutoResolveSchema).orElse(false);
        }
    }

    static boolean shouldAutoResolveSchema(String dataSchema)
    {
        return requireNonNull(dataSchema, "dataSchema is null").equalsIgnoreCase(AUTO_RESOLVE);
    }

    private Optional<KafkaTopicFieldGroup> getFieldGroup(SchemaTableName schemaTableName, KafkaTopicDescription topicDescription, boolean isKey)
    {
        if (!isAutoResolvable(topicDescription, isKey)) {
            return Optional.empty();
        }
        try {
            SchemaMetadata schemaMetadata = schemaRegistryClient.get().getLatestSchemaMetadata(topicDescription.getTopicName() + getSuffix(isKey));
            Schema schema = (new Schema.Parser()).parse(schemaMetadata.getSchema());
            List<Type> types = avroSchemaConverter.convertAvroSchema(schema);
            List<Schema.Field> avroFields = schema.getFields();
            checkState(avroFields.size() == types.size(), "incompatible schema");
            ImmutableList.Builder<KafkaTopicFieldDescription> fieldsBuilder = ImmutableList.builder();
            for (int i = 0; i < types.size(); i++) {
                Schema.Field field = avroFields.get(i);
                fieldsBuilder.add(new KafkaTopicFieldDescription(
                        field.name(),
                        types.get(i),
                        field.name(),
                        null,
                        null,
                        null,
                        false));
            }
            return Optional.of(new KafkaTopicFieldGroup(AVRO, getDataSchema(topicDescription, isKey), fieldsBuilder.build()));

        }
        catch (IOException | RestClientException e) {
            throw new TableNotFoundException(schemaTableName);
        }
    }

    static String getSuffix(boolean isKey) {
        if (isKey) {
            return "-key";
        }
        else {
            return "-value";
        }
    }

    private static Optional<String> getDataSchema(KafkaTopicDescription topicDescription, boolean isKey)
    {
        requireNonNull(topicDescription, "topicDescription is null");
        if (isKey) {
            return topicDescription.getKey().flatMap(KafkaTopicFieldGroup::getDataSchema);
        }
        else {
            return topicDescription.getMessage().flatMap(KafkaTopicFieldGroup::getDataSchema);
        }
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }
}
