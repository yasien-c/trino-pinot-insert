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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.prestosql.plugin.kafka.KafkaTopicDescription;
import io.prestosql.plugin.kafka.KafkaTopicFieldDescription;
import io.prestosql.plugin.kafka.KafkaTopicFieldGroup;
import io.prestosql.plugin.kafka.decoder.AvroConfluentRowDecoder;
import io.prestosql.plugin.kafka.lookup.SchemaRegistryTopicDescriptionLookup.DecodedTopicInfo;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeManager;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class TopicInfoDecoder
{
    private static final String COLUMN_LIST_DELIMITER = ";";
    private static final String PROPERTY_DELIMITER = "=";
    private static final String TYPE_INFO_DELIMITER = "#";
    private static final Set<String> FIELD_PROPERTIES;

    private final TypeManager typeManager;
    private final SchemaRegistryClient schemaRegistryClient;
    private final AvroSchemaConverter avroSchemaConverter;

    static {
        // Property names have to be lowercase since SchemaTableName is lowercase
        FIELD_PROPERTIES = ImmutableSet.of("mapping", "comment", "dataformat", "formathint", "hidden");
    }

    public TopicInfoDecoder(TypeManager typeManager, SchemaRegistryClient schemaRegistryClient)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        this.avroSchemaConverter = new AvroSchemaConverter(typeManager);
    }

    public KafkaTopicDescription decodeTopicInfo(DecodedTopicInfo decodedTopicInfo)
    {
        requireNonNull(decodedTopicInfo, "encodedTableName is null");
        Optional<KafkaTopicFieldGroup> key = getFieldGroup(decodedTopicInfo, true);
        Optional<KafkaTopicFieldGroup> message = getFieldGroup(decodedTopicInfo, false);

        return new KafkaTopicDescription(decodedTopicInfo.getSchemaTableName().getTableName(),
                Optional.of(decodedTopicInfo.getSchemaTableName().getSchemaName()),
                decodedTopicInfo.getTopicName(),
                key,
                message);
    }

    private Optional<KafkaTopicFieldGroup> getFieldGroup(DecodedTopicInfo decodedTopicInfo, boolean isKey)
    {
        requireNonNull(decodedTopicInfo, "decodedTopicInfo is null");
        String columns = getColumns(decodedTopicInfo, isKey);
        String dataFormat = getDataFormat(decodedTopicInfo, isKey);
        if (!columns.isEmpty()) {
            List<KafkaTopicFieldDescription> fields = parseFieldDescriptions(columns);
            return Optional.of(new KafkaTopicFieldGroup(dataFormat, getDataSchema(decodedTopicInfo, isKey), fields));
        }
        return resolveFieldDescriptions(dataFormat, getSubject(decodedTopicInfo, isKey));
    }

    private Optional<String> getSubject(DecodedTopicInfo decodedTopicInfo, boolean isKey)
    {
        requireNonNull(decodedTopicInfo, "decodedTopicInfo is null");
        if (isKey) {
            return decodedTopicInfo.getKeySubject();
        }
        return decodedTopicInfo.getMessageSubject();
    }

    private String getDataFormat(DecodedTopicInfo decodedTopicInfo, boolean isKey)
    {
        requireNonNull(decodedTopicInfo, "decodedTopicInfo is null");
        if (isKey) {
            return decodedTopicInfo.getKeyDataFormat();
        }
        return decodedTopicInfo.getMessageDataFormat();
    }

    private Optional<String> getDataSchema(DecodedTopicInfo decodedTopicInfo, boolean isKey)
    {
        requireNonNull(decodedTopicInfo, "decodedTopicInfo is null");
        if (isKey) {
            return decodedTopicInfo.getKeyDataSchema();
        }
        return decodedTopicInfo.getMessageDataSchema();
    }

    private static String getColumns(DecodedTopicInfo decodedTopicInfo, boolean isKey)
    {
        requireNonNull(decodedTopicInfo, "decodedTopicInfo is null");
        if (isKey) {
            return decodedTopicInfo.getKeyColumns();
        }
        return decodedTopicInfo.getMessageColumns();
    }

    private Optional<KafkaTopicFieldGroup> resolveFieldDescriptions(String dataFormat, Optional<String> subject)
    {
        if (!dataFormat.equalsIgnoreCase(AvroConfluentRowDecoder.NAME) || !subject.isPresent()) {
            return Optional.empty();
        }
        try {
            Schema schema = new Schema.Parser().parse(schemaRegistryClient.getLatestSchemaMetadata(subject.get()).getSchema());
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
            return Optional.of(new KafkaTopicFieldGroup(dataFormat, Optional.empty(), fieldsBuilder.build()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    private List<KafkaTopicFieldDescription> parseFieldDescriptions(String columns)
    {
        ImmutableList.Builder<KafkaTopicFieldDescription> fieldDescriptionsBuilder = ImmutableList.builder();
        Splitter.on(COLUMN_LIST_DELIMITER).trimResults().splitToList(columns).stream()
                .forEach(columnInfo -> fieldDescriptionsBuilder.add(parseFieldDescription(columnInfo)));
        return fieldDescriptionsBuilder.build();
    }

    private KafkaTopicFieldDescription parseFieldDescription(String field)
    {
        List<String> parts = Splitter.on(TYPE_INFO_DELIMITER).trimResults().omitEmptyStrings().splitToList(field);
        checkState(parts.size() >= 1, "Unexpected type info: '%s'. Expected format is <columnName>=<prestoType>;<property1>=<value1>;<property2>=<value2>...", field);
        ColumnNameAndType columnNameAndType = parseColumnNameAndType(parts.get(0));
        Map<String, String> columnProperties = parseColumnProperties(parts);
        String mapping = columnProperties.get("mapping");
        String comment = columnProperties.get("comment");
        String dataFormat = columnProperties.get("dataformat");
        String formatHint = columnProperties.get("formathint");
        boolean hidden = Optional.ofNullable(columnProperties.get("hidden")).map(Boolean::valueOf).orElse(false);

        return new KafkaTopicFieldDescription(columnNameAndType.getColumName(),
                columnNameAndType.getType(),
                mapping,
                comment,
                dataFormat,
                formatHint,
                hidden);
    }

    private Map<String, String> parseColumnProperties(List<String> parts)
    {
        checkState(parts != null && !parts.isEmpty(), "parts is null or empty");
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
        // First pair is columnName=prestoType
        for (int i = 1; i < parts.size(); i++) {
            List<String> propertyNameAndValue = Splitter.on(PROPERTY_DELIMITER).trimResults().omitEmptyStrings().splitToList(parts.get(i));
            checkState(propertyNameAndValue.size() == 2, "Unexpected property name and value: '%s'", parts.get(i));
            checkState(FIELD_PROPERTIES.contains(propertyNameAndValue.get(0)), "Unknown field property '%s'", propertyNameAndValue.get(0));
            propertiesBuilder.put(propertyNameAndValue.get(0), propertyNameAndValue.get(1));
        }
        return propertiesBuilder.build();
    }

    private ColumnNameAndType parseColumnNameAndType(String columnInfo)
    {
        List<String> nameAndType = Splitter.on(PROPERTY_DELIMITER).trimResults().omitEmptyStrings().splitToList(columnInfo);
        checkState(nameAndType.size() == 2, "Unexpected column info '%s'. Expected format is <columnName>=<prestoType>", columnInfo);
        String columnName = nameAndType.get(0);
        Type type = typeManager.getType(TypeId.of(nameAndType.get(1)));
        return new ColumnNameAndType(columnName, type);
    }

    public static class ColumnNameAndType
    {
        private final String columName;
        private final Type type;

        public ColumnNameAndType(String columName, Type type)
        {
            this.columName = requireNonNull(columName, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        public String getColumName()
        {
            return columName;
        }

        public Type getType()
        {
            return type;
        }
    }
}
