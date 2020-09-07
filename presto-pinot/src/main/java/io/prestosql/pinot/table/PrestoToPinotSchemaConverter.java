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
package io.prestosql.pinot.table;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableCustomConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.pinot.PinotTableProperties.DIMENSION_FIELDS_PROPERTY;
import static io.prestosql.pinot.PinotTableProperties.INDEX_INVERTED;
import static io.prestosql.pinot.PinotTableProperties.INDEX_NO_DICTIONARY;
import static io.prestosql.pinot.PinotTableProperties.INDEX_SORTED;
import static io.prestosql.pinot.PinotTableProperties.INDEX_STAR_TREE;
import static io.prestosql.pinot.PinotTableProperties.METRIC_FIELDS_PROPERTY;
import static io.prestosql.pinot.PinotTableProperties.OFFLINE_REPLICATION_FACTOR;
import static io.prestosql.pinot.PinotTableProperties.OFFLINE_RETENTION_DAYS;
import static io.prestosql.pinot.PinotTableProperties.REAL_TIME_CONSUMER_TYPE;
import static io.prestosql.pinot.PinotTableProperties.REAL_TIME_FLUSH_THRESHOLD_DESIRED_SIZE;
import static io.prestosql.pinot.PinotTableProperties.REAL_TIME_FLUSH_THRESHOLD_TIME;
import static io.prestosql.pinot.PinotTableProperties.REAL_TIME_KAFKA_BROKERS;
import static io.prestosql.pinot.PinotTableProperties.REAL_TIME_KAFKA_TOPIC;
import static io.prestosql.pinot.PinotTableProperties.REAL_TIME_REPLICAS_PER_PARTITION;
import static io.prestosql.pinot.PinotTableProperties.REAL_TIME_RETENTION_DAYS;
import static io.prestosql.pinot.PinotTableProperties.REAL_TIME_SCHEMA_REGISTRY_URL;
import static io.prestosql.pinot.PinotTableProperties.SCHEMA_NAME_PROPERTY;
import static io.prestosql.pinot.PinotTableProperties.TIME_FIELD_SPEC_PROPERTY;
import static io.prestosql.pinot.table.ConsumerType.AVRO;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PrestoToPinotSchemaConverter
{
    private static final Set<Type> SUPPORTED_PRIMITIVE_TYPES = ImmutableSet.of(
            BooleanType.BOOLEAN,
            TinyintType.TINYINT,
            SmallintType.SMALLINT,
            IntegerType.INTEGER,
            BigintType.BIGINT,
            RealType.REAL,
            DoubleType.DOUBLE,
            VarbinaryType.VARBINARY);

    private static final Map<Type, DataType> PRESTO_TO_PINOT_TYPE_MAP =
            ImmutableMap.<Type, DataType>builder()
                    .put(TinyintType.TINYINT, DataType.INT)
                    .put(SmallintType.SMALLINT, DataType.INT)
                    .put(IntegerType.INTEGER, DataType.INT)
                    .put(BigintType.BIGINT, DataType.LONG)
                    .put(RealType.REAL, DataType.FLOAT)
                    .put(DoubleType.DOUBLE, DataType.DOUBLE)
                    .build();

    private PrestoToPinotSchemaConverter() {}

    private static boolean isSupportedPrimitive(Type type)
    {
        return isVarcharType(type) || SUPPORTED_PRIMITIVE_TYPES.contains(type);
    }

    public static boolean isSupportedType(Type type)
    {
        if (isSupportedPrimitive(type)) {
            return true;
        }

        if (type instanceof ArrayType) {
            checkArgument(type.getTypeParameters().size() == 1, "expecting exactly one type parameter for array");
            return isSupportedType(type.getTypeParameters().get(0));
        }

        return false;
    }

    public static Schema convert(ConnectorTableMetadata tableMetadata)
    {
        requireNonNull(tableMetadata, "tableMetadata is null");
        Map<String, ColumnMetadata> columns = tableMetadata.getColumns().stream()
                .collect(toImmutableMap(column -> column.getName(), column -> column));
        Schema.SchemaBuilder schemaBuilder = new Schema.SchemaBuilder();
        Object schemaProperty = tableMetadata.getProperties().get(SCHEMA_NAME_PROPERTY);
        String schemaName = tableMetadata.getTable().getTableName();
        if (schemaProperty != null) {
            schemaName = schemaProperty.toString();
        }
        schemaBuilder.setSchemaName(schemaName);

        List<String> dimensionFields = (List<String>) tableMetadata.getProperties().get(DIMENSION_FIELDS_PROPERTY);
        checkState(!dimensionFields.isEmpty(), "Dimension fields is empty");
        for (String field : dimensionFields) {
            ColumnMetadata columnMetadata = requireNonNull(columns.get(field.toLowerCase(ENGLISH)), format("Column %s not found", field.toLowerCase(ENGLISH)));
            Type type = columnMetadata.getType();
            checkState(isSupportedType(type), "Unsupported type %s for dimension column", type);
            if (isSupportedPrimitive(type)) {
                schemaBuilder.addSingleValueDimension(field, PRESTO_TO_PINOT_TYPE_MAP.get(type));
            }
            else {
                schemaBuilder.addMultiValueDimension(columnMetadata.getName(), PRESTO_TO_PINOT_TYPE_MAP.get(getOnlyElement(type.getTypeParameters())));
            }
        }

        List<String> metricFields = (List<String>) tableMetadata.getProperties().get(METRIC_FIELDS_PROPERTY);
        for (String field : metricFields) {
            ColumnMetadata columnMetadata = requireNonNull(columns.get(field.toLowerCase(ENGLISH)), format("Column %s not found", field.toLowerCase(ENGLISH)));
            Type type = columnMetadata.getType();
            checkState(isSupportedPrimitive(type), "Unsupported type %s for metric column", type);
            schemaBuilder.addSingleValueDimension(field, PRESTO_TO_PINOT_TYPE_MAP.get(type));
        }

        TimeFieldSpec timeFieldSpec = ((PinotTimeFieldSpec) tableMetadata.getProperties().get(TIME_FIELD_SPEC_PROPERTY)).toTimeFieldSpec();
        ColumnMetadata columnMetadata = requireNonNull(columns.get(timeFieldSpec.getName().toLowerCase(ENGLISH)), "Time column is null");
        Type type = columnMetadata.getType();
        checkState(isSupportedPrimitive(type), "Unsupported type %s for time column", type);
        DataType pinotDataType = PRESTO_TO_PINOT_TYPE_MAP.get(type);
        DataType expectedDataType = timeFieldSpec.getDataType();
        checkState(pinotDataType.equals(expectedDataType), "Type mismatch for date time field %s", timeFieldSpec);
        schemaBuilder.addTime(timeFieldSpec.getIncomingGranularitySpec(), timeFieldSpec.getOutgoingGranularitySpec());
        return schemaBuilder.build();
    }

    public static Optional<TableConfig> getRealtimeConfig(Schema schema, ConnectorTableMetadata tableMetadata)
    {
        String kafkaTopic = (String) tableMetadata.getProperties().get(REAL_TIME_KAFKA_TOPIC);
        if (isNullOrEmpty(kafkaTopic)) {
            return Optional.empty();
        }

        List<String> kafkaBrokers = (List<String>) tableMetadata.getProperties().get(REAL_TIME_KAFKA_BROKERS);
        checkState(kafkaBrokers != null && !kafkaBrokers.isEmpty(), "No kafka brokers");

        ConsumerType consumerType = (ConsumerType) tableMetadata.getProperties().get(REAL_TIME_CONSUMER_TYPE);
        String schemaRegistryUrl = (String) tableMetadata.getProperties().get(REAL_TIME_SCHEMA_REGISTRY_URL);
        if (consumerType == AVRO) {
            checkState(!isNullOrEmpty(schemaRegistryUrl), "Schema registry url not found");
        }
        int realTimeRetentionDays = (int) tableMetadata.getProperties().get(REAL_TIME_RETENTION_DAYS);
        int realTimeReplicasPerPartition = (int) tableMetadata.getProperties().get(REAL_TIME_REPLICAS_PER_PARTITION);
        String realTimeFlushThresholdTime = (String) tableMetadata.getProperties().get(REAL_TIME_FLUSH_THRESHOLD_TIME);
        String realTimeFlushThresholdDesiredSize = (String) tableMetadata.getProperties().get(REAL_TIME_FLUSH_THRESHOLD_DESIRED_SIZE);

        SegmentsValidationAndRetentionConfig config = new SegmentsValidationAndRetentionConfig();
        config.setTimeColumnName(schema.getTimeFieldSpec().getName());
        config.setTimeType(schema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeType().name());
        config.setRetentionTimeUnit(TimeUnit.DAYS.name());
        config.setRetentionTimeValue(String.valueOf(realTimeRetentionDays));
        config.setSegmentPushType("APPEND");
        config.setSegmentPushFrequency("daily");
        config.setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy");
        config.setSchemaName(schema.getSchemaName());
        config.setReplicasPerPartition(String.valueOf(realTimeReplicasPerPartition));

        IndexingConfig indexingConfig = getIndexingConfig(tableMetadata);
        ImmutableMap.Builder<String, String> streamConfigsBuilder = ImmutableMap.builder();
        streamConfigsBuilder.put("streamType", "kafka")
                .put("stream.kafka.consumer.type", "LowLevel")
                .put("stream.kafka.topic.name", kafkaTopic)
                .put("stream.kafka.broker.list", String.join(",", kafkaBrokers))
                .put("realtime.segment.flush.threshold.time", realTimeFlushThresholdTime)
                .put("realtime.segment.flush.threshold.size", "0")
                .put("realtime.segment.flush.desired.size", realTimeFlushThresholdDesiredSize)
                .put("stream.kafka.consumer.prop.auto.isolation.level", "read_committed")
                .put("stream.kafka.consumer.prop.auto.offset.reset", "smallest")
                .put("stream.kafka.consumer.prop.group.id", format("%s_%s", schema.getSchemaName(), UUID.randomUUID()))
                .put("stream.kafka.consumer.prop.client.id", UUID.randomUUID().toString())
                .put("stream.kafka.consumer.factory.class.name", "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory");

        if (consumerType == AVRO) {
            streamConfigsBuilder.put("stream.kafka.decoder.class.name", "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder");
            streamConfigsBuilder.put("stream.kafka.decoder.prop.schema.registry.rest.url", schemaRegistryUrl);
        }
        else {
            streamConfigsBuilder.put("stream.kafka.decoder.class.name", "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder");
        }

        indexingConfig.setStreamConfigs(streamConfigsBuilder.build());

        TableConfig tableConfig = new TableConfig(
                schema.getSchemaName(),
                "REALTIME",
                config,
                new TenantConfig("DefaultTenant", "DefaultTenant", null),
                indexingConfig,
                new TableCustomConfig(null),
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
        return Optional.of(tableConfig);
    }

    public static TableConfig getOfflineConfig(Schema schema, ConnectorTableMetadata tableMetadata)
    {
        int offlineReplicationFactor = (int) tableMetadata.getProperties().get(OFFLINE_REPLICATION_FACTOR);
        int offlineRetentionDays = (int) tableMetadata.getProperties().get(OFFLINE_RETENTION_DAYS);
        SegmentsValidationAndRetentionConfig config = new SegmentsValidationAndRetentionConfig();
        config.setTimeColumnName(schema.getTimeFieldSpec().getName());
        config.setTimeType(schema.getTimeFieldSpec().getOutgoingGranularitySpec().getTimeType().name());
        config.setRetentionTimeUnit(TimeUnit.DAYS.name());
        config.setRetentionTimeValue(String.valueOf(offlineRetentionDays));
        config.setSegmentPushType("APPEND");
        config.setSegmentPushFrequency("daily");
        config.setSegmentAssignmentStrategy("BalanceNumSegmentAssignmentStrategy");
        config.setSchemaName(schema.getSchemaName());
        config.setReplication(String.valueOf(offlineReplicationFactor));
        IndexingConfig indexingConfig = getIndexingConfig(tableMetadata);

        return new TableConfig(
            schema.getSchemaName(),
            "OFFLINE",
            config,
            new TenantConfig("DefaultTenant", "DefaultTenant", null),
            indexingConfig,
            new TableCustomConfig(null),
            null,
            null,
            null,
            null,
            null,
            null,
            null);
    }

    public static IndexingConfig getIndexingConfig(ConnectorTableMetadata tableMetadata)
    {
        IndexingConfig indexingConfig = new IndexingConfig();
        List<String> invertedIndexColumns = (List<String>) tableMetadata.getProperties().get(INDEX_INVERTED);
        if (invertedIndexColumns != null && !invertedIndexColumns.isEmpty()) {
            indexingConfig.setInvertedIndexColumns(invertedIndexColumns);
        }
        List<String> noDictionaryColumns = (List<String>) tableMetadata.getProperties().get(INDEX_NO_DICTIONARY);
        if (noDictionaryColumns != null && !noDictionaryColumns.isEmpty()) {
            indexingConfig.setNoDictionaryColumns(noDictionaryColumns);
        }
        List<StarTreeIndexConfig> starTreeIndexConfigs = (List<StarTreeIndexConfig>)  tableMetadata.getProperties().get(INDEX_STAR_TREE);
        if (starTreeIndexConfigs != null && !starTreeIndexConfigs.isEmpty()) {
            indexingConfig.setStarTreeIndexConfigs(starTreeIndexConfigs);
        }
        indexingConfig.setCreateInvertedIndexDuringSegmentGeneration(true);
        indexingConfig.setLoadMode("MMAP");
        String sortedColumn = (String) tableMetadata.getProperties().get(INDEX_SORTED);
        if (!isNullOrEmpty(sortedColumn)) {
            indexingConfig.setSortedColumn(ImmutableList.of(sortedColumn));
        }
        return indexingConfig;
    }
}
