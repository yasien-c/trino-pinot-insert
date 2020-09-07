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
package io.prestosql.pinot;

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.prestosql.pinot.table.ConsumerType;
import io.prestosql.pinot.table.PinotTimeFieldSpec;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.ArrayType;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.utils.JsonUtils;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.prestosql.pinot.table.ConsumerType.AVRO;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class PinotTableProperties
{
    public static final String SCHEMA_NAME_PROPERTY = "schema_name";
    public static final String DIMENSION_FIELDS_PROPERTY = "dimensions";
    public static final String METRIC_FIELDS_PROPERTY = "metrics";
    public static final String TIME_FIELD_SPEC_PROPERTY = "time_field_spec";

    public static final String REAL_TIME_KAFKA_TOPIC = "kafka_topic";
    public static final String REAL_TIME_KAFKA_BROKERS = "kafka_brokers";
    public static final String REAL_TIME_SCHEMA_REGISTRY_URL = "schema_registry_url";
    public static final String REAL_TIME_RETENTION_DAYS = "realtime_retention";
    public static final String REAL_TIME_REPLICAS_PER_PARTITION = "realtime_replicas_per_partition";
    public static final String REAL_TIME_FLUSH_THRESHOLD_TIME = "realtime_flush_threshold_time";
    public static final String REAL_TIME_FLUSH_THRESHOLD_DESIRED_SIZE = "realtime_flush_threshold_size";
    public static final String REAL_TIME_CONSUMER_TYPE = "realtime_consumer_type";

    public static final String OFFLINE_REPLICATION_FACTOR = "offline_replication";
    public static final String OFFLINE_RETENTION_DAYS = "offline_retention";
    public static final String INDEX_INVERTED = "index_inverted";
    public static final String INDEX_NO_DICTIONARY = "index_no_dictionary";
    public static final String INDEX_SORTED = "index_sorted";
    public static final String INDEX_STAR_TREE = "index_star_tree";

    private final List<PropertyMetadata<?>> tableProperties;
    private static final JsonCodec<PinotTimeFieldSpec> TIME_FIELD_CODEC = jsonCodec(PinotTimeFieldSpec.class);

    @Inject
    public PinotTableProperties(PinotConfig config)
    {
        requireNonNull(config, "config is null");
        tableProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(PropertyMetadata.stringProperty(
                        SCHEMA_NAME_PROPERTY,
                        "Pinot Schema Name",
                        "",
                        false))
                .add(new PropertyMetadata<>(
                        DIMENSION_FIELDS_PROPERTY,
                        "Dimension Fields",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> (String) name)
                                .collect(Collectors.toList())),
                        value -> value))
                .add(new PropertyMetadata<>(
                        METRIC_FIELDS_PROPERTY,
                        "Metric Fields",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> (String) name)
                                .collect(Collectors.toList())),
                        value -> value))
                .add(new PropertyMetadata<>(
                        TIME_FIELD_SPEC_PROPERTY,
                        "Time Field Spec",
                        VARCHAR,
                        PinotTimeFieldSpec.class,
                        null,
                        false,
                        value -> TIME_FIELD_CODEC.fromJson((String) value),
                        value -> value))
                .add(PropertyMetadata.stringProperty(REAL_TIME_KAFKA_TOPIC, "Kafka topic", "", false))
                .add(new PropertyMetadata<>(
                        REAL_TIME_KAFKA_BROKERS,
                        "Kafka Brokers",
                        new ArrayType(VARCHAR),
                        List.class,
                        config.getDefaultKafkaBrokers(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> (String) name)
                                .collect(Collectors.toList())),
                        value -> value))
                .add(PropertyMetadata.stringProperty(REAL_TIME_SCHEMA_REGISTRY_URL, "Schema Registry Url", config.getDefaultSchemaRegistryUrl(), false))
                .add(PropertyMetadata.integerProperty(REAL_TIME_RETENTION_DAYS, "Real time retention days", 7, false))
                .add(PropertyMetadata.integerProperty(REAL_TIME_REPLICAS_PER_PARTITION, "Real time replicas per partition", 1, false))
                .add(PropertyMetadata.stringProperty(REAL_TIME_FLUSH_THRESHOLD_TIME, "Real time flush threshold time", "6h", false))
                .add(PropertyMetadata.stringProperty(REAL_TIME_FLUSH_THRESHOLD_DESIRED_SIZE, "Real time flush desired size", "200M", false))
                .add(PropertyMetadata.enumProperty(REAL_TIME_CONSUMER_TYPE, "Real time consumer type", ConsumerType.class, AVRO, false))
                .add(PropertyMetadata.integerProperty(OFFLINE_REPLICATION_FACTOR, "Offline replication factor", 1, false))
                .add(PropertyMetadata.integerProperty(OFFLINE_RETENTION_DAYS, "Offline time retention days", 365, false))
                .add(new PropertyMetadata<>(
                        INDEX_INVERTED,
                        "Inverted index columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> (String) name)
                                .collect(Collectors.toList())),
                        value -> value))
                .add(new PropertyMetadata<>(
                        INDEX_NO_DICTIONARY,
                        "Inverted index columns",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(name -> (String) name)
                                .collect(Collectors.toList())),
                        value -> value))
                .add(PropertyMetadata.stringProperty(INDEX_SORTED, "Sorted column", null, false))
                .add(new PropertyMetadata<>(
                        INDEX_STAR_TREE,
                        "Star tree index configs",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(PinotTableProperties::toStarTreeIndexConfig)
                                .collect(Collectors.toList())),
                        value -> value))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    private static StarTreeIndexConfig toStarTreeIndexConfig(Object value)
    {
        try {
            return JsonUtils.stringToObject((String) value, StarTreeIndexConfig.class);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }
}
