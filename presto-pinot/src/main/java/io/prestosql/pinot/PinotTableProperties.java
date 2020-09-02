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
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.ArrayType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class PinotTableProperties
{
    public static final String SCHEMA_NAME_PROPERTY = "schema_name";
    public static final String DIMENSION_FIELDS_PROPERTY = "dimensions";
    public static final String METRIC_FIELDS_PROPERTY = "metrics";
    public static final String DATE_TIME_FIELDS_PROPERTY = "date_time_fields";

    public static final String REAL_TIME_KAFKA_TOPIC = "realtime.kafka_topic";
    public static final String REAL_TIME_KAFKA_BROKERS = "realtime.kafka_brokers";
    public static final String REAL_TIME_SCHEMA_REGISTRY_URL = "realtime.schema_registry_url";
    public static final String REAL_TIME_RETENTION = "realtime.retention";

    private final List<PropertyMetadata<?>> tableProperties;

    public PinotTableProperties()
    {
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
                        DATE_TIME_FIELDS_PROPERTY,
                        "Metric Fields",
                        new ArrayType(VARCHAR),
                        List.class,
                        ImmutableList.of(),
                        false,
                        value -> ImmutableList.copyOf(((Collection<?>) value).stream()
                                .map(PinotTableProperties::fromString)
                                .collect(Collectors.toList())),
                        value -> value))
                .build();
    }

    public List<PropertyMetadata<?>> getTableProperties()
    {
        return tableProperties;
    }

    private static DateTimeFieldSpec fromString(Object jsonString)
    {
        try {
            return JsonUtils.stringToObject((String) jsonString, DateTimeFieldSpec.class);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
