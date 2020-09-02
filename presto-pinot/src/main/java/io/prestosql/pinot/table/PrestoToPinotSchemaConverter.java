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
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.pinot.PinotTableProperties.DATE_TIME_FIELDS_PROPERTY;
import static io.prestosql.pinot.PinotTableProperties.DIMENSION_FIELDS_PROPERTY;
import static io.prestosql.pinot.PinotTableProperties.METRIC_FIELDS_PROPERTY;
import static io.prestosql.pinot.PinotTableProperties.SCHEMA_NAME_PROPERTY;
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

    private static boolean isSupportedType(Type type)
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
        String schemaName = tableMetadata.getProperties().get(SCHEMA_NAME_PROPERTY).toString();
        if (isNullOrEmpty(schemaName)) {
            schemaBuilder.setSchemaName(tableMetadata.getTable().getTableName());
        }
        else {
            schemaBuilder.setSchemaName(schemaName);
        }
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

        List<DateTimeFieldSpec> dateTimeFieldSpecs = (List<DateTimeFieldSpec>) tableMetadata.getProperties().get(DATE_TIME_FIELDS_PROPERTY);
        for (DateTimeFieldSpec dateTimeFieldSpec : dateTimeFieldSpecs) {
            ColumnMetadata columnMetadata = requireNonNull(columns.get(dateTimeFieldSpec.getName().toLowerCase(ENGLISH)), format("Column %s not found", dateTimeFieldSpec.getName().toLowerCase(ENGLISH)));
            Type type = columnMetadata.getType();
            checkState(isSupportedPrimitive(type), "Unsupported type %s for metric column", type);
            DataType pinotDataType = PRESTO_TO_PINOT_TYPE_MAP.get(type);
            DataType expectedDataType = dateTimeFieldSpec.getDataType();
            checkState(pinotDataType.equals(expectedDataType), "Type mismatch for date time field %s", dateTimeFieldSpec);
            schemaBuilder.addDateTime(dateTimeFieldSpec.getName(), dateTimeFieldSpec.getDataType(), dateTimeFieldSpec.getFormat(), dateTimeFieldSpec.getGranularity(), dateTimeFieldSpec.getDefaultNullValue(), dateTimeFieldSpec.getTransformFunction());
        }
        return schemaBuilder.build();
    }
}
