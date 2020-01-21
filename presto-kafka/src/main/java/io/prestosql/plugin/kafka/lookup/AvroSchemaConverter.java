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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.block.MethodHandleUtil.compose;
import static io.prestosql.spi.block.MethodHandleUtil.nativeValueGetter;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.BYTES;
import static org.apache.avro.Schema.Type.DOUBLE;
import static org.apache.avro.Schema.Type.ENUM;
import static org.apache.avro.Schema.Type.FIXED;
import static org.apache.avro.Schema.Type.FLOAT;
import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.Type.UNION;

public class AvroSchemaConverter
{
    private static final Set<Schema.Type> INTEGRAL_TYPES = ImmutableSet.of(INT, LONG);
    private static final Set<Schema.Type> DECIMAL_TYPES = ImmutableSet.of(FLOAT, DOUBLE);
    private static final Set<Schema.Type> STRING_TYPES = ImmutableSet.of(STRING, ENUM);
    private static final Set<Schema.Type> BINARY_TYPES = ImmutableSet.of(BYTES, FIXED);

    private final TypeManager typeManager;

    public AvroSchemaConverter(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public List<Type> convertAvroSchema(Schema schema)
    {
        requireNonNull(schema, "schema is null");
        checkState(schema.getType().equals(RECORD), "schema is not an avro record");
        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        for (Field field : schema.getFields()) {
            builder.add(convert(field.schema()));
        }
        return builder.build();
    }

    private Type convert(Schema schema)
    {
        switch (schema.getType()) {
            case INT:
                return IntegerType.INTEGER;
            case LONG:
                return BigintType.BIGINT;
            case BOOLEAN:
                return BooleanType.BOOLEAN;
            case FLOAT:
                return RealType.REAL;
            case DOUBLE:
                return DoubleType.DOUBLE;
            case ENUM:
            case STRING:
                return VarcharType.VARCHAR;
            case BYTES:
            case FIXED:
                return VarbinaryType.VARBINARY;
            case UNION:
                return convertUnion(schema);
            case ARRAY:
                return convertArray(schema);
            case MAP:
                return convertMap(schema);
            case RECORD:
                return convertRecord(schema);
            default:
                throw new UnsupportedOperationException(format("Type %s not supported", schema.getType()));
        }
    }

    private Type convertUnion(Schema schema)
    {
        checkArgument(schema.getType().equals(UNION), "schema is not a union schema");
        // Cannot use ImmutableSet.Builder because types may contain multiple FIXED types with different sizes
        Set<Schema.Type> types = schema.getTypes().stream()
                .map(Schema::getType)
                .collect(toImmutableSet());

        if (types.contains(NULL)) {
            return convertUnion(Schema.createUnion(schema.getTypes().stream()
                    .filter(type -> type.getType() != NULL)
                    .collect(toImmutableList())));
        }
        else if (schema.getTypes().size() == 1) {
            return convert(getOnlyElement(schema.getTypes()));
        }
        else if (INTEGRAL_TYPES.containsAll(types)) {
            return BigintType.BIGINT;
        }
        else if (DECIMAL_TYPES.containsAll(types)) {
            return DoubleType.DOUBLE;
        }
        else if (STRING_TYPES.containsAll(types)) {
            return VarcharType.VARCHAR;
        }
        else if (BINARY_TYPES.containsAll(types)) {
            return VarbinaryType.VARBINARY;
        }
        throw new UnsupportedOperationException(format("Incompatible UNION type: '%s'", schema.toString(true)));
    }

    private Type createType(Type valueType)
    {
        Type keyType = VARCHAR;

        MethodHandle keyNativeEquals = typeManager.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
        MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = typeManager.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
        return new MapType(
                keyType,
                valueType,
                keyBlockNativeEquals,
                keyBlockEquals,
                keyNativeHashCode,
                keyBlockHashCode);
    }

    private Type convertArray(Schema schema)
    {
        checkArgument(schema.getType() == ARRAY, "schema is not an ARRAY");
        return new ArrayType(convert(schema.getElementType()));
    }

    private Type convertMap(Schema schema)
    {
        checkArgument(schema.getType() == MAP, "schema is not a MAP");
        return createType(convert(schema.getValueType()));
    }

    private Type convertRecord(Schema schema)
    {
        checkArgument(schema.getType() == RECORD, "schema is not a RECORD");
        //checkState(!schema.getFields().isEmpty(), "Row type must have at least 1 field");
        if (schema.getFields().isEmpty()) {
            return RowType.from(ImmutableList.of(new RowType.Field(Optional.of("dummy"), BooleanType.BOOLEAN)));
        }
        return RowType.from(schema.getFields().stream()
                .map(field -> new RowType.Field(Optional.ofNullable(field.name()), convert(field.schema())))
                .collect(toImmutableList()));
    }
}
