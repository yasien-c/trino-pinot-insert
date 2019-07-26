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
package io.prestosql.decoder.avro;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.ParameterKind;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeParameter;
import org.apache.avro.Schema;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.block.MethodHandleUtil.compose;
import static io.prestosql.spi.block.MethodHandleUtil.nativeValueGetter;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.NULL;
import static org.apache.avro.Schema.Type.RECORD;
import static org.apache.avro.Schema.Type.UNION;

public class AvroSchemaConverter
{
    private final TypeManager typeManager;

    public AvroSchemaConverter(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    public Type convert(Schema schema)
    {
        switch (schema.getType()) {
            case INT:
            case LONG:
                return BIGINT;
            case ENUM:
            case STRING:
                return VARCHAR;
            case FLOAT:
            case DOUBLE:
                return DOUBLE;
            case BOOLEAN:
                return BOOLEAN;
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

    private Type convertArray(Schema schema)
    {
        checkArgument(schema.getType() == ARRAY, "schema is not an ARRAY");
        return new ArrayType(convert(schema.getElementType()));
    }

    private Type convertUnion(Schema schema)
    {
        checkArgument(schema.getType() == UNION, "schema is not a UNION");
        if (schema.getTypes().size() != 2 || schema.getTypes().get(0).getType() != NULL) {
            throw new UnsupportedOperationException("Complex UNION types are not supported. Only simple nullable types are supported");
        }
        return convert(schema.getTypes().get(1));
    }

    private Type convertMap(Schema schema)
    {
        checkArgument(schema.getType() == MAP, "schema is not a MAP");
        return createType(convert(schema.getValueType()));
    }

    private Type convertRecord(Schema schema)
    {
        checkArgument(schema.getType() == RECORD, "schema is not a RECORD");
        if (schema.getFields().isEmpty()) {
            return RowType.from(ImmutableList.of(new RowType.Field(Optional.of("exists"), BOOLEAN)));
        }
        return RowType.from(schema.getFields().stream()
                .map(field -> new RowType.Field(Optional.ofNullable(field.name()), convert(field.schema())))
                .collect(toImmutableList()));
    }

    private Type createType(Type value)
    {
        TypeParameter firstParameter = TypeParameter.of(VARCHAR);
        TypeParameter secondParameter = TypeParameter.of(value);
        checkArgument(secondParameter.getKind() == ParameterKind.TYPE,
                "Expected key and type to be types, got %s",
                value);

        Type keyType = firstParameter.getType();
        Type valueType = secondParameter.getType();

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
}
