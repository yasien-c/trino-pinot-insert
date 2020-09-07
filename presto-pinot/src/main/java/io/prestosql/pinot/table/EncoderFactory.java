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

import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.apache.pinot.spi.data.FieldSpec;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.pinot.table.PrestoToPinotSchemaConverter.isSupportedType;
import static java.util.Objects.requireNonNull;

public class EncoderFactory
{
    private EncoderFactory() {}

    public static Encoder createEncoder(FieldSpec fieldSpec, Type prestoType)
    {
        requireNonNull(prestoType, "prestoType is null");
        requireNonNull(fieldSpec, "fieldSpec is null");
        checkState(isSupportedType(prestoType), "Unsupported type %s", prestoType);
        String fieldName = fieldSpec.getName();
        if (prestoType instanceof ArrayType) {
            checkState(!fieldSpec.isSingleValueField(), "Pinot and presto types are incompatible: %s, %s", prestoType, fieldSpec);
            ArrayType arrayType = (ArrayType) prestoType;
            return new ArrayEncoder(fieldSpec, arrayType.getElementType());
        }
        checkState(fieldSpec.isSingleValueField(), "Pinot and presto types are incompatible: %s, %s", prestoType, fieldSpec);
        if (prestoType instanceof BigintType) {
            return new BigintEncoder();
        }
        else if (prestoType instanceof IntegerType) {
            return new IntegerEncoder();
        }
        else if (prestoType instanceof SmallintType) {
            return new SmallintEncoder();
        }
        else if (prestoType instanceof TinyintType) {
            return new TinyintEncoder();
        }
        else if (prestoType instanceof DoubleType) {
            return new DoubleEncoder();
        }
        else if (prestoType instanceof RealType) {
            return new RealEncoder();
        }
        else if (prestoType instanceof VarcharType) {
            return new VarcharEncoder();
        }
        throw new UnsupportedOperationException();
    }
}
