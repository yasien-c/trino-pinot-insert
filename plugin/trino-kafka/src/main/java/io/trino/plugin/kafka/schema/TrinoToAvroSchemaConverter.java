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
package io.trino.plugin.kafka.schema;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Schema;

import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static java.lang.String.format;

public class TrinoToAvroSchemaConverter
{
    private TrinoToAvroSchemaConverter() {}

    public static Schema fromTrinoType(Type trinoType)
    {
        if (trinoType instanceof BigintType) {
            return Schema.create(Schema.Type.LONG);
        }
        else if (trinoType instanceof IntegerType || trinoType instanceof SmallintType || trinoType instanceof TinyintType) {
            return Schema.create(Schema.Type.INT);
        }
        else if (trinoType instanceof VarcharType) {
            return Schema.create(Schema.Type.STRING);
        }
        else if (trinoType instanceof BooleanType) {
            return Schema.create(Schema.Type.BOOLEAN);
        }
        else if (trinoType instanceof DoubleType) {
            return Schema.create(Schema.Type.DOUBLE);
        }
        else if (trinoType instanceof RealType) {
            return Schema.create(Schema.Type.FLOAT);
        }
        else if (trinoType instanceof VarbinaryType) {
            return Schema.create(Schema.Type.BYTES);
        }
        else if (trinoType instanceof DateType) {
            return Schema.create(Schema.Type.INT);
        }
        else if (trinoType instanceof TimestampType && ((TimestampType) trinoType).getPrecision() <= MAX_SHORT_PRECISION) {
            return Schema.create(Schema.Type.LONG);
        }
        else if (trinoType instanceof TimeType) {
            return Schema.create(Schema.Type.LONG);
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported type for single value key column: %s", trinoType));
        }
    }
}
