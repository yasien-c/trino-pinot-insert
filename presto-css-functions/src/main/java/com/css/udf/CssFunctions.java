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
package com.css.udf;

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.math.BigDecimal;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.Decimals.encodeScaledValue;
import static io.prestosql.spi.type.IntegerType.INTEGER;

public class CssFunctions
{
    private CssFunctions() {}

    @ScalarFunction
    @SqlType(StandardTypes.TIMESTAMP)
    public static long toTimestamp(ConnectorSession session, @SqlType("ROW(seconds bigint, nanos integer, exist boolean)") Block input)
    {
        long seconds = (long) BIGINT.getObjectValue(session, input, 0);
        int nanos = (int) INTEGER.getObjectValue(session, input, 1);
        return Math.round(seconds * 1000 + nanos / 1_000_000.0D);
    }

    @ScalarFunction
    @SqlType("decimal(38,9)")
    public static Slice toDecimal(ConnectorSession session, @SqlType("ROW(currency_code varchar, units bigint, nanos integer, exist boolean)") Block input)
    {
        long units = (long) BIGINT.getObjectValue(session, input, 1);
        int nanos = (int) INTEGER.getObjectValue(session, input, 2);
        return encodeScaledValue(new BigDecimal(units).add(new BigDecimal(nanos).movePointLeft(9)).setScale(9));
    }
}
