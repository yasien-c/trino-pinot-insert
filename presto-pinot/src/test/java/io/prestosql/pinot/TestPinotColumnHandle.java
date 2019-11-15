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

import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static io.prestosql.pinot.MetadataUtil.COLUMN_CODEC;
import static io.prestosql.pinot.PinotColumnHandle.PinotColumnType.DERIVED;
import static io.prestosql.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestPinotColumnHandle
{
    private final PinotColumnHandle columnHandle = new PinotColumnHandle("columnName", VARCHAR, REGULAR);

    @Test
    public void testJsonRoundTrip()
    {
        String json = COLUMN_CODEC.toJson(columnHandle);
        PinotColumnHandle copy = COLUMN_CODEC.fromJson(json);
        assertEquals(copy, columnHandle);
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester
                .equivalenceTester()
                .addEquivalentGroup(
                        new PinotColumnHandle("columnName", VARCHAR, REGULAR),
                        new PinotColumnHandle("columnName", VARCHAR, DERIVED),
                        new PinotColumnHandle("columnName", BIGINT, REGULAR),
                        new PinotColumnHandle("columnName", BIGINT, DERIVED))
                .addEquivalentGroup(
                        new PinotColumnHandle("columnNameX", VARCHAR, REGULAR),
                        new PinotColumnHandle("columnNameX", VARCHAR, DERIVED),
                        new PinotColumnHandle("columnNameX", BIGINT, REGULAR),
                        new PinotColumnHandle("columnNameX", BIGINT, DERIVED))
                .check();
    }
}
