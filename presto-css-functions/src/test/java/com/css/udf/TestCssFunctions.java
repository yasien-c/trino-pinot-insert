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

import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.operator.scalar.FunctionAssertions;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.sql.analyzer.FeaturesConfig;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.prestosql.util.DateTimeZoneIndex.getDateTimeZone;

@Test(groups = "test-css")
public class TestCssFunctions
        extends AbstractTestFunctions
{
    private static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(getTimeZoneKey("America/Los_Angeles"));

    @BeforeClass
    public void setUp()
    {
        // Necessary for running with "test-css" group since no setup methods from superclass will be run
        // If this is run without "test-css" then functionAssertions will not be null and should not be overwritten.
        if (functionAssertions == null) {
            functionAssertions = new FunctionAssertions(session, new FeaturesConfig());
        }
        functionAssertions.installPlugin(new CssFunctionsPlugin());
    }

    @Test
    public void testToDecimal()
    {
        assertFunction("to_decimal(cast(ROW('usd', 1560209281, cast(380000000 as integer), true) as ROW(currency_code varchar, units bigint, nanos integer, exist boolean)))",
                createDecimalType(38, 9),
                SqlDecimal.of("1560209281.380000000"));
    }

    @Test
    public void testToTimestamp()
    {
        assertFunction("to_timestamp(cast(ROW(1560209281, cast(380000000 as integer), true) as ROW(seconds bigint, nanos integer, exist boolean)))",
                TIMESTAMP,
                sqlTimestampOf(new DateTime(2019, 6, 10, 16, 28, 1, 380, DATE_TIME_ZONE), session));
    }
}
