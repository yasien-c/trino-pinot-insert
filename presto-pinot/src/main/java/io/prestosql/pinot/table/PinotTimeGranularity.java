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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class PinotTimeGranularity
{
    private final String name;
    private final TimeUnit timeUnit;

    public PinotTimeGranularity(TimeGranularitySpec timeGranularitySpec)
    {
        this(timeGranularitySpec.getName(), timeGranularitySpec.getTimeType());
    }

    @JsonCreator
    public PinotTimeGranularity(@JsonProperty String name, @JsonProperty TimeUnit timeUnit)
    {
        this.name = requireNonNull(name, "name is null");
        this.timeUnit = timeUnit;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public TimeUnit getTimeUnit()
    {
        return timeUnit;
    }

    public TimeGranularitySpec toTimeGranularitySpec()
    {
        return new TimeGranularitySpec(FieldSpec.DataType.LONG, timeUnit, name);
    }
}
