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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static java.util.Objects.requireNonNull;

public class PinotTable
{
    private final String name;
    private final List<PinotColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final Map<String, ColumnHandle> columnHandles;

    @JsonCreator
    public PinotTable(
            @JsonProperty("name") String name,
            @JsonProperty("columns") List<PinotColumn> columns)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));

        this.columnsMetadata = columns.stream().map(c -> new PinotColumnMetadata(c.getName(), c.getType())).collect(Collectors.toList());
        ImmutableMap.Builder<String, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : this.columnsMetadata) {
            PinotColumnMetadata pinotColumnMetadata = (PinotColumnMetadata) columnMetadata;
            columnHandlesBuilder.put(pinotColumnMetadata.getName(),
                    new PinotColumnHandle(pinotColumnMetadata.getPinotName(), pinotColumnMetadata.getType(), REGULAR));
        }
        this.columnHandles = columnHandlesBuilder.build();
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public List<PinotColumn> getColumns()
    {
        return columns;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }

    public Map<String, ColumnHandle> getColumnHandles()
    {
        return columnHandles;
    }
}
