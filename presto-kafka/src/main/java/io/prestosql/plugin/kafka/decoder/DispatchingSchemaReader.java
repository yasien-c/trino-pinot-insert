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
package io.prestosql.plugin.kafka.decoder;

import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Map;

import static io.prestosql.plugin.kafka.decoder.SchemaReader.DUMMY_SCHEMA_READER;
import static java.util.Objects.requireNonNull;

public class DispatchingSchemaReader
{
    private final Map<String, SchemaReader> schemaReaders;

    @Inject
    public DispatchingSchemaReader(Map<String, SchemaReader> schemaReaders)
    {
        requireNonNull(schemaReaders, "schemaReaders is null");
        this.schemaReaders = ImmutableMap.copyOf(schemaReaders);
    }

    public SchemaReader getSchemaReader(String dataFormat)
    {
        return schemaReaders.getOrDefault(dataFormat, DUMMY_SCHEMA_READER);
    }
}
