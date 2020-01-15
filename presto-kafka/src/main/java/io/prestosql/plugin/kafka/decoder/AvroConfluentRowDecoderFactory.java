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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.decoder.RowDecoderFactory;
import org.apache.avro.Schema;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AvroConfluentRowDecoderFactory
        implements RowDecoderFactory
{
    public static final String SCHEMA_REGISTRY_KEY = "schemaRegistryUrl";

    @Override
    public RowDecoder create(Map<String, String> decoderParams, Set<DecoderColumnHandle> columns)
    {
        checkState(decoderParams.containsKey(SCHEMA_REGISTRY_KEY), "Missing required parameter '%s'", SCHEMA_REGISTRY_KEY);
        String dataSchema = requireNonNull(decoderParams.get("dataSchema"), "dataSchema cannot be null");
        Schema parsedSchema = (new Schema.Parser()).parse(dataSchema);

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(decoderParams.get(SCHEMA_REGISTRY_KEY), 1000);
        return new AvroConfluentRowDecoder(parsedSchema, columns, schemaRegistryClient);
    }
}
