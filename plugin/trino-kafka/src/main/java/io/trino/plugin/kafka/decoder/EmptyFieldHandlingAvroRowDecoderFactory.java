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
package io.trino.plugin.kafka.decoder;

import com.google.inject.Inject;
import io.trino.decoder.DecoderColumnHandle;
import io.trino.decoder.RowDecoder;
import io.trino.decoder.RowDecoderFactory;
import io.trino.decoder.avro.AvroRowDecoderFactory;
import io.trino.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Set;

import static io.trino.plugin.kafka.schema.confluent.AvroSchemaConverter.EmptyFieldStrategy.ADD_DUMMY;
import static io.trino.plugin.kafka.schema.confluent.ConfluentSessionProperties.getEmptyFieldStrategy;
import static java.util.Objects.requireNonNull;

public class EmptyFieldHandlingAvroRowDecoderFactory
        implements RowDecoderFactory
{
    private final AvroRowDecoderFactory delegate;

    @Inject
    public EmptyFieldHandlingAvroRowDecoderFactory(AvroRowDecoderFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public RowDecoder create(Map<String, String> decoderParams, Set<DecoderColumnHandle> columns, ConnectorSession session)
    {
        RowDecoder rowDecoder = delegate.create(decoderParams, columns, session);
        if (getEmptyFieldStrategy(session) == ADD_DUMMY) {
            return new EmptyFieldHandlingAvroRowDecoder(rowDecoder);
        }
        return rowDecoder;
    }
}
