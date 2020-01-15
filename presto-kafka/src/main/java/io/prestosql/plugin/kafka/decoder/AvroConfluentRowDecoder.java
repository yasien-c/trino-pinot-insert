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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.decoder.avro.AvroColumnDecoder;
import io.prestosql.spi.PrestoException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Functions.identity;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class AvroConfluentRowDecoder
        implements RowDecoder
{
    public static final String NAME = "avro-confluent";
    public static final int HEADER_LENGTH = 1 + Integer.BYTES;

    private static ThreadLocal<BinaryDecoder> reuseDecoder = ThreadLocal.withInitial(() -> null);

    private final Schema targetSchema;
    private final Map<DecoderColumnHandle, AvroColumnDecoder> columnDecoders;
    private final SchemaRegistryClient schemaRegistryClient;
    private final LoadingCache<Integer, GenericDatumReader<GenericRecord>> avroRecordReaderCache;

    public AvroConfluentRowDecoder(Schema targetSchema, Set<DecoderColumnHandle> columns, SchemaRegistryClient schemaRegistryClient)
    {
        this.targetSchema = requireNonNull(targetSchema, "targetSchema is null");
        requireNonNull(columns, "columns is null");
        columnDecoders = columns.stream()
                .collect(toImmutableMap(identity(), AvroColumnDecoder::new));
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        avroRecordReaderCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(CacheLoader.from(this::lookupReader));
    }

    private GenericDatumReader<GenericRecord> lookupReader(Integer id)
    {
        try {
            Schema sourceSchema = schemaRegistryClient.getById(id);
            return new GenericDatumReader<>(sourceSchema, targetSchema);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap)
    {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(data, 1, Integer.BYTES);
            int schemaId = buffer.getInt();
            GenericDatumReader<GenericRecord> avroRecordReader = avroRecordReaderCache.getUnchecked(schemaId);
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, HEADER_LENGTH, data.length - HEADER_LENGTH, reuseDecoder.get());
            reuseDecoder.set(decoder);

            GenericRecord avroRecord = avroRecordReader.read(null, decoder);
            ImmutableMap.Builder<DecoderColumnHandle, FieldValueProvider> decodedColumnBuilder = ImmutableMap.builder();

            for (Map.Entry<DecoderColumnHandle, AvroColumnDecoder> entry : columnDecoders.entrySet()) {
                decodedColumnBuilder.put(entry.getKey(), entry.getValue().decodeField(avroRecord));
            }
            return Optional.of(decodedColumnBuilder.build());
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Decoding Avro record failed.", e);
        }
    }
}
