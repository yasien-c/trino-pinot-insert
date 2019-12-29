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
package io.prestosql.decoder.avro;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.prestosql.decoder.DecoderColumnHandle;
import io.prestosql.decoder.FieldValueProvider;
import io.prestosql.decoder.RowDecoder;
import io.prestosql.spi.PrestoException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
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
import static org.apache.avro.file.DataFileConstants.MAGIC;

public class AvroRowDecoder
        implements RowDecoder
{
    public static final String NAME = "avro";
    public static final int HEADER_LENGTH = 1 + Integer.BYTES;

    private static ThreadLocal<BinaryDecoder> reuseDecoder = ThreadLocal.withInitial(() -> null);

    private final Schema targetSchema;
    private final Map<DecoderColumnHandle, AvroColumnDecoder> columnDecoders;
    private final Optional<SchemaRegistryClient> schemaRegistryClient;
    private final Optional<LoadingCache<Integer, GenericDatumReader<GenericRecord>>> avroRecordReaderCache;
    private final GenericDatumReader<GenericRecord> defaultAvroRecordReader;
    private final ReaderSupplier readerSupplier;

    public AvroRowDecoder(Schema targetSchema, Set<DecoderColumnHandle> columns, Optional<SchemaRegistryClient> schemaRegistryClient)
    {
        this.targetSchema = requireNonNull(targetSchema, "targetSchema is null");
        requireNonNull(columns, "columns is null");
        columnDecoders = columns.stream()
                .collect(toImmutableMap(identity(), this::createColumnDecoder));
        this.schemaRegistryClient = requireNonNull(schemaRegistryClient, "schemaRegistryClient is null");
        if (schemaRegistryClient.isPresent()) {
            avroRecordReaderCache = Optional.of(CacheBuilder.newBuilder()
                    .maximumSize(1000)
                    .build(CacheLoader.from(this::lookupReader)));
        }
        else {
            avroRecordReaderCache = Optional.empty();
        }
        defaultAvroRecordReader = new GenericDatumReader<>(targetSchema);

        if (schemaRegistryClient.isPresent()) {
            readerSupplier = schemaId -> avroRecordReaderCache.get().getUnchecked(schemaId);
        }
        else {
            readerSupplier = schemaId -> defaultAvroRecordReader;
        }
    }

    private GenericDatumReader<GenericRecord> lookupReader(Integer id)
    {
        try {
            Schema sourceSchema = schemaRegistryClient.get().getById(id);
            return new GenericDatumReader<>(sourceSchema, targetSchema);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (RestClientException e) {
            throw new RuntimeException(e);
        }
    }

    private AvroColumnDecoder createColumnDecoder(DecoderColumnHandle columnHandle)
    {
        return new AvroColumnDecoder(columnHandle);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap)
    {
        Optional<GenericRecord> candidate = decodeAvroDataFileFormat(data);
        if (!candidate.isPresent()) {
            candidate = decodeAvroSchemaRegistryFormat(data);
        }

        GenericRecord avroRecord = candidate
                        .orElseThrow(() -> new PrestoException(GENERIC_INTERNAL_ERROR, "Decoding Avro record failed."));

        return Optional.of(columnDecoders.entrySet().stream()
                .collect(toImmutableMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue().decodeField(avroRecord))));
    }

    private void closeQuietly(DataFileStream<GenericRecord> stream)
    {
        try {
            if (stream != null) {
                stream.close();
            }
        }
        catch (IOException ignored) {
        }
    }

    private static boolean isAvroDataFileFormat(byte[] data)
    {
        if (data.length >= MAGIC.length) {
            for (int i = 0; i < MAGIC.length; i++) {
                if (data[i] != MAGIC[i]) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private static boolean isAvroSchemaRegistryFormat(byte[] data)
    {
        if (data.length >= HEADER_LENGTH) {
            if (data[0] == 0) {
                return true;
            }
        }
        return false;
    }

    private Optional<GenericRecord> decodeAvroDataFileFormat(byte[] data)
    {
        if (!isAvroDataFileFormat(data)) {
            return Optional.empty();
        }
        DataFileStream<GenericRecord> dataFileReader = null;
        try {
            // Assumes producer uses DataFileWriter or data comes in this particular format.
            // TODO: Support other forms for producers
            dataFileReader = new DataFileStream<>(new ByteArrayInputStream(data), defaultAvroRecordReader);
            if (!dataFileReader.hasNext()) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "No avro record found");
            }
            GenericRecord avroRecord = dataFileReader.next();
            if (dataFileReader.hasNext()) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unexpected extra record found");
            }
            return Optional.of(avroRecord);
        }
        catch (Exception e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Decoding Avro record failed.", e);
        }
        finally {
            closeQuietly(dataFileReader);
        }
    }

    private Optional<GenericRecord> decodeAvroSchemaRegistryFormat(byte[] data)
    {
        if (!isAvroSchemaRegistryFormat(data)) {
            return Optional.empty();
        }
        ByteBuffer buffer = ByteBuffer.wrap(data, 1, Integer.BYTES);
        GenericDatumReader<GenericRecord> avroRecordReader = readerSupplier.getAvroRecordReader(buffer.getInt());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, HEADER_LENGTH, data.length - HEADER_LENGTH, reuseDecoder.get());
        reuseDecoder.set(decoder);
        try {
            return Optional.of(avroRecordReader.read(null, decoder));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private interface ReaderSupplier
    {
        GenericDatumReader<GenericRecord> getAvroRecordReader(int schemaId);
    }
}
