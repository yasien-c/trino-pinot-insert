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
package io.trino.plugin.pinot.encoders;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileRecordReader;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileWriter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.pinot.core.segment.processing.framework.MergeType.CONCAT;
import static org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager.DATA_FILE_NAME;
import static org.apache.pinot.core.segment.processing.utils.SegmentProcessorUtils.getFieldSpecs;

public class GenericRowReadersBuilder
{
    private final Path directory;
    private final List<PinotColumnHandle> columnHandles;
    private final Map<Integer, Encoder> channelToEncoderMap;
    private final List<FieldSpec> fieldSpecs;
    private final Map<String, FieldSpec> fieldSpecMap;
    private final int sortedColumnCount;
    private final boolean isNullHandlingEnabled;
    private final Function<GenericRow, String> rowToSegmentFunction;
    private final Map<String, GenericRowFileManager> fileManagerMap = new HashMap<>();
    private GenericRow[] genericRowBuffer = new GenericRow[0];
    private final long thresholdBytes;

    public GenericRowReadersBuilder(List<PinotColumnHandle> columnHandles, Map<String, FieldSpec> fieldSpecMap, Map<Integer, Encoder> channelToEncoderMap, TableConfig tableConfig, Schema schema, Function<GenericRow, String> rowToSegmentFunction, Path taskTempLocation, long thresholdBytes)
    {
        this.directory = requireNonNull(taskTempLocation, "taskTempLocation is null").resolve("input");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.fieldSpecMap = requireNonNull(fieldSpecMap, "fieldSpecMap is null");
        this.channelToEncoderMap = requireNonNull(channelToEncoderMap, "channelToEncoderMap is null");
        requireNonNull(tableConfig, "tableConfig is null");
        this.isNullHandlingEnabled = tableConfig.getIndexingConfig().isNullHandlingEnabled();
        requireNonNull(schema, "schema is null");
        Pair<List<FieldSpec>, Integer> fieldSpecsAndSortColumns = getFieldSpecs(schema, CONCAT, getSortedColumns(tableConfig));
        this.fieldSpecs = fieldSpecsAndSortColumns.getLeft();
        this.sortedColumnCount = fieldSpecsAndSortColumns.getRight();
        checkState(!fieldSpecs.isEmpty(), "fieldSpecs is empty");
        this.rowToSegmentFunction = requireNonNull(rowToSegmentFunction, "rowToSegmentFunction is null");
        this.thresholdBytes = thresholdBytes;
        checkState(thresholdBytes > 0, "Threshold bytes must be > 0");
    }

    private GenericRowFileManager createFileManager(String segmentName)
    {
        try {
            Path outputDirectory = directory.resolve(segmentName);
            Files.createDirectories(outputDirectory);
            return new GenericRowFileManager(outputDirectory.toFile(), fieldSpecs, isNullHandlingEnabled, sortedColumnCount);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static List<String> getSortedColumns(TableConfig tableConfig)
    {
        // Pinot may return null values for indexingConfig and sortedColumn which is a list of sorted columns
        requireNonNull(tableConfig, "tableConfig is null");
        if (tableConfig.getIndexingConfig() == null || tableConfig.getIndexingConfig().getSortedColumn() == null) {
            return ImmutableList.of();
        }
        return tableConfig.getIndexingConfig().getSortedColumn();
    }

    private static void writeRecord(GenericRowFileWriter fileWriter, GenericRow row)
    {
        try {
            fileWriter.write(row);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public synchronized void append(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(page.getChannelCount() == columnHandles.size(), "Unexpected channel count for page: %s", page.getChannelCount());
        growGenericRowBufferIfNecessary(page.getPositionCount());
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            // TODO: add virtual
            Block block = page.getBlock(channel);
            String fieldName = columnHandles.get(channel).getColumnName();
            FieldSpec fieldSpec = fieldSpecMap.get(fieldName);
            Object defaultNullValue;
            if (fieldSpec.isSingleValueField()) {
                defaultNullValue = fieldSpecMap.get(fieldName).getDefaultNullValue();
            }
            else {
                defaultNullValue = new Object[] {fieldSpecMap.get(fieldName).getDefaultNullValue()};
            }
            Encoder encoder = channelToEncoderMap.get(channel);
            for (int position = 0; position < block.getPositionCount(); position++) {
                Object value = encoder.encode(block, position);
                if (value == null) {
                    genericRowBuffer[position].putDefaultNullValue(fieldName, defaultNullValue);
                }
                else {
                    genericRowBuffer[position].putValue(fieldName, value);
                }
            }
        }
        for (int position = 0; position < page.getPositionCount(); position++) {
            GenericRow row = genericRowBuffer[position];
            String segment = rowToSegmentFunction.apply(row);
            writeRecord(getOrCreateFileWriter(segment), row);
            row.clear();
        }
    }

    private void growGenericRowBufferIfNecessary(int positionCount)
    {
        if (genericRowBuffer.length < positionCount) {
            GenericRow[] newGenericRowBuffer = new GenericRow[positionCount];
            System.arraycopy(genericRowBuffer, 0, newGenericRowBuffer, 0, genericRowBuffer.length);
            for (int index = genericRowBuffer.length; index < newGenericRowBuffer.length; index++) {
                newGenericRowBuffer[index] = new GenericRow();
            }
            genericRowBuffer = newGenericRowBuffer;
        }
    }

    private GenericRowFileWriter getOrCreateFileWriter(String segment)
    {
        try {
            return fileManagerMap.computeIfAbsent(segment, k -> createFileManager(segment)).getFileWriter();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void closeFileWriter(GenericRowFileManager fileManager)
    {
        try {
            fileManager.getFileWriter().close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private GenericRowFileReader getReader(GenericRowFileManager fileManager)
    {
        try {
            return fileManager.getFileReader();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public synchronized Map<String, List<RecordReader>> build()
    {
        Map<String, List<RecordReader>> readersBuilder = new HashMap<>();
        for (Map.Entry<String, GenericRowFileManager> entry : fileManagerMap.entrySet()) {
            String segment = entry.getKey();
            GenericRowFileManager fileManager = entry.getValue();
            closeFileWriter(fileManager);
            GenericRowFileReader reader = getReader(fileManager);
            int totalRows = reader.getNumRows();
            if (totalRows == 0) {
                continue;
            }
            File dataFile = directory.resolve(segment).resolve(DATA_FILE_NAME).toFile();
            if (!dataFile.exists()) {
                throw new UncheckedIOException(new FileNotFoundException(format("File %s not found", dataFile.getAbsolutePath())));
            }
            long size = dataFile.length();
            GenericRowFileRecordReader recordReader = reader.getRecordReader();

            int segmentCount = toIntExact(Math.max(size / thresholdBytes, 1L));
            int rowsPerSegment = totalRows / segmentCount;
            if (totalRows % segmentCount > 0) {
                rowsPerSegment++;
            }
            rowsPerSegment = Math.max(rowsPerSegment, 1);
            ImmutableList.Builder<RecordReader> recordReaderBuilder = ImmutableList.builder();
            for (int startRow = 0; startRow < totalRows; ) {
                int endRowExclusive = Math.min(startRow + rowsPerSegment, totalRows);
                recordReaderBuilder.add(recordReader.getRecordReaderForRange(startRow, endRowExclusive));
                startRow = endRowExclusive;
            }
            readersBuilder.put(segment, recordReaderBuilder.build());
        }
        genericRowBuffer = new GenericRow[0];
        return readersBuilder;
    }
}
