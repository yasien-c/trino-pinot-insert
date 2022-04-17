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
package io.trino.plugin.pinot;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.encoders.Encoder;
import io.trino.plugin.pinot.encoders.ErrorSuppressingRecordTransformerWrapper;
import io.trino.plugin.pinot.encoders.ProcessedSegmentMetadata;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSink;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileReader;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.segment.spi.creator.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;
import org.apache.pinot.spi.utils.retry.RetryPolicies;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static io.trino.plugin.pinot.encoders.EncoderFactory.createEncoder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toMap;
import static org.apache.pinot.core.segment.processing.framework.MergeType.CONCAT;
import static org.apache.pinot.core.segment.processing.utils.SegmentProcessorUtils.getFieldSpecs;

public class PinotAsyncDiskBasedMetadataPushPageSink
        implements ConnectorPageSink
{
    public static final JsonCodec<ProcessedSegmentMetadata> PROCESSED_SEGMENT_METADATA_JSON_CODEC = jsonCodec(ProcessedSegmentMetadata.class);

    private static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();
    private static final String DIMENSION_TABLE_SEGMENT_KEY = "dim";

    private final PinotFS pinotFS;
    private final PinotInsertTableHandle pinotInsertTableHandle;
    private final List<URI> pinotControllerUrls;
    private final String taskTempLocation;
    private final String segmentTempLocation;
    private final String segmentBuildLocation;
    private final TableConfig tableConfig;
    private final Schema pinotSchema;
    private final String timeColumnName;
    private final PinotClient pinotClient;
    private final URI segmentDeepstoreURI;
    private final Map<String, FieldSpec> fieldSpecMap;
    private final Map<Integer, Encoder> channelToEncoderMap;
    private final Function<GenericRow, String> rowToSegmentFunction;
    private final long createdAtEpochMillis;
    private final long thresholdBytes;
    private final ListeningExecutorService metadataPushExecutor;
    private final ListeningExecutorService segmentBuilderExecutor;
    private final Map<String, GenericRowBuffer> segmentToGenericRowBuffer = new ConcurrentHashMap<>();
    private final AtomicBoolean isCancelled = new AtomicBoolean();
    private final Queue<ListenableFuture<SegmentMetadataDescriptor>> segmentBuildFutures = new ConcurrentLinkedQueue<>();
    private final long segmentUploadTimeoutMillis;
    private final long finishInsertTimeoutMillis;
    private final Ticker ticker = Ticker.systemTicker();
    private final boolean segmentBuildOnHeapEnabled;

    @SuppressWarnings("unchecked")
    public PinotAsyncDiskBasedMetadataPushPageSink(ListeningExecutorService segmentBuilderExecutor, ListeningExecutorService metadataPushExecutorService, URI segmentDeepstoreURI, PinotFS pinotFS, PinotInsertTableHandle pinotInsertTableHandle, PinotClient pinotClient, String segmentCreationBaseDirectory, long thresholdBytes, long segmentUploadTimeoutMillis, long finishInsertTimeoutMillis, boolean segmentBuildOnHeapEnabled, List<URI> pinotControllerUrls)
    {
        this.segmentBuilderExecutor = requireNonNull(segmentBuilderExecutor, "segmentBuilderExecutor is null");
        this.metadataPushExecutor = requireNonNull(metadataPushExecutorService, "metadataPushExecutorService is null");
        this.segmentDeepstoreURI = requireNonNull(segmentDeepstoreURI, "segmentDeepstoreURI is null");
        this.pinotFS = requireNonNull(pinotFS, "pinotFS is null");
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
        this.pinotInsertTableHandle = requireNonNull(pinotInsertTableHandle, "pinotInsertTableHandle is null");
        this.pinotControllerUrls = requireNonNull(pinotControllerUrls, "pinotControllerUrls is null");
        this.taskTempLocation = String.join(File.separator, segmentCreationBaseDirectory, UUID.randomUUID().toString());
        this.segmentTempLocation = String.join(File.separator, taskTempLocation, pinotInsertTableHandle.getPinotTableName(), "segments");
        this.segmentBuildLocation = String.join(File.separator, taskTempLocation, pinotInsertTableHandle.getPinotTableName(), "segmentBuilder");
        this.tableConfig = pinotClient.getTableConfig(pinotInsertTableHandle.getPinotTableName()).getOfflineConfig().get();
        this.pinotSchema = pinotClient.getTableSchema(pinotInsertTableHandle.getPinotTableName());
        this.timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();

        if (pinotInsertTableHandle.getDateTimeField().isPresent()) {
            PinotDateTimeField dateTimeFieldTransformer = pinotInsertTableHandle.getDateTimeField().get();
            LongUnaryOperator transformFunction = dateTimeFieldTransformer.getToMillisTransform();
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            this.rowToSegmentFunction = row -> dateFormat.format(Instant.ofEpochMilli(transformFunction.applyAsLong((long) row.getValue(timeColumnName))).atZone(ZoneId.of("UTC")).toLocalDate());
        }
        else {
            this.rowToSegmentFunction = row -> DIMENSION_TABLE_SEGMENT_KEY;
        }
        this.fieldSpecMap = pinotSchema.getAllFieldSpecs().stream()
                .collect(toMap(fieldSpec -> fieldSpec.getName(), fieldSpec -> fieldSpec));
        ImmutableMap.Builder<Integer, Encoder> channelToEncoderMapBuilder = ImmutableMap.builder();
        for (int channel = 0; channel < pinotInsertTableHandle.getColumnHandles().size(); channel++) {
            PinotColumnHandle columnHandle = pinotInsertTableHandle.getColumnHandles().get(channel);
            // TODO: add virtual
            channelToEncoderMapBuilder.put(channel, createEncoder(fieldSpecMap.get(columnHandle.getColumnName()), columnHandle.getDataType()));
        }
        this.channelToEncoderMap = channelToEncoderMapBuilder.buildOrThrow();
        this.thresholdBytes = thresholdBytes;
        this.createdAtEpochMillis = Instant.now().toEpochMilli();
        this.segmentUploadTimeoutMillis = segmentUploadTimeoutMillis;
        this.finishInsertTimeoutMillis = finishInsertTimeoutMillis;
        this.segmentBuildOnHeapEnabled = segmentBuildOnHeapEnabled;
        try {
            Files.createDirectories(Paths.get(segmentTempLocation));
            Files.createDirectories(Paths.get(segmentBuildLocation));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        List<GenericRowWithSize> rows = toGenericRows(page);
        Map<String, List<GenericRowWithSize>> segmentToRows = new HashMap<>();
        for (GenericRowWithSize row : rows) {
            String segmentDate = rowToSegmentFunction.apply(row.getRow());
            segmentToRows.computeIfAbsent(segmentDate, k -> new ArrayList<>()).add(row);
        }
        for (Map.Entry<String, List<GenericRowWithSize>> entry : segmentToRows.entrySet()) {
            segmentToGenericRowBuffer.computeIfAbsent(entry.getKey(), k -> new GenericRowBuffer()).add(entry.getValue());
        }
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        List<ListenableFuture<ProcessedSegmentMetadata>> metadataUploadFutures = new ArrayList<>();
        try {
            segmentToGenericRowBuffer.values().forEach(GenericRowBuffer::finish);
            List<SegmentMetadataDescriptor> uploadedSegments = Futures.allAsList(segmentBuildFutures)
                    .get(finishInsertTimeoutMillis, MILLISECONDS);
            if (!uploadedSegments.isEmpty()) {
                List<String> segmentsToReplace = segmentToGenericRowBuffer.keySet().stream()
                        .flatMap(segmentDate -> getSegmentsToReplace(segmentDate).stream())
                        .collect(toImmutableList());
                List<String> newSegments = uploadedSegments.stream()
                        .map(SegmentMetadataDescriptor::getSegmentName)
                        .collect(toImmutableList());
                PinotClient.PinotStartReplaceSegmentsResponse replaceSegmentsResponse = startReplaceSegments(segmentsToReplace, newSegments);

                for (SegmentMetadataDescriptor uploadedSegment : uploadedSegments) {
                    List<Header> headers = new ArrayList<>();
                    headers.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.DOWNLOAD_URI, uploadedSegment.getSegmentURI().toString()));
                    headers.add(new BasicHeader(FileUploadDownloadClient.CustomHeaders.UPLOAD_TYPE,
                            FileUploadDownloadClient.FileUploadType.METADATA.toString()));
                    metadataUploadFutures.add(metadataPushExecutor.submit(() ->
                    {
                        try {
                            long beginNanos = ticker.read();
                            RetryPolicies.exponentialBackoffRetryPolicy(3, 1000, 5).attempt(
                                    () -> {
                                        try {
                                            SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT
                                                    .uploadSegmentMetadata(FileUploadDownloadClient.getUploadSegmentURI(getControllerUrl()), uploadedSegment.getSegmentName(),
                                                            uploadedSegment.getSegmentMetadataFile(), headers, FileUploadDownloadClient.makeTableParam(pinotInsertTableHandle.getPinotTableName()),
                                                            FileUploadDownloadClient.DEFAULT_SOCKET_TIMEOUT_MS);
                                            checkState(response.getStatusCode() == 200, "Unexpected response: '%s'", response.getResponse());
                                            return true;
                                        }
                                        catch (Exception e) {
                                            return false;
                                        }
                                    });
                            long metadataPushTimeMillis = ticker.read() - beginNanos;
                            return new ProcessedSegmentMetadata(uploadedSegment.getSegmentName(),
                                    uploadedSegment.getSegmentDescriptor().getSegmentBuildTimeMillis(),
                                    uploadedSegment.getSegmentPushTimeMillis(),
                                    NANOSECONDS.toMillis(metadataPushTimeMillis),
                                    uploadedSegment.getSegmentDescriptor().getSegmentSizeBytes(),
                                    uploadedSegment.getSegmentDescriptor().getCompressedSegmentSizeBytes(),
                                    uploadedSegment.getSegmentDescriptor().getSegmentRows());
                        }
                        catch (Exception e) {
                            throwIfUnchecked(e);
                            throw new RuntimeException(e);
                        }
                    }));
                }
                List<ProcessedSegmentMetadata> processedSegmentMetadata = Futures.allAsList(metadataUploadFutures).get(finishInsertTimeoutMillis, MILLISECONDS);
                endReplaceSegments(replaceSegmentsResponse, newSegments);
                pinotClient.deleteSegments(pinotInsertTableHandle.getPinotTableName(), segmentsToReplace);
                return completedFuture(processedSegmentMetadata.stream()
                        .map(completionMetadata -> wrappedBuffer(PROCESSED_SEGMENT_METADATA_JSON_CODEC.toJsonBytes(completionMetadata)))
                        .collect(toImmutableList()));
            }
            return completedFuture(ImmutableList.of());
        }
        catch (Exception e) {
            metadataUploadFutures.forEach(this::cancelFutureIgnoringExceptions);
            segmentBuildFutures.forEach(this::cancelFutureIgnoringExceptions);
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        finally {
            cleanup();
        }
    }

    private void cancelFutureIgnoringExceptions(Future<?> future)
    {
        try {
            future.cancel(true);
        }
        catch (Exception e) {
            // ignored
        }
    }

    private PinotClient.PinotStartReplaceSegmentsResponse startReplaceSegments(List<String> segmentsToReplace, List<String> newSegments)
    {
        try {
            return pinotClient.startReplaceSegments(pinotInsertTableHandle.getPinotTableName(), segmentsToReplace, newSegments);
        }
        catch (PinotException e) {
            PinotClient.PinotStartReplaceSegmentsResponse response = pinotClient.startReplaceSegments(pinotInsertTableHandle.getPinotTableName(), ImmutableList.of(), newSegments);
            pinotClient.deleteSegments(pinotInsertTableHandle.getPinotTableName(), segmentsToReplace);
            return response;
        }
    }

    private void endReplaceSegments(PinotClient.PinotStartReplaceSegmentsResponse response, List<String> newSegments)
    {
        try {
            pinotClient.endReplaceSegments(pinotInsertTableHandle.getPinotTableName(), response);
        }
        catch (Exception e) {
            pinotClient.deleteSegments(pinotInsertTableHandle.getPinotTableName(), newSegments);
            throw e;
        }
    }

    private List<String> getSegmentsToReplace(String segmentDate)
    {
        return pinotClient.getSegments(pinotInsertTableHandle.getPinotTableName()).getOffline().stream()
                .filter(segment -> segment.contains(segmentDate))
                .collect(toImmutableList());
    }

    @Override
    public void abort()
    {
        isCancelled.set(true);
        segmentBuildFutures.forEach(this::cancelFutureIgnoringExceptions);
        cleanup();
    }

    private void cleanup()
    {
        try {
            deleteRecursively(Paths.get(taskTempLocation), ALLOW_INSECURE);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void throwIfCancelled()
    {
        if (isCancelled.get()) {
            throw new PinotException(PINOT_EXCEPTION, Optional.empty(), "Insert is cancelled");
        }
    }

    private URI getControllerUrl()
    {
        return pinotControllerUrls.get(ThreadLocalRandom.current().nextInt(pinotControllerUrls.size()));
    }

    private List<GenericRowWithSize> toGenericRows(Page page)
    {
        requireNonNull(page, "page is null");
        List<PinotColumnHandle> columnHandles = pinotInsertTableHandle.getColumnHandles();
        checkState(page.getChannelCount() == columnHandles.size(), "Unexpected channel count for page: %s", page.getChannelCount());
        if (page.getPositionCount() <= 0) {
            return ImmutableList.of();
        }
        long[] dataSize = new long[page.getBlock(0).getPositionCount()];
        GenericRow[] rows = new GenericRow[page.getBlock(0).getPositionCount()];
        for (int position = 0; position < page.getPositionCount(); position++) {
            rows[position] = new GenericRow();
        }
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            // TODO: add virtual
            Block block = page.getBlock(channel);
            String fieldName = columnHandles.get(channel).getColumnName();
            FieldSpec fieldSpec = fieldSpecMap.get(fieldName);
            Object defaultNullValue;
            if (fieldSpec.isSingleValueField()) {
                defaultNullValue = fieldSpec.getDefaultNullValue();
            }
            else {
                defaultNullValue = new Object[] {fieldSpec.getDefaultNullValue()};
            }

            Encoder encoder = channelToEncoderMap.get(channel);
            for (int position = 0; position < block.getPositionCount(); position++) {
                Object value = encoder.encode(block, position);
                if (value == null) {
                    rows[position].putDefaultNullValue(fieldName, defaultNullValue);
                }
                else {
                    rows[position].putValue(fieldName, value);
                }
                dataSize[position] += block.getEstimatedDataSizeForStats(position);
            }
        }
        ImmutableList.Builder<GenericRowWithSize> rowWithSizeBuilder = ImmutableList.builder();
        for (int position = 0; position < rows.length; position++) {
            rowWithSizeBuilder.add(new GenericRowWithSize(rows[position], dataSize[position]));
        }
        return rowWithSizeBuilder.build();
    }

    private static class SegmentDescriptor
    {
        private final String segmentName;
        private final Path segmentTgzPath;
        private final Path segmentOutputDirectory;
        private final long segmentRows;
        private final long segmentSizeBytes;
        private final long compressedSegmentSizeBytes;
        private final long segmentBuildTimeMillis;

        public SegmentDescriptor(String segmentName, Path segmentTgzPath, Path segmentOutputDirectory, long segmentRows, long segmentSizeBytes, long compressedSegmentSizeBytes, long segmentBuildTimeMillis)
        {
            this.segmentName = requireNonNull(segmentName, "segmentName is null");
            this.segmentTgzPath = requireNonNull(segmentTgzPath, "segmentTgzPath is null");
            this.segmentOutputDirectory = requireNonNull(segmentOutputDirectory, "segmentOutputDirectory is null");
            this.segmentRows = segmentRows;
            this.segmentSizeBytes = segmentSizeBytes;
            this.compressedSegmentSizeBytes = compressedSegmentSizeBytes;
            this.segmentBuildTimeMillis = segmentBuildTimeMillis;
            checkState(segmentOutputDirectory.toFile().isDirectory(), "Segment output directory '%s' is not a directory", segmentOutputDirectory);
        }

        public String getSegmentName()
        {
            return segmentName;
        }

        public Path getSegmentTgzPath()
        {
            return segmentTgzPath;
        }

        public Path getSegmentOutputDirectory()
        {
            return segmentOutputDirectory;
        }

        public long getSegmentRows()
        {
            return segmentRows;
        }

        public long getSegmentSizeBytes()
        {
            return segmentSizeBytes;
        }

        public long getCompressedSegmentSizeBytes()
        {
            return compressedSegmentSizeBytes;
        }

        public long getSegmentBuildTimeMillis()
        {
            return segmentBuildTimeMillis;
        }
    }

    private static class SegmentMetadataDescriptor
    {
        private final String segmentName;
        private final URI segmentURI;
        private final File segmentMetadataFile;
        private final SegmentDescriptor segmentDescriptor;
        private final long segmentPushTimeMillis;

        public SegmentMetadataDescriptor(String segmentName, URI segmentURI, File segmentMetadataFile, SegmentDescriptor segmentDescriptor, long segmentPushTimeMillis)
        {
            this.segmentName = requireNonNull(segmentName, "segmentName is null");
            this.segmentURI = requireNonNull(segmentURI, "segmentURI is null");
            this.segmentMetadataFile = requireNonNull(segmentMetadataFile, "segmentMetadataFile is null");
            this.segmentDescriptor = requireNonNull(segmentDescriptor, "segmentDescriptor is null");
            this.segmentPushTimeMillis = segmentPushTimeMillis;
        }

        public String getSegmentName()
        {
            return segmentName;
        }

        public URI getSegmentURI()
        {
            return segmentURI;
        }

        public File getSegmentMetadataFile()
        {
            return segmentMetadataFile;
        }

        public SegmentDescriptor getSegmentDescriptor()
        {
            return segmentDescriptor;
        }

        public long getSegmentPushTimeMillis()
        {
            return segmentPushTimeMillis;
        }
    }

    private static class GenericRowWithSize
    {
        private GenericRow row;
        private long dataSize;

        public GenericRowWithSize(GenericRow row, long dataSize)
        {
            this.row = requireNonNull(row, "row is null");
            this.dataSize = dataSize;
        }

        public GenericRow getRow()
        {
            return row;
        }

        public long getDataSize()
        {
            return dataSize;
        }
    }

    private class GenericRowBuffer
    {
        private long currentSize;
        // Necessary because currentGenericRowFileReaderBuilder.getRecordFileReader() will throw an exception if the rowcount is 0
        private int currentRowCount;
        private int bufferedRowCount;
        private final SegmentUploader segmentUploader = new SegmentUploader();
        private GenericRowFileReaderBuilder currentGenericRowFileReaderBuilder = new GenericRowFileReaderBuilder();
        private GenericRowFileReaderBuilder bufferedGenericRowFileReaderBuilder;

        public synchronized void add(List<GenericRowWithSize> rowsWithSizes)
        {
            for (GenericRowWithSize rowWithSize : rowsWithSizes) {
                currentGenericRowFileReaderBuilder.add(rowWithSize.getRow());
                currentSize += rowWithSize.getDataSize();
                currentRowCount++;
                if (currentSize >= thresholdBytes) {
                    if (bufferedRowCount > 0) {
                        segmentUploader.upload(bufferedGenericRowFileReaderBuilder.getRecordFileReader().getRecordReader(), bufferedGenericRowFileReaderBuilder::cleanup);
                    }
                    bufferedGenericRowFileReaderBuilder = currentGenericRowFileReaderBuilder;
                    bufferedRowCount = currentRowCount;
                    currentGenericRowFileReaderBuilder = new GenericRowFileReaderBuilder();
                    currentSize = 0;
                    currentRowCount = 0;
                }
            }
        }

        public synchronized void finish()
        {
            try {
                if (bufferedRowCount == 0) {
                    if (currentRowCount > 0) {
                        GenericRowFileReader currentRecordReader = currentGenericRowFileReaderBuilder.getRecordFileReader();
                        segmentUploader.upload(currentRecordReader.getRecordReader(), currentGenericRowFileReaderBuilder::cleanup);
                    }
                }
                else if (currentRowCount == 0) {
                    segmentUploader.upload(bufferedGenericRowFileReaderBuilder.getRecordFileReader().getRecordReader(), bufferedGenericRowFileReaderBuilder::cleanup);
                }
                else {
                    // Need to add current records to buffered record builder so the records are sorted
                    RecordReader currentRowsReader = currentGenericRowFileReaderBuilder.getRecordFileReader().getRecordReader();
                    while (currentRowsReader.hasNext()) {
                        bufferedGenericRowFileReaderBuilder.add(currentRowsReader.next());
                    }
                    GenericRowFileReader bufferedRecordReader = bufferedGenericRowFileReaderBuilder.getRecordFileReader();
                    int totalRows = bufferedRecordReader.getNumRows();
                    int mid = totalRows / 2;
                    if (totalRows > 0) {
                        if (mid > 0) {
                            // These will be cleaned up when the task finishes
                            // Calling asynchronously can result in the file being deleted by one task while read by another
                            segmentUploader.upload(bufferedRecordReader.getRecordReader().getRecordReaderForRange(0, mid), () -> {});
                            segmentUploader.upload(bufferedRecordReader.getRecordReader().getRecordReaderForRange(mid, totalRows), () -> {});
                        }
                        else {
                            segmentUploader.upload(bufferedRecordReader.getRecordReader(), bufferedGenericRowFileReaderBuilder::cleanup);
                        }
                    }
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            finally {
                currentSize = 0;
                currentRowCount = 0;
                bufferedRowCount = 0;
                bufferedGenericRowFileReaderBuilder = null;
                currentGenericRowFileReaderBuilder = null;
            }
        }
    }

    private class SegmentUploader
    {
        private final String rawTableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
        private int sequenceId;
        private final String segmentPushType;
        private final String segmentPushFrequency;

        public SegmentUploader()
        {
            if (tableConfig.getIngestionConfig() != null &&
                    tableConfig.getIngestionConfig().getBatchIngestionConfig() != null) {
                segmentPushType = tableConfig.getIngestionConfig().getBatchIngestionConfig().getSegmentIngestionType();
                segmentPushFrequency = tableConfig.getIngestionConfig().getBatchIngestionConfig().getSegmentIngestionFrequency();
            }
            else {
                // Deprecated config
                segmentPushType = tableConfig.getValidationConfig().getSegmentPushType();
                segmentPushFrequency = tableConfig.getValidationConfig().getSegmentPushFrequency();
            }
        }

        public void upload(RecordReader recordReader, Runnable cleanup)
        {
            if (isCancelled.get()) {
                return;
            }
            int currentSequenceId = sequenceId++;

            segmentBuildFutures.add(segmentBuilderExecutor.submit(() -> {
                try {
                    SegmentDescriptor segmentDescriptor = createSegment(recordReader, currentSequenceId);
                    return pushSegmentAndExtractMetadata(segmentDescriptor);
                }
                finally {
                    cleanup.run();
                }
            }));
        }

        private SegmentDescriptor createSegment(RecordReader recordReader, int sequenceId)
        {
            try {
                throwIfCancelled();
                long beginNanos = ticker.read();
                String timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
                String rawTableName = TableNameBuilder.extractRawTableName(tableConfig.getTableName());
                SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, pinotSchema);
                segmentGeneratorConfig.setTableName(rawTableName);
                segmentGeneratorConfig.setOutDir(segmentTempLocation);
                segmentGeneratorConfig.setOnHeap(segmentBuildOnHeapEnabled);
                if (timeColumnName != null) {
                    DateTimeFormatSpec formatSpec = new DateTimeFormatSpec(pinotSchema.getDateTimeSpec(timeColumnName).getFormat());
                    segmentGeneratorConfig.setSegmentNameGenerator(new NormalizedDateSegmentNameGenerator(
                            rawTableName,
                            format("%s-%s", rawTableName, createdAtEpochMillis),
                            false,
                            segmentPushType,
                            segmentPushFrequency,
                            formatSpec,
                            null));
                }
                else {
                    checkState(tableConfig.isDimTable(), "Null time column only allowed for dimension tables");
                }
                segmentGeneratorConfig.setSequenceId(sequenceId);
                SegmentCreationDataSource dataSource = new RecordReaderSegmentCreationDataSource(recordReader);
                RecordTransformer recordTransformer =
                        new ErrorSuppressingRecordTransformerWrapper(
                                CompositeTransformer.getDefaultTransformer(tableConfig, pinotSchema));
                SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
                driver.init(segmentGeneratorConfig, dataSource, recordTransformer, null);
                driver.build();
                File segmentOutputDirectory = driver.getOutputDirectory();
                File tgzPath = new File(String.join(File.separator, taskTempLocation, segmentOutputDirectory.getName() + ".tar.gz"));
                TarGzCompressionUtils.createTarGzFile(segmentOutputDirectory, tgzPath);
                long buildTimeNanos = ticker.read() - beginNanos;
                long segmentSize = getTotalSize(segmentOutputDirectory);
                return new SegmentDescriptor(driver.getSegmentName(), tgzPath.toPath(), segmentOutputDirectory.toPath(), driver.getSegmentStats().getTotalDocCount(), segmentSize, tgzPath.length(), NANOSECONDS.toMillis(buildTimeNanos));
            }
            catch (Exception e) {
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }

        private String getSegmentPushFrequency()
        {
            if (tableConfig.getIngestionConfig() != null &&
                    tableConfig.getIngestionConfig().getBatchIngestionConfig() != null) {
                return tableConfig.getIngestionConfig().getBatchIngestionConfig().getSegmentIngestionFrequency();
            }
            return tableConfig.getValidationConfig().getSegmentPushFrequency();
        }

        private File generateSegmentMetadataFileFromDirectory(Path segmentDirectory)
        {
            throwIfCancelled();
            File segmentMetadataDir = new File(taskTempLocation, "segmentMetadataDir-" + randomUUID());
            Path segmentSubdirectory = segmentDirectory.resolve(SegmentVersion.v3.name());
            try {
                Files.createDirectories(segmentMetadataDir.toPath());
                Files.move(segmentSubdirectory.resolve(V1Constants.MetadataKeys.METADATA_FILE_NAME), segmentMetadataDir.toPath().resolve(V1Constants.MetadataKeys.METADATA_FILE_NAME));
                Files.move(segmentSubdirectory.resolve(V1Constants.SEGMENT_CREATION_META), segmentMetadataDir.toPath().resolve(V1Constants.SEGMENT_CREATION_META));

                File segmentMetadataTarFile = new File(taskTempLocation,
                        "segmentMetadata-" + randomUUID() + TarGzCompressionUtils.TAR_GZ_FILE_EXTENSION);
                TarGzCompressionUtils.createTarGzFile(segmentMetadataDir, segmentMetadataTarFile);

                return segmentMetadataTarFile;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            finally {
                try {
                    deleteRecursively(segmentMetadataDir.toPath(), ALLOW_INSECURE);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        private SegmentMetadataDescriptor pushSegmentAndExtractMetadata(SegmentDescriptor segmentDescriptor)
        {
            throwIfCancelled();
            URI segmentURI = segmentDeepstoreURI.resolve(segmentDeepstoreURI.getPath() + File.separator + rawTableName + File.separator + segmentDescriptor.getSegmentName()).normalize();
            try {
                long beginNanos = ticker.read();
                RetryPolicies.exponentialBackoffRetryPolicy(3, 1000, 5).attempt(
                        () ->
                                metadataPushExecutor.submit(() -> {
                                    try {
                                        pinotFS.copyFromLocalFile(segmentDescriptor.segmentTgzPath.toFile(), segmentURI);
                                        return true;
                                    }
                                    catch (Exception e) {
                                        throwIfUnchecked(e);
                                        throw new RuntimeException(e);
                                    }
                                }).get(segmentUploadTimeoutMillis, MILLISECONDS));
                long pushTimeMillis = ticker.read() - beginNanos;
                return new SegmentMetadataDescriptor(segmentDescriptor.getSegmentName(), segmentURI, generateSegmentMetadataFileFromDirectory(segmentDescriptor.getSegmentOutputDirectory()), segmentDescriptor, NANOSECONDS.toMillis(pushTimeMillis));
            }
            catch (AttemptsExceededException | RetriableOperationException e) {
                throw new RuntimeException(e);
            }
            finally {
                try {
                    Files.deleteIfExists(segmentDescriptor.getSegmentTgzPath());
                    deleteRecursively(segmentDescriptor.getSegmentOutputDirectory(), ALLOW_INSECURE);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    private long getTotalSize(File path)
    {
        if (path.isFile()) {
            return path.length();
        }
        long totalSize = 0;
        for (File file : path.listFiles()) {
            if (file.isFile()) {
                totalSize += file.length();
            }
            else if (file.isDirectory()) {
                totalSize += getTotalSize(file);
            }
            else {
                throw new UnsupportedOperationException(format("Unsupported file type for file '%s'", file.getPath()));
            }
        }
        return totalSize;
    }

    private class GenericRowFileReaderBuilder
    {
        private final List<FieldSpec> fieldSpecs;
        private final int sortedColumnCount;
        private final GenericRowFileManager fileManager;
        private final Path outputDirectory;

        public GenericRowFileReaderBuilder()
        {
            Pair<List<FieldSpec>, Integer> fieldSpecsAndSortColumns = getFieldSpecs(pinotSchema, CONCAT, getSortedColumns());
            this.fieldSpecs = fieldSpecsAndSortColumns.getLeft();
            this.sortedColumnCount = fieldSpecsAndSortColumns.getRight();
            try {
                this.outputDirectory = Paths.get(segmentBuildLocation).resolve(randomUUID().toString());
                Files.createDirectories(outputDirectory);
                this.fileManager = new GenericRowFileManager(outputDirectory.toFile(), fieldSpecs, tableConfig.getIndexingConfig().isNullHandlingEnabled(), sortedColumnCount);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private List<String> getSortedColumns()
        {
            if (tableConfig.getIndexingConfig() == null || tableConfig.getIndexingConfig().getSortedColumn() == null) {
                return ImmutableList.of();
            }
            return tableConfig.getIndexingConfig().getSortedColumn();
        }

        public void add(GenericRow row)
        {
            try {
                fileManager.getFileWriter().write(row);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public GenericRowFileReader getRecordFileReader()
        {
            try {
                fileManager.closeFileWriter();
                return fileManager.getFileReader();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public void cleanup()
        {
            try {
                deleteRecursively(outputDirectory, ALLOW_INSECURE);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }
}
