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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.encoders.Encoder;
import io.trino.plugin.pinot.encoders.ErrorSuppressingRecordTransformerWrapper;
import io.trino.plugin.pinot.encoders.GenericRowReadersBuilder;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSink;
import org.apache.pinot.common.exception.HttpErrorStatusException;
import org.apache.pinot.common.utils.FileUploadDownloadClient;
import org.apache.pinot.common.utils.SimpleHttpResponse;
import org.apache.pinot.common.utils.TarGzCompressionUtils;
import org.apache.pinot.segment.local.recordtransformer.ComplexTypeTransformer;
import org.apache.pinot.segment.local.recordtransformer.CompositeTransformer;
import org.apache.pinot.segment.local.recordtransformer.RecordTransformer;
import org.apache.pinot.segment.local.segment.creator.RecordReaderSegmentCreationDataSource;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentCreationDataSource;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.creator.name.NormalizedDateSegmentNameGenerator;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.ingestion.batch.spec.Constants;
import org.apache.pinot.spi.utils.retry.RetryPolicies;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.pinot.encoders.EncoderFactory.createEncoder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toMap;

public class PinotDiskBasedPageSink
        implements ConnectorPageSink
{
    private static final FileUploadDownloadClient FILE_UPLOAD_DOWNLOAD_CLIENT = new FileUploadDownloadClient();
    private static final String DIMENSION_TABLE_SEGMENT_KEY = "dim";
    private final PinotInsertTableHandle pinotInsertTableHandle;
    private final List<URI> pinotControllerUrls;
    private final String taskTempLocation;
    private final String segmentTempLocation;
    private final GenericRowReadersBuilder genericRowReadersBuilder;
    private final TableConfig tableConfig;
    private final Schema pinotSchema;
    private final String timeColumnName;
    private final PinotClient pinotClient;

    @SuppressWarnings("unchecked")
    public PinotDiskBasedPageSink(PinotInsertTableHandle pinotInsertTableHandle, PinotClient pinotClient, String segmentCreationBaseDirectory, long segmentSizeBytes, List<URI> pinotControllerUrls)
    {
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
        this.pinotInsertTableHandle = requireNonNull(pinotInsertTableHandle, "pinotInsertTableHandle is null");
        this.pinotControllerUrls = requireNonNull(pinotControllerUrls, "pinotControllerUrls is null");
        this.taskTempLocation = String.join(File.separator, segmentCreationBaseDirectory, UUID.randomUUID().toString());
        this.segmentTempLocation = String.join(File.separator, taskTempLocation, pinotInsertTableHandle.getPinotTableName(), "segments");
        this.tableConfig = pinotClient.getTableConfig(pinotInsertTableHandle.getPinotTableName()).getOfflineConfig().get();
        this.pinotSchema = pinotClient.getTableSchema(pinotInsertTableHandle.getPinotTableName());
        this.timeColumnName = tableConfig.getValidationConfig().getTimeColumnName();
        Function<GenericRow, String> rowToSegmentFunction;
        if (pinotInsertTableHandle.getDateTimeField().isPresent()) {
            PinotDateTimeField dateTimeFieldTransformer = pinotInsertTableHandle.getDateTimeField().get();
            LongUnaryOperator transformFunction = dateTimeFieldTransformer.getToMillisTransform();
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            rowToSegmentFunction = row -> dateFormat.format(Instant.ofEpochMilli(transformFunction.applyAsLong((long) row.getValue(timeColumnName))).atZone(ZoneId.of("UTC")).toLocalDate());
        }
        else {
            rowToSegmentFunction = row -> DIMENSION_TABLE_SEGMENT_KEY;
        }
        Map<String, FieldSpec> fieldSpecMap = pinotSchema.getAllFieldSpecs().stream()
                .collect(toMap(fieldSpec -> fieldSpec.getName(), fieldSpec -> fieldSpec));
        ImmutableMap.Builder<Integer, Encoder> encoderMapBuilder = ImmutableMap.builder();
        for (int channel = 0; channel < pinotInsertTableHandle.getColumnHandles().size(); channel++) {
            PinotColumnHandle columnHandle = pinotInsertTableHandle.getColumnHandles().get(channel);
            // TODO: add virtual
            encoderMapBuilder.put(channel, createEncoder(fieldSpecMap.get(columnHandle.getColumnName()), columnHandle.getDataType()));
        }
        this.genericRowReadersBuilder = new GenericRowReadersBuilder(pinotInsertTableHandle.getColumnHandles(), fieldSpecMap, encoderMapBuilder.buildOrThrow(), tableConfig, pinotSchema, rowToSegmentFunction, Paths.get(taskTempLocation), segmentSizeBytes);
    }

    private static class SegmentNameAndPath
    {
        private final String segmentName;
        private final Path segmentPath;

        public SegmentNameAndPath(String segmentName, Path segmentPath)
        {
            this.segmentName = requireNonNull(segmentName, "segmentName is null");
            this.segmentPath = requireNonNull(segmentPath, "segmentPath is null");
        }

        public String getSegmentName()
        {
            return segmentName;
        }

        public Path getSegmentPath()
        {
            return segmentPath;
        }
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        genericRowReadersBuilder.append(page);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Map<String, List<RecordReader>> segments = genericRowReadersBuilder.build();
        long createdAtEpochMillis = Instant.now().toEpochMilli();

        if (!segments.isEmpty()) {
            try {
                for (Iterator<Map.Entry<String, List<RecordReader>>> iterator = segments.entrySet().iterator(); iterator.hasNext(); ) {
                    Map.Entry<String, List<RecordReader>> entry = iterator.next();
                    String segmentDate = entry.getKey();
                    List<RecordReader> recordReaders = entry.getValue();
                    iterator.remove();
                    ImmutableList.Builder<SegmentNameAndPath> segmentNameAndPathBuilder = ImmutableList.builder();
                    for (int i = 0; i < recordReaders.size(); i++) {
                        RecordReader recordReader = recordReaders.get(i);
                        SegmentNameAndPath segmentNameAndPath = createSegment(recordReader, i, createdAtEpochMillis);
                        segmentNameAndPathBuilder.add(segmentNameAndPath);
                    }
                    List<String> segmentsToReplace = getSegmentsToReplace(segmentDate);
                    List<SegmentNameAndPath> segmentNameAndPaths = segmentNameAndPathBuilder.build();
                    List<String> newSegments = segmentNameAndPaths.stream()
                            .map(SegmentNameAndPath::getSegmentName)
                            .collect(toImmutableList());
                    PinotClient.PinotStartReplaceSegmentsResponse response = pinotClient.startReplaceSegments(pinotInsertTableHandle.getPinotTableName(), segmentsToReplace, newSegments);
                    segmentNameAndPaths
                            .forEach(segmentNameAndPath -> publishOfflineSegment(segmentNameAndPath.getSegmentPath()));
                    endReplaceSegments(response, newSegments);
                    pinotClient.deleteSegments(pinotInsertTableHandle.getPinotTableName(), segmentsToReplace);
                }
            }
            finally {
                cleanup();
            }
        }
        return completedFuture(ImmutableList.of(Slices.EMPTY_SLICE));
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
                .filter(segment -> segmentDate.equals(DIMENSION_TABLE_SEGMENT_KEY) || segment.contains(segmentDate))
                .collect(toImmutableList());
    }

    @Override
    public void abort()
    {
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

    private synchronized SegmentNameAndPath createSegment(RecordReader recordReader, int sequenceId, long createdAtEpochMillis)
    {
        try {
            Files.createDirectories(Paths.get(taskTempLocation));
            SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, pinotSchema);
            segmentGeneratorConfig.setTableName(pinotInsertTableHandle.getPinotTableName());
            segmentGeneratorConfig.setOutDir(segmentTempLocation);
            if (timeColumnName != null) {
                DateTimeFormatSpec formatSpec = new DateTimeFormatSpec(pinotSchema.getDateTimeSpec(timeColumnName).getFormat());
                segmentGeneratorConfig.setSegmentNameGenerator(new NormalizedDateSegmentNameGenerator(
                        pinotInsertTableHandle.getPinotTableName(),
                        format("%s-%s", pinotInsertTableHandle.getPinotTableName(), createdAtEpochMillis),
                        false,
                        tableConfig.getValidationConfig().getSegmentPushType(),
                        tableConfig.getValidationConfig().getSegmentPushFrequency(),
                        formatSpec,
                        null));
            }
            else {
                segmentGeneratorConfig.setSegmentNameGenerator(new NormalizedDateSegmentNameGenerator(
                        pinotInsertTableHandle.getPinotTableName(),
                        format("%s-%s", pinotInsertTableHandle.getPinotTableName(), createdAtEpochMillis),
                        false,
                        "REFRESH",
                        null,
                        null,
                        null));
            }
            segmentGeneratorConfig.setSequenceId(sequenceId);
            SegmentCreationDataSource dataSource = new RecordReaderSegmentCreationDataSource(recordReader);
            RecordTransformer recordTransformer =
                    new ErrorSuppressingRecordTransformerWrapper(
                            CompositeTransformer.getDefaultTransformer(tableConfig, pinotSchema));
            SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
            driver.init(segmentGeneratorConfig, dataSource, recordTransformer, ComplexTypeTransformer.getComplexTypeTransformer(tableConfig));
            driver.build();
            File segmentOutputDirectory = driver.getOutputDirectory();
            File tgzPath = new File(String.join(File.separator, taskTempLocation, segmentOutputDirectory.getName() + ".tar.gz"));
            TarGzCompressionUtils.createTarGzFile(segmentOutputDirectory, tgzPath);
            return new SegmentNameAndPath(driver.getSegmentName(), Paths.get(tgzPath.getAbsolutePath()));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void publishOfflineSegment(Path segmentPath)
    {
        try {
            String fileName = segmentPath.toFile().getName();
            checkArgument(fileName.endsWith(Constants.TAR_GZ_FILE_EXT));
            String segmentName = fileName.substring(0, fileName.length() - Constants.TAR_GZ_FILE_EXT.length());
            RetryPolicies.exponentialBackoffRetryPolicy(3, 1000, 5).attempt(() -> {
                try (InputStream inputStream = Files.newInputStream(segmentPath)) {
                    SimpleHttpResponse response = FILE_UPLOAD_DOWNLOAD_CLIENT.uploadSegment(
                            getControllerUrl().resolve("/v2/segments"),
                            segmentName,
                            inputStream,
                            pinotInsertTableHandle.getPinotTableName());
                    // TODO: {"status":"Successfully uploaded segment: myTable2_2020-09-09_2020-09-09 of table: myTable2"}
                    checkState(response.getStatusCode() == 200, "Unexpected response: '%s'", response.getResponse());
                    return true;
                }
                catch (HttpErrorStatusException e) {
                    int statusCode = e.getStatusCode();
                    if (statusCode >= 500) {
                        // Temporary exception
                        //LOGGER.warn("Caught temporary exception while pushing table: {} segment: {} to {}, will retry", TOPIC_AND_TABLE, segmentName, controllerHostAndPort, e);
                        return false;
                    }
                    else {
                        // Permanent exception
                        //LOGGER.error("Caught permanent exception while pushing table: {} segment: {} to {}, won't retry", TOPIC_AND_TABLE,                                 segmentName, controllerHostAndPort, e);
                        throw e;
                    }
                }
            });
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            try {
                Files.deleteIfExists(segmentPath);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private URI getControllerUrl()
    {
        return pinotControllerUrls.get(ThreadLocalRandom.current().nextInt(pinotControllerUrls.size()));
    }
}
