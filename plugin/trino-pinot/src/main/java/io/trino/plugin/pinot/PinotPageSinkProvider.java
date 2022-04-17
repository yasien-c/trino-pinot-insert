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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.ExecutorServiceAdapter;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.deepstore.DeepStore;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PinotPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final String segmentCreationBaseDirectory;
    private final List<URI> pinotControllerUrls;
    private final long segmentSizeBytes;
    private final long segmentUploadTimeoutMillis;
    private final long finishInsertTimeoutMillis;
    private final PinotClient pinotClient;
    private final Optional<DeepStore> pinotDeepStore;
    private final ExecutorService segmentBuilderExecutor;
    private final ListeningExecutorService metadataPushExecutor;
    private final boolean segmentBuildOnHeapEnabled;
    private final int perQuerySegmentBuilderParallelism;

    @Inject
    public PinotPageSinkProvider(PinotConfig config, PinotClient pinotClient, Optional<DeepStore> pinotDeepStore)
    {
        requireNonNull(config, "config is null");
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
        this.segmentCreationBaseDirectory = config.getSegmentCreationBaseDirectory();
        this.pinotControllerUrls = ImmutableList.copyOf(config.getControllerUrls());
        this.segmentSizeBytes = config.getSegmentCreationDataSize().toBytes();
        this.segmentUploadTimeoutMillis = config.getSegmentUploadTimeout().toMillis();
        this.finishInsertTimeoutMillis = config.getFinishInsertTimeout().toMillis();
        this.pinotDeepStore = requireNonNull(pinotDeepStore, "pinotDeepStore is null");
        this.metadataPushExecutor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("pinot-metadata-push-%s")));
        this.segmentBuilderExecutor = new ThreadPoolExecutor(config.getSegmentBuilderParallelism(), config.getSegmentBuilderParallelism(), 0, MILLISECONDS, new TimedBlockingQueue<>(config.getSegmentBuilderQueueSize(), config.getSegmentBuilderQueueTimeout().toMillis()), daemonThreadsNamed("pinot-segment-builder-%s"));
        this.segmentBuildOnHeapEnabled = config.isSegmentBuildOnHeapEnabled();
        this.perQuerySegmentBuilderParallelism = config.getPerQuerySegmentBuilderParallelism();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        PinotInsertTableHandle pinotInsertTableHandle = (PinotInsertTableHandle) insertTableHandle;
        if (pinotDeepStore.isEmpty()) {
            return new PinotDiskBasedPageSink(pinotInsertTableHandle, pinotClient, segmentCreationBaseDirectory, segmentSizeBytes, pinotControllerUrls);
        }
        PinotFSFactory.init(pinotDeepStore.get().getPinotConfiguration());
        PinotFS pinotFS = PinotFSFactory.create(pinotDeepStore.get().getDeepStoreUri().getScheme());
        return new PinotAsyncDiskBasedMetadataPushPageSink(listeningDecorator(ExecutorServiceAdapter.from(new BoundedExecutor(segmentBuilderExecutor, perQuerySegmentBuilderParallelism))), metadataPushExecutor, pinotDeepStore.get().getDeepStoreUri(), pinotFS, pinotInsertTableHandle, pinotClient, segmentCreationBaseDirectory, segmentSizeBytes, segmentUploadTimeoutMillis, finishInsertTimeoutMillis, segmentBuildOnHeapEnabled, pinotControllerUrls);
    }

    private static class TimedBlockingQueue<T>
            extends ArrayBlockingQueue<T>
    {
        private final long timeoutMillis;

        public TimedBlockingQueue(int maxSize, long timeoutMillis)
        {
            super(maxSize);
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public boolean offer(T element)
        {
            try {
                if (!offer(element, timeoutMillis, MILLISECONDS)) {
                    throw new UncheckedTimeoutException();
                }
                return true;
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }
}
