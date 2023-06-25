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
package io.trino.filesystem.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.filesystem.gcs.GcsUtils.getBlob;
import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static java.util.Collections.reverseOrder;
import static java.util.Objects.requireNonNull;

public class GcsFileSystem
        implements TrinoFileSystem
{
    private final ListeningExecutorService executorService;
    private final Storage storage;
    private final int readBlockSize;
    private final long writeBlockSizeBytes;
    private final int pageSize;
    private final int batchSize;

    public GcsFileSystem(ListeningExecutorService executorService, Storage storage, int readBlockSize, long writeBlockSizeBytes, int pageSize, int batchSize)
    {
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.storage = requireNonNull(storage, "storage is null");
        // TODO: checkArguments
        this.readBlockSize = readBlockSize;
        this.writeBlockSizeBytes = writeBlockSizeBytes;
        this.pageSize = pageSize;
        this.batchSize = batchSize;
    }

    @Override
    public TrinoInputFile newInputFile(Location location)
    {
        GcsLocation gcsLocation = new GcsLocation(location);
        Blob blob = getBlob(storage, gcsLocation, OptionalLong.empty());
        return new GcsInputFile(gcsLocation, blob, readBlockSize, OptionalLong.empty());
    }

    @Override
    public TrinoInputFile newInputFile(Location location, long length)
    {
        GcsLocation gcsLocation = new GcsLocation(location);
        Blob blob = getBlob(storage, gcsLocation, OptionalLong.empty());
        return new GcsInputFile(gcsLocation, blob, readBlockSize, OptionalLong.of(length));
    }

    @Override
    public TrinoOutputFile newOutputFile(Location location)
    {
        GcsLocation gcsLocation = new GcsLocation(location);
        Blob blob = getBlob(storage, gcsLocation, OptionalLong.empty());
        return new GcsOutputFile(gcsLocation, blob, writeBlockSizeBytes);
    }

    @Override
    public void deleteFile(Location location)
            throws IOException
    {
        GcsLocation gcsLocation = new GcsLocation(location);
        Blob blob = getBlob(storage, gcsLocation, OptionalLong.empty());
        blob.delete();
    }

    @Override
    public void deleteFiles(Collection<Location> locations)
            throws IOException
    {
        ImmutableList.Builder<ListenableFuture<?>> batchFuturesBuilder = ImmutableList.builder();
        try {
            StorageBatch batch = storage.batch();
            int currentBatchSize = 0;
            for (Location location : locations) {
                if (currentBatchSize == batchSize) {
                    batchFuturesBuilder.add(executorService.submit(batch::submit));
                    batch = storage.batch();
                    currentBatchSize = 0;
                }
                GcsLocation gcsLocation = new GcsLocation(location);
                Blob blob = getBlob(storage, gcsLocation, OptionalLong.empty());
                checkState(!blob.isDirectory(), "Location %s refers to a directory not a file", gcsLocation);
                batch.delete(blob.getBlobId());
                currentBatchSize++;
            }
            if (currentBatchSize > 0) {
                batchFuturesBuilder.add(executorService.submit(batch::submit));
            }
            Futures.allAsList(batchFuturesBuilder.build()).get();
        }
        catch (RuntimeException | InterruptedException | ExecutionException e) {
            RuntimeException runtimeException = e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
            throw handleGcsException(runtimeException, "delete files", locations);
        }
    }

    @Override
    public void deleteDirectory(Location location)
            throws IOException
    {
        GcsLocation gcsLocation = new GcsLocation(location);
        try {
            // Recommended batch size is 100: https://cloud.google.com/storage/docs/batch
            StorageBatch batch = storage.batch();
            List<Blob> directories = new ArrayList<>();
            ImmutableList.Builder<ListenableFuture<?>> batchFuturesBuilder = ImmutableList.builder();
            int currentBatchSize = 0;
            for (Blob blob : getPage(gcsLocation, true, OptionalInt.empty()).iterateAll()) {
                if (currentBatchSize == batchSize) {
                    batchFuturesBuilder.add(executorService.submit(batch::submit));
                    batch = storage.batch();
                    currentBatchSize = 0;
                }
                if (blob.isDirectory()) {
                    directories.add(blob);
                }
                batch.delete(blob.getBlobId());
                currentBatchSize++;
            }
            if (currentBatchSize == batchSize) {
                batchFuturesBuilder.add(executorService.submit(batch::submit));
                batch = storage.batch();
            }
            Collections.sort(directories, reverseOrder());
            for (Blob blob : directories) {
                if (currentBatchSize == batchSize) {
                    batchFuturesBuilder.add(executorService.submit(batch::submit));
                    batch = storage.batch();
                }
                batch.delete(blob.getBlobId());
                currentBatchSize++;
            }
            if (currentBatchSize > 0) {
                batchFuturesBuilder.add(executorService.submit(batch::submit));
            }
            Futures.allAsList(batchFuturesBuilder.build()).get();
        }
        catch (RuntimeException | InterruptedException | ExecutionException e) {
            RuntimeException runtimeException = e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
            throw handleGcsException(runtimeException, "delete directory", gcsLocation);
        }
    }

    @Override
    public void renameFile(Location source, Location target)
            throws IOException
    {
        GcsLocation sourceLocation = new GcsLocation(source);
        GcsLocation targetLocation = new GcsLocation(target);
        try {
            Blob sourceBlob = getBlob(storage, sourceLocation, OptionalLong.empty());
            Blob targetBlob = getBlob(storage, targetLocation, OptionalLong.empty());
            checkState(targetBlob == null, "target location already exists", targetLocation);
            storage.copy(
                    Storage.CopyRequest.newBuilder()
                            .setSource(sourceBlob.getBlobId())
                            .setTarget(BlobId.of(targetLocation.bucket(), targetLocation.key()), Storage.BlobTargetOption.doesNotExist()).build());
            Blob copiedObject = storage.get(BlobId.of(targetLocation.bucket(), targetLocation.key()));
            // Delete the original blob now that we've copied to where we want it, finishing the "move"
            // operation
            storage.get(sourceBlob.getBlobId()).delete();
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "rename file", sourceLocation);
        }
    }

    @Override
    public FileIterator listFiles(Location location)
            throws IOException
    {
        GcsLocation gcsLocation = new GcsLocation(location);
        try {
            return new GcsFileIterator(gcsLocation, getPage(gcsLocation, false, OptionalInt.empty()));
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "list files", gcsLocation);
        }
    }

    private Page<Blob> getPage(GcsLocation location, boolean heirarchical, OptionalInt customPageSize)
    {
        ImmutableList.Builder<Storage.BlobListOption> optionsBuilder = ImmutableList.builder();

        if (heirarchical) {
            optionsBuilder.add(Storage.BlobListOption.currentDirectory());
        }
        optionsBuilder.add(Storage.BlobListOption.prefix(location.key()));
        optionsBuilder.add(Storage.BlobListOption.pageSize(customPageSize.orElse(this.pageSize)));
        return storage.list(location.bucket(), optionsBuilder.build().toArray(new Storage.BlobListOption[0]));
    }

    @Override
    public Optional<Boolean> directoryExists(Location location)
            throws IOException
    {
        GcsLocation gcsLocation = new GcsLocation(location);
        try {
            Page<Blob> blobs = storage.list(gcsLocation.bucket(), Storage.BlobListOption.prefix(gcsLocation.key()), Storage.BlobListOption.currentDirectory(), Storage.BlobListOption.pageSize(1));
            Iterator<Blob> blobIterator = blobs.getValues().iterator();
            if (blobIterator.hasNext()) {
                Blob blob = blobIterator.next();
                return Optional.of(blob.isDirectory());
            }
            return Optional.of(false);
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "directory exists", gcsLocation);
        }
    }
}
