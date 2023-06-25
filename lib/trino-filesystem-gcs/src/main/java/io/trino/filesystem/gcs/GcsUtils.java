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

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.trino.filesystem.Location;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class GcsUtils
{
    private GcsUtils() {}

    //TODO: add more exceptions, see AzureUtils
    public static IOException handleGcsException(RuntimeException exception, String action, GcsLocation location)
            throws IOException
    {
        throw new IOException("Error %s file: %s".formatted(action, location), exception);
    }

    public static IOException handleGcsException(RuntimeException exception, String action, Collection<Location> locations)
            throws IOException
    {
        throw new IOException("Error %s file: %s".formatted(action, locations), exception);
    }

    public static ReadChannel getReadChannel(Blob blob, GcsLocation location, long position, int readBlockSize, Blob.BlobSourceOption... blobSourceOptions)
            throws IOException
    {
        long fileSize = requireNonNull(blob.getSize(), "blob size is null");
        if (position >= fileSize) {
            throw new IOException("Cannot read at %s. File size is %s: %s".formatted(position, fileSize, location));
        }
        ReadChannel readChannel = blob.reader(blobSourceOptions);

        readChannel.setChunkSize(readBlockSize);
        readChannel.seek(position);
        return readChannel;
    }

    @Nullable
    public static Blob getBlob(Storage storage, GcsLocation location, OptionalLong generation, Storage.BlobGetOption... blobGetOptions)
    {
        if (generation.isEmpty()) {
            return storage.get(BlobId.of(location.bucket(), location.key()), blobGetOptions);
        }
        // TODO: blobGetOption.generationMatch()
        return storage.get(BlobId.of(location.bucket(), location.key(), generation.getAsLong()), blobGetOptions);
    }
}
