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

import com.google.cloud.storage.Blob;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static java.util.Objects.requireNonNull;

public class GcsOutputFile
        implements TrinoOutputFile
{
    private final GcsLocation location;
    private final Blob blob;
    private final long writeBlockSizeBytes;

    public GcsOutputFile(GcsLocation location, Blob blob, long writeBlockSizeBytes)
    {
        this.location = requireNonNull(location, "location is null");
        this.blob = requireNonNull(blob, "blob is null");
        checkArgument(writeBlockSizeBytes >= 0, "writeBlockSizeBytes is negative");
        this.writeBlockSizeBytes = writeBlockSizeBytes;
    }

    @Override
    public OutputStream createOrOverwrite()
            throws IOException
    {
        return TrinoOutputFile.super.createOrOverwrite();
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return createOutputStream(memoryContext, false);
    }

    @Override
    public OutputStream createOrOverwrite(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        return createOutputStream(memoryContext, true);
    }

    private OutputStream createOutputStream(AggregatedMemoryContext memoryContext, boolean overwrite)
            throws IOException
    {
        try {
            if (!overwrite) {
                checkState(!blob.exists(), "File '%s' already exists".formatted(location));
            }
            return new GcsOutputStream(location, blob, memoryContext, writeBlockSizeBytes);
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "writing file", location);
        }
    }

    @Override
    public Location location()
    {
        return location.location();
    }

    private boolean exists()
    {
        return blob.exists();
    }
}
