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

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.common.primitives.Ints;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.memory.context.LocalMemoryContext;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class GcsOutputStream
        extends OutputStream
{
    private static final int BUFFER_SIZE = 8192;

    private final GcsLocation location;
    private final Blob blob;
    private final long writeBlockSizeBytes;
    private final LocalMemoryContext memoryContext;
    private final WriteChannel writeChannel;
    private long writtenBytes;
    private boolean closed;

    public GcsOutputStream(GcsLocation location, Blob blob, AggregatedMemoryContext memoryContext, long writeBlockSizeBytes)
    {
        this.location = requireNonNull(location, "location is null");
        this.blob = requireNonNull(blob, "blob is null");
        checkArgument(writeBlockSizeBytes >= 0, "writeBlockSizeBytes is negative");
        // Default size is 15Mib, see com.google.cloud.BaseWriteChannel
        this.writeBlockSizeBytes = writeBlockSizeBytes;
        requireNonNull(memoryContext, "memoryContext is null");
        this.memoryContext = memoryContext.newLocalMemoryContext(GcsOutputStream.class.getSimpleName());
        this.writeChannel = blob.writer();
        this.writeChannel.setChunkSize(Ints.saturatedCast(writeBlockSizeBytes));
    }

    @Override
    public void write(int b)
            throws IOException
    {
        ensureOpen();
        try {
            // WriteChannel is buffered internally: see com.google.cloud.BaseWriteChannel
            writeChannel.write(ByteBuffer.wrap(new byte[]{(byte) (b & 0xff)}));
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "writing file", location);
        }
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        ByteBuffer wrappedBuffer = ByteBuffer.wrap(buffer, offset, length);
        int bytesWritten = 0;
        try {
            bytesWritten = writeChannel.write(wrappedBuffer);
            checkState(bytesWritten == length, "Unexpected bytes written length: %s should be %s".formatted(bytesWritten, length));
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "writing file", location);
        }
        recordBytesWritten(bytesWritten);
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Output stream closed: " + location);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            closed = true;
            try {
                // The buffer is flushed internally. See com.google.cloud.BaseWriteChannel
                writeChannel.close();
            }
            catch (IOException e) {
                // Azure close sometimes rethrows IOExceptions from worker threads, so the
                // stack traces are disconnected from this call. Wrapping here solves that problem.
                throw new IOException("Error closing file: " + location, e);
            }
            finally {
                memoryContext.close();
            }
        }
    }

    private void recordBytesWritten(int size)
    {
        if (writtenBytes < writeBlockSizeBytes) {
            // assume that there is only one pending block buffer, and that it grows as written bytes grow
            memoryContext.setBytes(BUFFER_SIZE + min(writtenBytes + size, writeBlockSizeBytes));
        }
        writtenBytes += size;
    }
}
