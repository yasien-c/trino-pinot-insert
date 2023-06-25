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
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static java.util.Objects.requireNonNull;

public class GcsInputFile
        implements TrinoInputFile
{
    private final GcsLocation location;
    private final Blob blob;
    private final int readBlockSize;
    private OptionalLong length;
    private Optional<Instant> lastModified = Optional.empty();

    public GcsInputFile(GcsLocation location, Blob blob, int readBockSize, OptionalLong length)
    {
        this.location = requireNonNull(location, "location is null");
        this.blob = requireNonNull(blob, "blob is null");
        this.readBlockSize = readBockSize;
        this.length = requireNonNull(length, "length is null");
    }

    @Override
    public TrinoInput newInput()
    {
        return new GcsInput(location, blob, readBlockSize, OptionalLong.empty());
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        return new GcsInputStream(location, blob, readBlockSize);
    }

    @Override
    public long length()
            throws IOException
    {
        if (length.isEmpty()) {
            loadProperties();
        }
        return length.orElseThrow();
    }

    private void loadProperties()
            throws IOException
    {
        try {
            length = OptionalLong.of(blob.getSize());
            lastModified = Optional.of(Instant.from(blob.getUpdateTimeOffsetDateTime()));
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "fetching properties for file", location);
        }
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        if (lastModified.isEmpty()) {
            loadProperties();
        }
        return lastModified.orElseThrow();
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return blob.exists();
    }

    @Override
    public Location location()
    {
        return location.location();
    }
}
