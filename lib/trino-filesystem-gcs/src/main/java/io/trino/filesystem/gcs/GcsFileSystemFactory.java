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

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;

import java.io.IOException;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;

public class GcsFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final int readBlockSize;
    private final long writeBlockSizeBytes;
    private final int pageSize;
    private final int batchSize;
    private final ListeningExecutorService executorService;
    private final GcsStorageFactory storageFactory;

    @Inject
    public GcsFileSystemFactory(GcsFileSystemConfig config, ListeningExecutorService executorService, GcsStorageFactory storageFactory)
            throws IOException
    {
        this.readBlockSize = toIntExact(config.getReadBlockSize().toBytes());
        this.writeBlockSizeBytes = config.getWriteBlockSize().toBytes();
        this.pageSize = config.getPageSize();
        this.batchSize = config.getBatchSize();
        this.executorService = executorService;
        this.storageFactory = storageFactory;
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        try {
            return new GcsFileSystem(executorService, storageFactory.create(identity), readBlockSize, writeBlockSizeBytes, pageSize, batchSize);
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
