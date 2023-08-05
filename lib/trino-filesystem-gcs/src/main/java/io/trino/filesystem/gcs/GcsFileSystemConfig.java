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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import static com.google.common.base.Preconditions.checkState;

public class GcsFileSystemConfig
{
    private DataSize readBlockSize = DataSize.of(4, DataSize.Unit.MEGABYTE);
    private DataSize writeBlockSize = DataSize.of(4, DataSize.Unit.MEGABYTE);
    private int pageSize = 100;
    private int batchSize = 100;

    private String projectId;

    private boolean useGcsAccessToken;
    private String jsonKey;
    private String jsonKeyFilePath;

    @NotNull
    public DataSize getReadBlockSize()
    {
        return readBlockSize;
    }

    @Config("gcs.read-block-size")
    public GcsFileSystemConfig setReadBlockSize(DataSize readBlockSize)
    {
        this.readBlockSize = readBlockSize;
        return this;
    }

    @NotNull
    public DataSize getWriteBlockSize()
    {
        return writeBlockSize;
    }

    @Config("gcs.write-block-size")
    public GcsFileSystemConfig setWriteBlockSize(DataSize writeBlockSize)
    {
        this.writeBlockSize = writeBlockSize;
        return this;
    }

    @Min(1)
    public int getPageSize()
    {
        return pageSize;
    }

    @Config("gcs.page-size")
    public GcsFileSystemConfig setPageSize(int pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    @Min(1)
    public int getBatchSize()
    {
        return batchSize;
    }

    @Config("gcs.batch-size")
    public GcsFileSystemConfig setBatchSize(int batchSize)
    {
        this.batchSize = batchSize;
        return this;
    }

    @Nullable
    public String getProjectId()
    {
        return projectId;
    }

    @Config("gcs.projectId")
    public GcsFileSystemConfig setProjectId(String projectId)
    {
        this.projectId = projectId;
        return this;
    }

    public boolean isUseGcsAccessToken()
    {
        return useGcsAccessToken;
    }

    @Config("gcs.use-access-token")
    public GcsFileSystemConfig setUseGcsAccessToken(boolean useGcsAccessToken)
    {
        this.useGcsAccessToken = useGcsAccessToken;
        return this;
    }

    @Nullable
    public String getJsonKey()
    {
        return jsonKey;
    }

    @Config("gcs.json-key")
    @ConfigSecuritySensitive
    public GcsFileSystemConfig setJsonKey(String jsonKey)
    {
        this.jsonKey = jsonKey;
        return this;
    }

    @Nullable
    @FileExists
    public String getJsonKeyFilePath()
    {
        return jsonKeyFilePath;
    }

    @Config("gcs.json-key-file-path")
    @ConfigDescription("JSON key file used to access Google Cloud Storage")
    public GcsFileSystemConfig setJsonKeyFilePath(String jsonKeyFilePath)
    {
        this.jsonKeyFilePath = jsonKeyFilePath;
        return this;
    }

    public void validate()
    {
        // This cannot be normal validation, as it would make it impossible to write TestHiveGcsConfig.testExplicitPropertyMappings

        if (useGcsAccessToken) {
            checkState(jsonKey == null, "Cannot specify 'hive.gcs.json-key' when 'hive.gcs.use-access-token' is set");
            checkState(jsonKeyFilePath == null, "Cannot specify 'hive.gcs.json-key-file-path' when 'hive.gcs.use-access-token' is set");
        }
        checkState(jsonKey == null || jsonKeyFilePath == null, "'hive.gcs.json-key' and 'hive.gcs.json-key-file-path' cannot be both set");
    }
}
