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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GcsStorageFactory
{
    public static final String GCS_OAUTH_KEY = "gcs.oauth";
    // Copied from com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.util.CredentialFactory
    public static final ImmutableList<String> DEFAULT_SCOPES = ImmutableList.of("https://www.googleapis.com/auth/cloud-platform");
    private final String projectId;
    private final boolean useGcsAccessToken;
    private final Optional<GoogleCredentials> jsonGoogleCredential;

    @Inject
    public GcsStorageFactory(GcsFileSystemConfig config)
            throws IOException
    {
        projectId = config.getProjectId();
        useGcsAccessToken = config.isUseGcsAccessToken();
        String jsonKey = config.getJsonKey();
        String jsonKeyFilePath = config.getJsonKeyFilePath();
        if (jsonKey != null) {
            try (InputStream inputStream = new ByteArrayInputStream(jsonKey.getBytes(UTF_8))) {
                jsonGoogleCredential = Optional.of(GoogleCredentials.fromStream(inputStream).createScoped(DEFAULT_SCOPES));
            }
        }
        else if (jsonKeyFilePath != null) {
            try (FileInputStream inputStream = new FileInputStream(jsonKeyFilePath)) {
                jsonGoogleCredential = Optional.of(GoogleCredentials.fromStream(inputStream).createScoped(DEFAULT_SCOPES));
            }
        }
        else {
            jsonGoogleCredential = Optional.empty();
        }
    }

    public Storage create(ConnectorIdentity identity)
    {
        try {
            GoogleCredentials credentials;
            if (useGcsAccessToken) {
                String accessToken = nullToEmpty(identity.getExtraCredentials().get(GCS_OAUTH_KEY));
                try (ByteArrayInputStream inputStream = new ByteArrayInputStream(accessToken.getBytes(UTF_8))) {
                    credentials = GoogleCredentials.fromStream(inputStream).createScoped(DEFAULT_SCOPES);
                }
            }
            else {
                credentials = jsonGoogleCredential.orElseThrow(() -> new IllegalStateException("GCS credentials not configured"));
            }
            StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder();
            if (projectId != null) {
                storageOptionsBuilder.setProjectId(projectId);
            }
            return storageOptionsBuilder.setCredentials(credentials).build().getService();
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }
}
