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
package io.trino.plugin.pinot.deepstore;

import com.google.inject.Inject;
import org.apache.pinot.spi.env.PinotConfiguration;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class PinotDeepStore
        implements DeepStore
{
    private final URI deepStoreUri;
    private final PinotConfiguration pinotConfiguration;

    public enum DeepStoreProvider
    {
        NONE,
        GCS,
        S3
    }

    @Inject
    public PinotDeepStore(PinotConfiguration pinotConfiguration, PinotDeepStoreConfig pinotDeepStoreConfig)
    {
        requireNonNull(pinotDeepStoreConfig, "pinotDeepStoreConfig is null");
        this.deepStoreUri = pinotDeepStoreConfig.getDeepStoreUri();
        this.pinotConfiguration = pinotConfiguration.subset("storage.factory");
    }

    public URI getDeepStoreUri()
    {
        return deepStoreUri;
    }

    public PinotConfiguration getPinotConfiguration()
    {
        return pinotConfiguration;
    }
}
