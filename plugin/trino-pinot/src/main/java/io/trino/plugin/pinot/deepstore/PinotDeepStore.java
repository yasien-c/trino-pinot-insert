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
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public class PinotDeepStore
{
    private final URI deepStoreUri;
    private final PinotFS pinotFS;

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
        PinotFSFactory.init(pinotConfiguration.subset("storage.factory"));
        this.pinotFS = PinotFSFactory.create(deepStoreUri.getScheme());
    }

    public URI getDeepStoreUri()
    {
        return deepStoreUri;
    }

    public PinotFS getPinotFS()
    {
        return pinotFS;
    }
}
