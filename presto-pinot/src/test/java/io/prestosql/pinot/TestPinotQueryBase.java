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
package io.prestosql.pinot;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestPinotQueryBase
{
    protected static PinotTableHandle realtimeOnlyTable = new PinotTableHandle("schema", "realtimeOnly");
    protected static PinotTableHandle hybridTable = new PinotTableHandle("schema", "hybrid");

    protected final PinotConfig pinotConfig = new PinotConfig();

    protected final PinotClusterInfoFetcher mockClusterInfoFetcher = new MockPinotClusterInfoFetcher(pinotConfig);
    protected final PinotMetadata pinotMetadata = new PinotMetadata(
            mockClusterInfoFetcher,
            pinotConfig,
            newCachedThreadPool(threadsNamed("mock-pinot-metadata-fetcher")));

    protected List<String> getColumnNames(String table)
    {
        return pinotMetadata.getPinotColumns(table).stream()
                .map(PinotColumn::getName)
                .collect(toImmutableList());
    }
}
