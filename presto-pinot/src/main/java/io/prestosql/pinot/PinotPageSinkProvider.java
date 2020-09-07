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

import io.prestosql.pinot.client.PinotClient;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

import javax.inject.Inject;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PinotPageSinkProvider
        implements ConnectorPageSinkProvider
{
    private final PinotClient pinotClient;
    private final String segmentCreationBaseDirectory;
    private List<String> pinotControllerUrls;

    @Inject
    public PinotPageSinkProvider(
            PinotConfig config,
            PinotClient pinotClient)
    {
        requireNonNull(config, "config is null");
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
        this.segmentCreationBaseDirectory = config.getSegmentCreationBaseDirectory();
        this.pinotControllerUrls = config.getControllerUrls();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle outputTableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle insertTableHandle)
    {
        PinotInsertTableHandle pinotInsertTableHandle = (PinotInsertTableHandle) insertTableHandle;
        return new PinotPageSink(pinotInsertTableHandle, pinotClient, segmentCreationBaseDirectory, pinotControllerUrls);
    }
}
