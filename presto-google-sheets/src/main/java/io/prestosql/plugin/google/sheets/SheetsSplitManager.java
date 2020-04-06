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
package io.prestosql.plugin.google.sheets;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class SheetsSplitManager
        implements ConnectorSplitManager
{

    private final SheetsClient sheetsClient;
    private final int maxRowsPerSplit;

    @Inject
    public SheetsSplitManager(SheetsClient sheetsClient, SheetsConfig config)
    {
        this.sheetsClient = requireNonNull(sheetsClient, "client is null");
        maxRowsPerSplit = requireNonNull(config, "config is null").getMaxRowsPerSplit();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        SheetsTableHandle tableHandle = (SheetsTableHandle) connectorTableHandle;
        SheetDataLocation dataLocation = sheetsClient.parseDataLocationNoHeader(tableHandle.getTableName());
        List<ConnectorSplit> splits = dataLocation.partition(maxRowsPerSplit).stream()
                .map(location -> new SheetsSplit(tableHandle.getSchemaName(), tableHandle.getTableName(), location))
                .collect(toImmutableList());
        return new FixedSplitSource(splits);
    }
}
