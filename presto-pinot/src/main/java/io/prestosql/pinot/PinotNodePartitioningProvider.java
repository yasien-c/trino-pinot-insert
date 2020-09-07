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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Node;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.BucketFunction;
import io.prestosql.spi.connector.ConnectorBucketNodeMap;
import io.prestosql.spi.connector.ConnectorNodePartitioningProvider;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import org.apache.pinot.spi.data.TimeFieldSpec;

import javax.inject.Inject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.prestosql.spi.connector.ConnectorBucketNodeMap.createBucketNodeMap;
import static java.util.Objects.requireNonNull;

public class PinotNodePartitioningProvider
        implements ConnectorNodePartitioningProvider
{
    private final NodeManager nodeManager;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    @Inject
    public PinotNodePartitioningProvider(NodeManager nodeManager)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public ConnectorBucketNodeMap getBucketNodeMap(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        PinotPartitioningHandle handle = (PinotPartitioningHandle) partitioningHandle;
        Map<String, Node> nodesById = uniqueIndex(nodeManager.getRequiredWorkerNodes(), Node::getNodeIdentifier);
        ImmutableList.Builder<Node> bucketToNode = ImmutableList.builder();
        for (String nodeIdentifier : handle.getNodes().get()) {
            Node node = nodesById.get(nodeIdentifier);
            if (node == null) {
                throw new PrestoException(NO_NODES_AVAILABLE, "Node for bucket is offline: " + nodeIdentifier);
            }
            bucketToNode.add(node);
        }
        return createBucketNodeMap(bucketToNode.build());
    }

    @Override
    public ToIntFunction<ConnectorSplit> getSplitBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle)
    {
        PinotPartitioningHandle pinotPartitioningHandle = (PinotPartitioningHandle) partitioningHandle;
        if (pinotPartitioningHandle.getNodes().isPresent()) {
            return value -> ThreadLocalRandom.current().nextInt(pinotPartitioningHandle.getNodes().get().size());
        }
        return value -> 0;
    }

    @Override
    public BucketFunction getBucketFunction(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorPartitioningHandle partitioningHandle,
            List<Type> partitionChannelTypes,
            int bucketCount)
    {
        Type type = getOnlyElement(partitionChannelTypes);
        checkState(type instanceof BigintType, "Unexpected type");
        // TODO: get from partitioning handle
        PinotPartitioningHandle pinotPartitioningHandle = (PinotPartitioningHandle) partitioningHandle;
        checkState(pinotPartitioningHandle.getTimeFieldSpec().isPresent(), "Timefield spec is not present");
        TimeFieldSpec timeFieldSpec = pinotPartitioningHandle.getTimeFieldSpec().get().toTimeFieldSpec();
        TimeUnit timeUnit = timeFieldSpec.getIncomingGranularitySpec().getTimeType();

        return (page, position) -> {
            long timeValue = page.getBlock(0).getLong(position, 0);
            String format = dateFormat.format(new Date(timeUnit.toMillis(timeValue)));
            return Objects.hash(format) % bucketCount;
        };
    }
}
