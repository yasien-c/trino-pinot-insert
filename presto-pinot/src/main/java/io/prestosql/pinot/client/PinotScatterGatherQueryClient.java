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
package io.prestosql.pinot.client;

import com.yammer.metrics.core.MetricsRegistry;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.InstanceRequest;
import org.apache.pinot.common.response.ServerInstance;
import org.apache.pinot.common.utils.DataTable;
import org.apache.pinot.core.common.datatable.DataTableFactory;
import org.apache.pinot.pql.parsers.Pql2CompilationException;
import org.apache.pinot.pql.parsers.Pql2Compiler;
import org.apache.pinot.serde.SerDe;
import org.apache.pinot.transport.common.CompositeFuture;
import org.apache.pinot.transport.metrics.NettyClientMetrics;
import org.apache.pinot.transport.netty.PooledNettyClientResourceManager;
import org.apache.pinot.transport.pool.KeyedPool;
import org.apache.pinot.transport.pool.KeyedPoolImpl;
import org.apache.pinot.transport.scattergather.ScatterGather;
import org.apache.pinot.transport.scattergather.ScatterGatherImpl;
import org.apache.pinot.transport.scattergather.ScatterGatherRequest;
import org.apache.pinot.transport.scattergather.ScatterGatherStats;
import org.apache.thrift.protocol.TCompactProtocol;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static java.lang.String.format;

public class PinotScatterGatherQueryClient
{
    private static final Pql2Compiler REQUEST_COMPILER = new Pql2Compiler();
    private static final String PRESTO_HOST_PREFIX = "presto-pinot-master";
    private static final boolean DEFAULT_EMIT_TABLE_LEVEL_METRICS = true;

    private final String prestoHostId;
    private final BrokerMetrics brokerMetrics;
    private final ScatterGather scatterGatherer;
    private final long requestTimeoutMillis;

    public enum ErrorCode
    {
        PINOT_INSUFFICIENT_SERVER_RESPONSE(true),
        PINOT_INVALID_PQL_GENERATED(false),
        PINOT_UNCLASSIFIED_ERROR(false);

        private final boolean retriable;

        ErrorCode(boolean retriable)
        {
            this.retriable = retriable;
        }

        public boolean isRetriable()
        {
            return retriable;
        }
    }

    public static class PinotException
            extends RuntimeException
    {
        private final ErrorCode errorCode;

        public PinotException(ErrorCode errorCode, String message, Throwable t)
        {
            super(message, t);
            this.errorCode = errorCode;
        }

        public PinotException(ErrorCode errorCode, String message)
        {
            this(errorCode, message, null);
        }

        public ErrorCode getErrorCode()
        {
            return errorCode;
        }
    }

    public static class Config
    {
        private final long idleTimeoutMillis;
        private final long requestTimeoutMillis;
        private final int threadPoolSize;
        private final int minConnectionsPerServer;
        private final int maxBacklogPerServer;
        private final int maxConnectionsPerServer;

        public Config(long idleTimeoutMillis, long requestTimeoutMillis, int threadPoolSize, int minConnectionsPerServer, int maxBacklogPerServer, int maxConnectionsPerServer)
        {
            this.idleTimeoutMillis = idleTimeoutMillis;
            this.requestTimeoutMillis = requestTimeoutMillis;
            this.threadPoolSize = threadPoolSize;
            this.minConnectionsPerServer = minConnectionsPerServer;
            this.maxBacklogPerServer = maxBacklogPerServer;
            this.maxConnectionsPerServer = maxConnectionsPerServer;
        }

        public long getIdleTimeoutMillis()
        {
            return idleTimeoutMillis;
        }

        public long getRequestTimeoutMillis()
        {
            return requestTimeoutMillis;
        }

        public int getThreadPoolSize()
        {
            return threadPoolSize;
        }

        public int getMinConnectionsPerServer()
        {
            return minConnectionsPerServer;
        }

        public int getMaxBacklogPerServer()
        {
            return maxBacklogPerServer;
        }

        public int getMaxConnectionsPerServer()
        {
            return maxConnectionsPerServer;
        }
    }

    public PinotScatterGatherQueryClient(Config pinotConfig)
    {
        prestoHostId = getDefaultPrestoId();
        requestTimeoutMillis = pinotConfig.getRequestTimeoutMillis();

        MetricsRegistry registry = new MetricsRegistry();
        brokerMetrics = new BrokerMetrics(registry, DEFAULT_EMIT_TABLE_LEVEL_METRICS);
        brokerMetrics.initializeGlobalMeters();

        final NettyClientMetrics clientMetrics = new NettyClientMetrics(registry, "presto_pinot_client_");

        // Setup Netty Connection Pool
        PooledNettyClientResourceManager resourceManager = new PooledNettyClientResourceManager(new NioEventLoopGroup(), new HashedWheelTimer(), clientMetrics);
        ExecutorService requestSenderPool = Executors.newFixedThreadPool(pinotConfig.getThreadPoolSize());
        ScheduledThreadPoolExecutor poolTimeoutExecutor = new ScheduledThreadPoolExecutor(50);
        KeyedPool<PooledNettyClientResourceManager.PooledClientConnection> connPool = new KeyedPoolImpl<>(pinotConfig.getMinConnectionsPerServer(), pinotConfig.getMaxConnectionsPerServer(), pinotConfig.getIdleTimeoutMillis(),
                pinotConfig.getMaxBacklogPerServer(), resourceManager, poolTimeoutExecutor, requestSenderPool, registry);
        resourceManager.setPool(connPool);

        // Setup ScatterGather
        scatterGatherer = new ScatterGatherImpl(connPool, requestSenderPool);
    }

    private static <T> T doWithRetries(int retries, Function<Integer, T> caller)
    {
        PinotException firstError = null;
        for (int i = 0; i < retries; ++i) {
            try {
                return caller.apply(i);
            }
            catch (PinotException e) {
                if (firstError == null) {
                    firstError = e;
                }
                if (!e.getErrorCode().isRetriable()) {
                    throw e;
                }
            }
        }
        throw firstError;
    }

    private String getDefaultPrestoId()
    {
        String defaultBrokerId;
        try {
            defaultBrokerId = PRESTO_HOST_PREFIX + InetAddress.getLocalHost().getHostName();
        }
        catch (UnknownHostException e) {
            defaultBrokerId = PRESTO_HOST_PREFIX;
        }
        return defaultBrokerId;
    }

    public Map<ServerInstance, DataTable> queryPinotServerForDataTable(String pql, String serverHost, List<String> segments, long connectionTimeoutInMillis, int pinotRetryCount)
    {
        BrokerRequest brokerRequest;
        try {
            brokerRequest = REQUEST_COMPILER.compileToBrokerRequest(pql);
        }
        catch (Pql2CompilationException e) {
            throw new PinotException(ErrorCode.PINOT_INVALID_PQL_GENERATED, format("Parsing error with on %s, Error = %s", serverHost, e.getMessage()), e);
        }

        Map<String, List<String>> routingTable = new HashMap<>();
        routingTable.put(serverHost, new ArrayList<>(segments));

        // Unfortunately the retries will all hit the same server because the routing decision has already been made by the pinot broker
        Map<ServerInstance, byte[]> serverResponseMap = doWithRetries(pinotRetryCount, (requestId) -> {
            ScatterGatherRequest scatterRequest = new ScatterGatherRequestWrapper(brokerRequest, routingTable, requestId, connectionTimeoutInMillis, prestoHostId);

            ScatterGatherStats scatterGatherStats = new ScatterGatherStats();
            CompositeFuture<byte[]> compositeFuture = routeScatterGather(scatterRequest, scatterGatherStats);
            return gatherServerResponses(routingTable, compositeFuture, brokerRequest.getQuerySource().getTableName());
        });
        return deserializeServerResponses(serverResponseMap, brokerRequest.getQuerySource().getTableName());
    }

    private Map<ServerInstance, byte[]> gatherServerResponses(
            Map<String, List<String>> routingTable,
            CompositeFuture<byte[]> compositeFuture,
            String tableNameWithType)
    {
        try {
            Map<ServerInstance, byte[]> serverResponseMap = compositeFuture.get(requestTimeoutMillis, TimeUnit.MILLISECONDS);
            if (serverResponseMap.size() != routingTable.size()) {
                Map<String, String> routingTableForLogging = new HashMap<>();
                routingTable.entrySet().forEach(entry -> {
                    String valueToPrint = entry.getValue().size() > 10 ? format("%d segments", entry.getValue().size()) : entry.getValue().toString();
                    routingTableForLogging.put(entry.getKey(), valueToPrint);
                });
                throw new PinotException(ErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE, String.format("%d of %d servers responded with routing table servers: %s, error: %s", serverResponseMap.size(), routingTable.size(), routingTableForLogging, compositeFuture.getError()));
            }
            for (Map.Entry<ServerInstance, byte[]> entry : serverResponseMap.entrySet()) {
                if (entry.getValue().length == 0) {
                    throw new PinotException(ErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE, String.format("Got empty response with from server: %s", entry.getKey().getShortHostName()));
                }
            }
            return serverResponseMap;
        }
        catch (ExecutionException | InterruptedException | TimeoutException e) {
            Throwable err = e instanceof ExecutionException ? e.getCause() : e;
            try {
                compositeFuture.cancel(true);
            }
            catch (Throwable t) {
                // ignored
            }
            throw new PinotException(ErrorCode.PINOT_UNCLASSIFIED_ERROR, String.format("Caught exception while fetching responses for table: %s", tableNameWithType), err);
        }
    }

    /**
     * Deserialize the server responses, put the de-serialized data table into the data table map passed in, append
     * processing exceptions to the processing exception list passed in.
     * <p>For hybrid use case, multiple responses might be from the same instance. Use response sequence to distinguish
     * them.
     *
     * @param responseMap map from server to response.
     * @param tableNameWithType table name with type suffix.
     * @return dataTableMap map from server to data table.
     */
    private Map<ServerInstance, DataTable> deserializeServerResponses(
            Map<ServerInstance, byte[]> responseMap,
            String tableNameWithType)
    {
        Map<ServerInstance, DataTable> dataTableMap = new HashMap<>();
        for (Map.Entry<ServerInstance, byte[]> entry : responseMap.entrySet()) {
            ServerInstance serverInstance = entry.getKey();
            byte[] value = entry.getValue();
            if (value == null || value.length == 0) {
                continue;
            }
            try {
                dataTableMap.put(serverInstance, DataTableFactory.getDataTable(value));
            }
            catch (IOException e) {
                throw new PinotException(ErrorCode.PINOT_UNCLASSIFIED_ERROR, String.format("Caught exceptions while deserializing response for table: %s from server: %s", tableNameWithType, serverInstance), e);
            }
        }
        return dataTableMap;
    }

    private CompositeFuture<byte[]> routeScatterGather(ScatterGatherRequest scatterRequest, ScatterGatherStats scatterGatherStats)
    {
        try {
            return this.scatterGatherer.scatterGather(scatterRequest, scatterGatherStats, true, brokerMetrics);
        }
        catch (InterruptedException e) {
            throw new PinotException(ErrorCode.PINOT_UNCLASSIFIED_ERROR, format("Interrupted while sending request: %s", scatterRequest), e);
        }
    }

    private static class ScatterGatherRequestWrapper
            implements ScatterGatherRequest
    {
        private final BrokerRequest brokerRequest;
        private final Map<String, List<String>> routingTable;
        private final long requestId;
        private final long requestTimeoutMs;
        private final String brokerId;

        public ScatterGatherRequestWrapper(BrokerRequest request, Map<String, List<String>> routingTable, long requestId, long requestTimeoutMs, String brokerId)
        {
            brokerRequest = request;
            this.routingTable = routingTable;
            this.requestId = requestId;

            this.requestTimeoutMs = requestTimeoutMs;
            this.brokerId = brokerId;
        }

        @Override
        public Map<String, List<String>> getRoutingTable()
        {
            return routingTable;
        }

        @Override
        public byte[] getRequestForService(List<String> segments)
        {
            InstanceRequest request = new InstanceRequest();
            request.setRequestId(requestId);
            request.setEnableTrace(brokerRequest.isEnableTrace());
            request.setQuery(brokerRequest);
            request.setSearchSegments(segments);
            request.setBrokerId(brokerId);
            return new SerDe(new TCompactProtocol.Factory()).serialize(request);
        }

        @Override
        public long getRequestId()
        {
            return requestId;
        }

        @Override
        public long getRequestTimeoutMs()
        {
            return requestTimeoutMs;
        }

        @Override
        public BrokerRequest getBrokerRequest()
        {
            return brokerRequest;
        }

        @Override
        public String toString()
        {
            if (routingTable == null) {
                return null;
            }

            return Arrays.toString(routingTable.entrySet().toArray());
        }
    }
}
