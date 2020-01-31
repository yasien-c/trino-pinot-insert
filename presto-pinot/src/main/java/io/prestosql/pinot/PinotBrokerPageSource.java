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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.pinot.query.BrokerPqlWithContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.FixedWidthType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.pinot.PinotErrorCode.PINOT_DECODE_ERROR;
import static io.prestosql.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static io.prestosql.pinot.PinotErrorCode.PINOT_INSUFFICIENT_SERVER_RESPONSE;
import static io.prestosql.pinot.PinotErrorCode.PINOT_UNEXPECTED_RESPONSE;
import static io.prestosql.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static io.prestosql.pinot.PinotMetrics.doWithRetries;
import static java.lang.Boolean.parseBoolean;
import static java.util.Objects.requireNonNull;

public class PinotBrokerPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(PinotBrokerPageSource.class);

    private static final String REQUEST_PAYLOAD_TEMPLATE = "{\"pql\" : \"%s\" }";
    private static final String QUERY_URL_TEMPLATE = "http://%s/query";

    private static final String PINOT_INFINITY = "∞";
    private static final String PINOT_POSITIVE_INFINITY = "+" + PINOT_INFINITY;
    private static final String PINOT_NEGATIVE_INFINITY = "-" + PINOT_INFINITY;

    private static final Double PRESTO_INFINITY = Double.POSITIVE_INFINITY;
    private static final Double PRESTO_NEGATIVE_INFINITY = Double.NEGATIVE_INFINITY;

    private final BrokerPqlWithContext brokerPql;
    private final List<PinotColumnHandle> columnHandles;
    private final PinotClusterInfoFetcher clusterInfoFetcher;
    private final ConnectorSession session;
    private final ObjectMapper objectMapper;

    private boolean finished;
    private long readTimeNanos;
    private long completedBytes;

    public PinotBrokerPageSource(
            ConnectorSession session,
            BrokerPqlWithContext brokerPql,
            List<PinotColumnHandle> columnHandles,
            PinotClusterInfoFetcher clusterInfoFetcher,
            ObjectMapper objectMapper)
    {
        this.brokerPql = requireNonNull(brokerPql, "broker is null");
        this.clusterInfoFetcher = requireNonNull(clusterInfoFetcher, "cluster info fetcher is null");
        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.session = requireNonNull(session, "session is null");
        this.objectMapper = requireNonNull(objectMapper, "object mapper is null");
    }

    static Double parseDouble(String value)
    {
        try {
            return Double.valueOf(value);
        }
        catch (NumberFormatException ne) {
            switch (value) {
                case PINOT_INFINITY:
                case PINOT_POSITIVE_INFINITY:
                    return PRESTO_INFINITY;
                case PINOT_NEGATIVE_INFINITY:
                    return PRESTO_NEGATIVE_INFINITY;
            }
            throw new PinotException(PINOT_DECODE_ERROR, Optional.empty(), "Cannot decode double value from pinot " + value, ne);
        }
    }

    private void setValue(Type type, BlockBuilder blockBuilder, String value)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        if (!(type instanceof FixedWidthType) && !(type instanceof VarcharType)) {
            throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "type '" + type + "' not supported");
        }
        if (type instanceof FixedWidthType) {
            completedBytes += ((FixedWidthType) type).getFixedSize();
            if (type instanceof BigintType) {
                type.writeLong(blockBuilder, parseDouble(value).longValue());
            }
            else if (type instanceof IntegerType) {
                blockBuilder.writeInt(parseDouble(value).intValue());
            }
            else if (type instanceof BooleanType) {
                type.writeBoolean(blockBuilder, parseBoolean(value));
            }
            else {
                throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "type '" + type + "' not supported");
            }
        }
        else {
            Slice slice = Slices.utf8Slice(value);
            blockBuilder.writeBytes(slice, 0, slice.length()).closeEntry();
            completedBytes += slice.length();
        }
    }

    private void setValuesForGroupby(
            List<BlockBuilder> blockBuilders,
            List<Type> types,
            Map<String, Integer> columnIndices,
            Map<Integer, String> groupByFunctions,
            List<String> groupByColumns,
            JsonNode group,
            String[] values)
    {
        checkState(groupByColumns.size() == group.size(), "Expected '%s' groupBy columns got '%s'", group.size(), groupByColumns.size());
        for (int i = 0; i < group.size(); i++) {
            int index = columnIndices.getOrDefault(groupByColumns.get(i), -1);
            if (index >= 0) {
                setValue(types.get(index), blockBuilders.get(index), group.get(i).asText());
            }
        }
        for (int i = 0; i < values.length; i++) {
            String columnName = groupByFunctions.get(i);
            if (columnName != null) {
                int index = columnIndices.getOrDefault(columnName, -1);
                if (index >= 0) {
                    setValue(types.get(index), blockBuilders.get(index), values[i]);
                }
            }
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }

        long start = System.nanoTime();
        try {
            List<Type> expectedTypes = columnHandles.stream()
                    .map(PinotColumnHandle::getDataType)
                    .collect(Collectors.toList());
            PageBuilder pageBuilder = new PageBuilder(expectedTypes);
            ImmutableList.Builder<BlockBuilder> columnBlockBuilders = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableMap.Builder<String, Integer> columnIndices = ImmutableMap.builder();
            for (int i = 0; i < columnHandles.size(); i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                columnBlockBuilders.add(blockBuilder);
                columnTypes.add(expectedTypes.get(i));
                columnIndices.put(columnHandles.get(i).getColumnName(), i);
            }

            int counter = issuePqlAndPopulate(
                    brokerPql.getTable(),
                    brokerPql.getPql(),
                    brokerPql.getGroupByClauses(),
                    columnBlockBuilders.build(),
                    columnTypes.build(),
                    columnIndices.build());
            pageBuilder.declarePositions(counter);
            Page page = pageBuilder.build();

            // TODO: Implement chunking if the result set is ginormous
            finished = true;

            return page;
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }

    private int issuePqlAndPopulate(
            String table,
            String pql,
            int numGroupByClause,
            List<BlockBuilder> blockBuilders,
            List<Type> types,
            Map<String, Integer> columnIndices)
    {
        return doWithRetries(PinotSessionProperties.getPinotRetryCount(session), (retryNumber) -> {
            String queryHost = clusterInfoFetcher.getBrokerHost(table);
            log.info("Query '%s' on broker host '%s'", queryHost, pql);
            Request.Builder builder = Request.Builder
                    .preparePost()
                    .setUri(URI.create(String.format(QUERY_URL_TEMPLATE, queryHost)));
            String body = clusterInfoFetcher.doHttpActionWithHeaders(builder, Optional.of(String.format(REQUEST_PAYLOAD_TEMPLATE, pql)));

            return populateFromPqlResults(pql, numGroupByClause, blockBuilders, types, columnIndices, body);
        });
    }

    @VisibleForTesting
    public int populateFromPqlResults(
            String pql,
            int numGroupByClause,
            List<BlockBuilder> blockBuilders,
            List<Type> types,
            Map<String, Integer> columnIndices,
            String body)
    {
        JsonNode jsonBody;

        try {
            jsonBody = objectMapper.readTree(body);
        }
        catch (IOException e) {
            throw new PinotException(PINOT_UNEXPECTED_RESPONSE, Optional.of(pql), "Couldn't parse response", e);
        }

        JsonNode numServersResponded = jsonBody.get("numServersResponded");
        JsonNode numServersQueried = jsonBody.get("numServersQueried");

        if (numServersQueried == null || numServersResponded == null || numServersQueried.asInt() > numServersResponded.asInt()) {
            throw new PinotException(
                    PINOT_INSUFFICIENT_SERVER_RESPONSE,
                    Optional.of(pql),
                    String.format("Only %s out of %s servers responded for query %s", numServersResponded.asInt(), numServersQueried.asInt(), pql));
        }

        JsonNode exceptions = jsonBody.get("exceptions");
        if (exceptions != null && exceptions.isArray() && exceptions.size() > 0) {
            // Pinot is known to return exceptions with benign errorcodes like 200
            // so we treat any exception as an error
            throw new PinotException(
                    PINOT_EXCEPTION,
                    Optional.of(pql),
                    String.format("Query %s encountered exception %s", pql, exceptions.get(0)));
        }

        JsonNode aggregationResults = jsonBody.get("aggregationResults");
        JsonNode selectionResults = jsonBody.get("selectionResults");

        int rowCount;
        if (aggregationResults != null && aggregationResults.isArray()) {
            // This is map is populated only when we have multiple aggregates with a group by
            checkState(aggregationResults.size() >= 1, "Expected at least one metric to be present");
            Map<JsonNode, String[]> groupToValue = aggregationResults.size() == 1 || numGroupByClause == 0 ? null : new HashMap<>();
            Map<Integer, String> indiceToGroupByFunction = new HashMap<>();
            rowCount = 0;
            String[] singleAggregation = new String[1];
            Boolean seenGroupByResult = null;
            List<String> groupByColumnNames = new ArrayList<>();
            for (int aggregationIndex = 0; aggregationIndex < aggregationResults.size(); aggregationIndex++) {
                JsonNode result = aggregationResults.get(aggregationIndex);

                JsonNode metricValuesForEachGroup = result.get("groupByResult");

                if (metricValuesForEachGroup != null) {
                    checkState(seenGroupByResult == null || seenGroupByResult);
                    seenGroupByResult = true;
                    checkState(numGroupByClause > 0, "Expected having non zero group by clauses");
                    JsonNode groupByColumns = checkNotNull(result.get("groupByColumns"), "groupByColumns missing in %s", pql);
                    if (groupByColumns.size() != numGroupByClause) {
                        throw new PinotException(
                                PINOT_UNEXPECTED_RESPONSE,
                                Optional.of(pql),
                                String.format("Expected %d gby columns but got %s instead from pinot", numGroupByClause, groupByColumns));
                    }
                    if (groupByColumnNames.isEmpty()) {
                        for (int i = 0; i < groupByColumns.size(); i++) {
                            groupByColumnNames.add(groupByColumns.get(i).asText());
                        }
                    }
                    indiceToGroupByFunction.computeIfAbsent(aggregationIndex, (ignored) -> result.get("function").asText());
                    // group by aggregation
                    for (int groupByIndex = 0; groupByIndex < metricValuesForEachGroup.size(); groupByIndex++) {
                        JsonNode row = metricValuesForEachGroup.get(groupByIndex);
                        JsonNode group = row.get("group");
                        if (group == null || !group.isArray() || group.size() != numGroupByClause) {
                            throw new PinotException(
                                    PINOT_UNEXPECTED_RESPONSE,
                                    Optional.of(pql),
                                    String.format("Expected %d group by columns but got only a group of size %d (%s)", numGroupByClause, group.size(), group));
                        }
                        if (groupToValue == null) {
                            singleAggregation[0] = row.get("value").asText();
                            setValuesForGroupby(blockBuilders, types, columnIndices, indiceToGroupByFunction, groupByColumnNames, group, singleAggregation);
                            rowCount++;
                        }
                        else {
                            groupToValue.computeIfAbsent(group, (ignored) -> new String[aggregationResults.size()])[aggregationIndex] = row.get("value").asText();
                        }
                    }
                }
                else {
                    checkState(seenGroupByResult == null || !seenGroupByResult);
                    seenGroupByResult = false;
                    // simple aggregation
                    // TODO: Validate that this is expected semantically
                    checkState(numGroupByClause == 0, "Expected no group by columns in pinot");
                    int index = columnIndices.getOrDefault(result.get("function").asText(), -1);
                    if (index >= 0) {
                        setValue(types.get(index), blockBuilders.get(index), result.get("value").asText());
                    }
                    rowCount = 1;
                }
            }

            if (groupToValue != null) {
                checkState(rowCount == 0, "Row count shouldn't have changed from zero");

                groupToValue.forEach((group, values) -> setValuesForGroupby(blockBuilders, types, columnIndices, indiceToGroupByFunction, groupByColumnNames, group, values));
                rowCount = groupToValue.size();
            }
        }
        else if (selectionResults != null) {
            JsonNode columns = selectionResults.get("columns");
            JsonNode results = selectionResults.get("results");
            if (columns == null || results == null || !columns.isArray() || !results.isArray() || columns.size() < blockBuilders.size()) {
                throw new PinotException(
                        PINOT_UNEXPECTED_RESPONSE,
                        Optional.of(pql),
                        String.format("Columns and results expected for %s, expected %d columns but got %d", pql, blockBuilders.size(), columns == null ? 0 : columns.size()));
            }
            for (int rowNumber = 0; rowNumber < results.size(); ++rowNumber) {
                JsonNode result = results.get(rowNumber);
                if (result == null || result.size() < blockBuilders.size()) {
                    throw new PinotException(
                            PINOT_UNEXPECTED_RESPONSE,
                            Optional.of(pql),
                            String.format("Expected row of %d columns", blockBuilders.size()));
                }
                for (int columnNumber = 0; columnNumber < columns.size(); columnNumber++) {
                    int index = columnIndices.getOrDefault(columns.get(columnNumber).asText(), -1);
                    if (index >= 0) {
                        setValue(types.get(index), blockBuilders.get(index), result.get(columnNumber).asText());
                    }
                }
            }
            rowCount = results.size();
        }
        else {
            throw new PinotException(
                    PINOT_UNEXPECTED_RESPONSE,
                    Optional.of(pql),
                    "Expected one of aggregationResults or selectionResults to be present");
        }

        checkState(rowCount >= 0, "Expected row count to be initialized");
        return rowCount;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        finished = true;
    }
}
