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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.pinot.query.AggregationExpression;
import io.prestosql.pinot.query.DynamicTable;
import io.prestosql.pinot.query.DynamicTableBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ColumnNotFoundException;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.pinot.common.data.Schema;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static io.prestosql.pinot.PinotColumn.getPinotColumnsForPinotSchema;
import static io.prestosql.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static io.prestosql.pinot.query.DynamicTableBuilder.DOUBLE_AGGREGATIONS;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PinotMetadata
        implements ConnectorMetadata
{
    private static final Object ALL_TABLES_CACHE_KEY = new Object();

    private final LoadingCache<String, List<PinotColumn>> pinotTableColumnCache;
    private final LoadingCache<Object, List<String>> allTablesCache;
    private final PinotConfig pinotConfig;

    @Inject
    public PinotMetadata(
            PinotClusterInfoFetcher pinotClusterInfoFetcher,
            PinotConfig pinotConfig,
            @ForPinot Executor executor)
    {
        this.pinotConfig = requireNonNull(pinotConfig, "pinot config");
        long metadataCacheExpiryMillis = this.pinotConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);
        this.allTablesCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(metadataCacheExpiryMillis, TimeUnit.MILLISECONDS)
                .build(asyncReloading(CacheLoader.from(pinotClusterInfoFetcher::getAllTables), executor));
        this.pinotTableColumnCache =
                CacheBuilder.newBuilder()
                        .refreshAfterWrite(metadataCacheExpiryMillis, TimeUnit.MILLISECONDS)
                        .build(asyncReloading(new CacheLoader<String, List<PinotColumn>>()
                        {
                            @Override
                            public List<PinotColumn> load(String tableName)
                                    throws Exception
                            {
                                Schema tablePinotSchema = pinotClusterInfoFetcher.getTableSchema(tableName);
                                return getPinotColumnsForPinotSchema(tablePinotSchema);
                            }
                        }, executor));

        executor.execute(() -> this.allTablesCache.refresh(ALL_TABLES_CACHE_KEY));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of("default");
    }

    @Override
    public PinotTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (tableName.getTableName().trim().startsWith("select ")) {
            DynamicTable dynamicTable = DynamicTableBuilder.buildFromPql(this, tableName);
            return new PinotTableHandle(tableName.getSchemaName(), dynamicTable.getTableName(), TupleDomain.all(), OptionalLong.empty(), Optional.of(dynamicTable));
        }
        String pinotTableName = getPinotTableNameFromPrestoTableName(tableName.getTableName());
        return new PinotTableHandle(tableName.getSchemaName(), pinotTableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) table;
        if (pinotTableHandle.getQuery().isPresent()) {
            DynamicTable dynamicTable = pinotTableHandle.getQuery().get();
            Map<String, ColumnHandle> columnHandles = getColumnHandles(session, table);
            ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
            for (String columnName : dynamicTable.getSelections()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }
            for (String columnName : dynamicTable.getGroupingColumns()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }
            for (AggregationExpression aggregationExpression : dynamicTable.getAggregateColumns()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }
            SchemaTableName schemaTableName = new SchemaTableName(pinotTableHandle.getSchemaName(), dynamicTable.getTableName());
            return new ConnectorTableMetadata(schemaTableName, columnMetadataBuilder.build());
        }
        SchemaTableName tableName = new SchemaTableName(pinotTableHandle.getSchemaName(), pinotTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String table : getPinotTableNames()) {
            builder.add(new SchemaTableName("default", table));
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        if (pinotTableHandle.getQuery().isPresent()) {
            return getDynamicTableColumnHandles(pinotTableHandle);
        }
        String pinotTableName = getPinotTableNameFromPrestoTableName(pinotTableHandle.getTableName());
        PinotTable table = getPinotTable(pinotTableName);
        if (table == null) {
            throw new TableNotFoundException(pinotTableHandle.toSchemaTableName());
        }
        return table.getColumnHandles();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((PinotColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        PinotTableHandle handle = (PinotTableHandle) table;
        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }
        Optional<DynamicTable> dynamicTable = handle.getQuery();
        if (dynamicTable.isPresent() &&
                (!dynamicTable.get().getLimit().isPresent() || dynamicTable.get().getLimit().getAsLong() > limit)) {
            dynamicTable = Optional.of(new DynamicTable(dynamicTable.get().getTableName(),
                    dynamicTable.get().getSelections(),
                    dynamicTable.get().getGroupingColumns(),
                    dynamicTable.get().getFilter(),
                    dynamicTable.get().getAggregateColumns(),
                    dynamicTable.get().getOrderBy(),
                    OptionalLong.of(limit),
                    dynamicTable.get().getOffset(),
                    dynamicTable.get().getQuery()));
        }

        handle = new PinotTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getConstraint(),
                OptionalLong.of(limit),
                dynamicTable);
        return Optional.of(new LimitApplicationResult<>(handle, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        PinotTableHandle handle = (PinotTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new PinotTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                newDomain,
                handle.getLimit(),
                handle.getQuery());
        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary()));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    public PinotTable getPinotTable(String tableName)
    {
        return new PinotTable(tableName, getFromCache(pinotTableColumnCache, tableName));
    }

    private List<String> getPinotTableNames()
    {
        return getFromCache(allTablesCache, ALL_TABLES_CACHE_KEY);
    }

    private static <K, V> V getFromCache(LoadingCache<K, V> cache, K key)
    {
        V value = cache.getIfPresent(key);
        if (value != null) {
            return value;
        }
        try {
            return cache.get(key);
        }
        catch (ExecutionException e) {
            throw new PinotException(PinotErrorCode.PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "Cannot fetch from cache " + key, e.getCause());
        }
    }

    private String getPinotTableNameFromPrestoTableName(String prestoTableName)
    {
        List<String> allTables = getPinotTableNames();
        for (String pinotTableName : allTables) {
            if (prestoTableName.equalsIgnoreCase(pinotTableName)) {
                return pinotTableName;
            }
        }
        throw new PinotException(PinotErrorCode.PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "Unable to find the presto table " + prestoTableName + " in " + allTables);
    }

    private Map<String, ColumnHandle> getDynamicTableColumnHandles(PinotTableHandle pinotTableHandle)
    {
        checkState(pinotTableHandle.getQuery().isPresent(), "dynamic table not present");
        String schemaName = pinotTableHandle.getSchemaName();
        DynamicTable dynamicTable = pinotTableHandle.getQuery().get();
        PinotTable table = getPinotTable(dynamicTable.getTableName());
        if (table == null) {
            throw new TableNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()));
        }

        Map<String, ColumnHandle> columnHandles = table.getColumnHandles();
        ImmutableMap.Builder<String, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
        for (String columnName : dynamicTable.getSelections()) {
            PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
            if (columnHandle == null) {
                throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), columnName);
            }
            columnHandlesBuilder.put(columnName.toLowerCase(ENGLISH), columnHandle);
        }
        for (String columnName : dynamicTable.getGroupingColumns()) {
            PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
            if (columnHandle == null) {
                throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), columnName);
            }
            columnHandlesBuilder.put(columnName.toLowerCase(ENGLISH), columnHandle);
        }
        for (AggregationExpression aggregationExpression : dynamicTable.getAggregateColumns()) {
            if (DOUBLE_AGGREGATIONS.contains(aggregationExpression.getAggregationType().toLowerCase(ENGLISH))) {
                columnHandlesBuilder.put(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH),
                        new PinotColumnHandle(aggregationExpression.getOutputColumnName(), DOUBLE, REGULAR));
            }
            else {
                PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(aggregationExpression.getBaseColumnName().toLowerCase(ENGLISH));
                if (columnHandle == null) {
                    throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), aggregationExpression.getBaseColumnName());
                }
                columnHandlesBuilder.put(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH),
                        new PinotColumnHandle(aggregationExpression.getOutputColumnName(),
                                columnHandle.getDataType(),
                                REGULAR));
            }
        }
        return columnHandlesBuilder.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        String pinotTableName = getPinotTableNameFromPrestoTableName(tableName.getTableName());
        PinotTable table = getPinotTable(pinotTableName);
        if (table == null) {
            return null;
        }
        return new ConnectorTableMetadata(tableName, table.getColumnsMetadata());
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (!prefix.getSchema().isPresent() || !prefix.getTable().isPresent()) {
            return listTables(session, Optional.empty());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get()));
    }
}
