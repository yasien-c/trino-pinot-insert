package io.trino.plugin.pinot.query.ptf;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.query.OrderByExpression;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PinotQueryRelationHandle
        implements PinotRelationHandle
{
    private final String remoteTableName;
    private final Optional<PinotTableType> tableType;
    private final List<PinotColumnHandle> projections;
    private final Optional<String> filter;
    private final List<PinotColumnHandle> groupingColumns;
    private final List<PinotColumnHandle> aggregateColumns;
    private final Optional<String> havingExpression;
    private final List<OrderByExpression> orderBy;
    private final OptionalLong limit;
    private final OptionalLong offset;
    private final boolean isAggregateInProjections;

    @JsonCreator
    public PinotQueryRelationHandle(
            @JsonProperty String remoteTableName,
            @JsonProperty Optional<PinotTableType> tableType,
            @JsonProperty List<PinotColumnHandle> projections,
            @JsonProperty Optional<String> filter,
            @JsonProperty List<PinotColumnHandle> groupingColumns,
            @JsonProperty List<PinotColumnHandle> aggregateColumns,
            @JsonProperty Optional<String> havingExpression,
            @JsonProperty List<OrderByExpression> orderBy,
            @JsonProperty OptionalLong limit,
            @JsonProperty OptionalLong offset)
    {
        this.remoteTableName = requireNonNull(remoteTableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
        this.filter = requireNonNull(filter, "filter is null");
        this.groupingColumns = ImmutableList.copyOf(requireNonNull(groupingColumns, "groupingColumns is null"));
        this.aggregateColumns = ImmutableList.copyOf(requireNonNull(aggregateColumns, "aggregateColumns is null"));
        this.havingExpression = requireNonNull(havingExpression, "havingExpression is null");
        this.orderBy = ImmutableList.copyOf(requireNonNull(orderBy, "orderBy is null"));
        this.limit = requireNonNull(limit, "limit is null");
        this.offset = requireNonNull(offset, "offset is null");
        this.isAggregateInProjections = projections.stream()
                .anyMatch(PinotColumnHandle::isAggregate);
    }

    @JsonProperty
    public String getRemoteTableName()
    {
        return remoteTableName;
    }

    @JsonProperty
    public Optional<PinotTableType> getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public List<PinotColumnHandle> getProjections()
    {
        return projections;
    }

    @JsonProperty
    public Optional<String> getFilter()
    {
        return filter;
    }

    @JsonProperty
    public List<PinotColumnHandle> getGroupingColumns()
    {
        return groupingColumns;
    }

    @JsonProperty
    public List<PinotColumnHandle> getAggregateColumns()
    {
        return aggregateColumns;
    }

    @JsonProperty
    public Optional<String> getHavingExpression()
    {
        return havingExpression;
    }

    @JsonProperty
    public List<OrderByExpression> getOrderBy()
    {
        return orderBy;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public OptionalLong getOffset()
    {
        return offset;
    }

    public boolean isAggregateInProjections()
    {
        return isAggregateInProjections;
    }

    public Stream<PinotColumnHandle> getColumnHandlesForSelect()
    {
        return Streams.concat(getProjections().stream(), getAggregateColumns().stream());
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }

        if (!(other instanceof PinotRelationHandle)) {
            return false;
        }

        PinotQueryRelationHandle that = (PinotQueryRelationHandle) other;
        return remoteTableName.equals(that.remoteTableName) &&
                tableType.equals(that.tableType) &&
                projections.equals(that.projections) &&
                filter.equals(that.filter) &&
                groupingColumns.equals(that.groupingColumns) &&
                aggregateColumns.equals(that.aggregateColumns) &&
                havingExpression.equals(that.havingExpression) &&
                orderBy.equals(that.orderBy) &&
                limit.equals(that.limit) &&
                offset.equals(that.offset);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(remoteTableName, tableType, projections, filter, groupingColumns, aggregateColumns, havingExpression, orderBy, limit, offset);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("remoteTableName", remoteTableName)
                .add("tableType", tableType)
                .add("projections", projections)
                .add("filter", filter)
                .add("groupingColumns", groupingColumns)
                .add("aggregateColumns", aggregateColumns)
                .add("havingExpression", havingExpression)
                .add("orderBy", orderBy)
                .add("limit", limit)
                .add("offset", offset)
                .toString();
    }
}
