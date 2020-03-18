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
package com.css.eventlistener;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.eventlistener.QueryCompletedEvent;
import io.prestosql.spi.eventlistener.QueryContext;
import io.prestosql.spi.eventlistener.QueryCreatedEvent;
import io.prestosql.spi.eventlistener.QueryFailureInfo;
import io.prestosql.spi.eventlistener.QueryIOMetadata;
import io.prestosql.spi.eventlistener.QueryInputMetadata;
import io.prestosql.spi.eventlistener.QueryMetadata;
import io.prestosql.spi.eventlistener.QueryStatistics;
import io.prestosql.spi.eventlistener.SplitCompletedEvent;
import io.prestosql.spi.eventlistener.StageCpuDistribution;
import io.prestosql.spi.eventlistener.StageGcStatistics;
import io.prestosql.spi.session.ResourceEstimates;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

import java.time.Duration;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class KafkaEventListener
        extends AbstractAsyncEventListener<GenericRecord>
{
    private static final ZoneId ZONE_ID = ZoneId.of("America/Los_Angeles");
    private static final JsonCodec<Map<String, String>> SESSION_PROPERTIES_CODEC = mapJsonCodec(String.class, String.class);
    private static final JsonCodec<List<QueryInputMetadata>> INPUT_METADATA_CODEC = listJsonCodec(QueryInputMetadata.class);
    private static final JsonCodec<List<String>> LIST_STRING_CODEC = listJsonCodec(String.class);
    private static final JsonCodec<List<StageCpuDistribution>> CPU_DISTRIBUTIONS_CODEC = listJsonCodec(StageCpuDistribution.class);
    private static final JsonCodec<List<StageGcStatistics>> STAGE_GC_STATISTICS_CODEC = listJsonCodec(StageGcStatistics.class);
    private static final JsonCodec<List<PrestoWarning>> WARNINGS_CODEC = listJsonCodec(PrestoWarning.class);

    private final Schema eventSchema = SchemaBuilder.record("queryEvent")
            .fields()
            .name("event_type").type().stringType().noDefault()
            .name("query_id").type().stringType().noDefault()
            .name("transaction_id").type().optional().stringType()
            .name("user").type().stringType().noDefault()
            .name("principal").type().optional().stringType()
            .name("source").type().optional().stringType()
            .name("server_version").type().stringType().noDefault()
            .name("environment").type().stringType().noDefault()
            .name("catalog").type().optional().stringType()
            .name("schema").type().optional().stringType()
            .name("remote_client_address").type().optional().stringType()
            .name("client_info").type().optional().stringType()
            .name("client_tags").type().optional().array().items().stringType()
            .name("user_agent").type().optional().stringType()
            .name("resource_estimates").type().optional().map().values().stringType()
            .name("query_state").type().optional().stringType()
            .name("uri").type().optional().stringType()
            .name("field_names").type().optional().array().items().stringType()
            .name("query").type().optional().stringType()
            .name("peak_user_memory_bytes").type().optional().longType()
            .name("peak_total_memory_bytes").type().optional().longType()
            .name("peak_task_user_memory").type().optional().longType()
            .name("peak_task_total_memory").type().optional().longType()
            .name("internal_network_bytes").type().optional().longType()
            .name("internal_network_rows").type().optional().longType()
            .name("create_time").type().optional().stringType()
            .name("execution_start_time").type().optional().stringType()
            .name("end_time").type().optional().stringType()
            .name("queued_time_ms").type().optional().longType()
            .name("analysis_time_ms").type().optional().longType()
            .name("total_split_wall_time_ms").type().optional().longType()
            .name("total_split_cpu_time_ms").type().optional().longType()
            .name("total_data_size_bytes").type().optional().longType()
            .name("total_rows").type().optional().longType()
            .name("inputs_data_size_bytes").type().optional().longType()
            .name("input_rows").type().optional().longType()
            .name("output_data_size_bytes").type().optional().longType()
            .name("output_rows").type().optional().longType()
            .name("written_data_size_bytes").type().optional().longType()
            .name("written_rows").type().optional().longType()
            .name("cumulative_memory_bytes").type().optional().doubleType()
            .name("stage_gc_statistics").type().optional().stringType()
            .name("completed_splits").type().optional().intType()
            .name("error_code").type().optional().stringType()
            .name("error_category").type().optional().stringType() // user, internal, external, insufficient resources
            .name("failure_type").type().optional().stringType() // exception class
            .name("failure_message").type().optional().stringType()
            .name("failure_task").type().optional().stringType()
            .name("failure_host").type().optional().stringType()
            .name("output_stage_json").type().optional().stringType()
            .name("failures_json").type().optional().stringType()
            .name("warnings_json").type().optional().stringType()
            .name("inputs_json").type().optional().stringType()
            .name("session_properties_json").type().optional().stringType()
            .name("resource_group_name").type().optional().stringType()
            .name("cpu_distributions").type().optional().stringType()
            .name("operator_summaries").type().optional().array().items().stringType()
            .name("plan").type().optional().stringType()
            .name("plan_node_stats").type().optional().stringType()
            .name("create_time_epoch_seconds").type().optional().longType()
            .endRecord();

    @Inject
    public KafkaEventListener(KafkaEventLogger eventLogger, KafkaEventLoggerConfig config)
    {
        super(eventLogger, config.getMaxQueueLength());
    }

    @Override
    protected Optional<GenericRecord> normalizeEvent(QueryCreatedEvent event)
    {
        requireNonNull(event, "event is null");
        QueryMetadata metadata = event.getMetadata();
        QueryContext context = event.getContext();
        return Optional.of(new GenericRecordBuilder(eventSchema)
                .set("event_type", EventType.QUERY_CREATED.name())
                .set("query_id", metadata.getQueryId())
                .set("transaction_id", metadata.getTransactionId().orElse(""))
                .set("user", context.getUser())
                .set("principal", context.getPrincipal().orElse(""))
                .set("source", context.getSource().orElse(""))
                .set("server_version", context.getServerVersion())
                .set("environment", context.getEnvironment())
                .set("catalog", context.getCatalog().orElse(""))
                .set("schema", context.getSchema().orElse(""))
                .set("remote_client_address", context.getRemoteClientAddress().orElse(""))
                .set("client_info", context.getClientInfo().orElse(""))
                .set("client_tags", context.getClientTags())
                .set("user_agent", context.getUserAgent().orElse(""))
                .set("resource_estimates", toEstimatesMap(context.getResourceEstimates()))
                .set("query_state", metadata.getQueryState())
                .set("uri", metadata.getUri().toString())
                .set("query", metadata.getQuery())
                .set("create_time", event.getCreateTime().atZone(ZONE_ID).toString())
                .set("resource_group_name", context.getResourceGroupId().map(resourceGroupId -> LIST_STRING_CODEC.toJson(resourceGroupId.getSegments())).orElse(""))
                .set("session_properties_json", SESSION_PROPERTIES_CODEC.toJson(context.getSessionProperties()))
                .set("create_time_epoch_seconds", event.getCreateTime().getEpochSecond())
                .build());
    }

    @Override
    protected Optional<GenericRecord> normalizeEvent(QueryCompletedEvent event)
    {
        requireNonNull(event, "event is null");
        QueryMetadata metadata = event.getMetadata();
        QueryContext context = event.getContext();
        QueryStatistics statistics = event.getStatistics();
        Optional<QueryFailureInfo> failureInfo = event.getFailureInfo();
        QueryIOMetadata queryIOMetadata = event.getIoMetadata();
        List<QueryInputMetadata> inputs = queryIOMetadata.getInputs();

        return Optional.of(new GenericRecordBuilder(eventSchema)
                .set("event_type", EventType.QUERY_COMPLETED.name())
                .set("query_id", metadata.getQueryId())
                .set("transaction_id", metadata.getTransactionId().orElse(""))
                .set("user", context.getUser())
                .set("principal", context.getPrincipal().orElse(""))
                .set("source", context.getSource().orElse(""))
                .set("server_version", context.getServerVersion())
                .set("environment", context.getEnvironment())
                .set("catalog", context.getCatalog().orElse(""))
                .set("schema", context.getSchema().orElse(""))
                .set("remote_client_address", context.getRemoteClientAddress().orElse(""))
                .set("client_info", context.getClientInfo().orElse(""))
                .set("client_tags", context.getClientTags())
                .set("user_agent", context.getUserAgent().orElse(""))
                .set("resource_estimates", toEstimatesMap(context.getResourceEstimates()))
                .set("query_state", metadata.getQueryState())
                .set("uri", metadata.getUri().toString())
                .set("field_names", inputs.stream().flatMap(input -> input.getColumns().stream()).collect(toList()))
                .set("query", metadata.getQuery())
                .set("peak_user_memory_bytes", statistics.getPeakUserMemoryBytes())
                .set("peak_total_memory_bytes", statistics.getPeakTotalNonRevocableMemoryBytes())
                .set("peak_task_user_memory", statistics.getPeakTaskUserMemory())
                .set("peak_task_total_memory", statistics.getPeakTaskTotalMemory())
                .set("internal_network_bytes", statistics.getInternalNetworkBytes())
                .set("internal_network_rows", statistics.getInternalNetworkRows())
                .set("create_time", event.getCreateTime().atZone(ZONE_ID).toString())
                .set("execution_start_time", event.getExecutionStartTime().atZone(ZONE_ID).toString())
                .set("end_time", event.getEndTime().atZone(ZONE_ID).toString())
                .set("queued_time_ms", statistics.getQueuedTime().toMillis())
                .set("analysis_time_ms", statistics.getAnalysisTime().map(Duration::toMillis).orElse(0L))
                .set("total_split_wall_time_ms", statistics.getWallTime().toMillis())
                .set("total_split_cpu_time_ms", statistics.getCpuTime().toMillis())
                .set("total_data_size_bytes", statistics.getTotalBytes())
                .set("total_rows", statistics.getTotalRows())
                .set("inputs_data_size_bytes", statistics.getPhysicalInputBytes())
                .set("input_rows", statistics.getPhysicalInputRows())
                .set("output_data_size_bytes", statistics.getOutputBytes())
                .set("output_rows", statistics.getOutputRows())
                .set("written_data_size_bytes", statistics.getWrittenBytes())
                .set("written_rows", statistics.getWrittenRows())
                .set("cumulative_memory_bytes", statistics.getCumulativeMemory())
                .set("stage_gc_statistics", STAGE_GC_STATISTICS_CODEC.toJson(statistics.getStageGcStatistics()))
                .set("completed_splits", statistics.getCompletedSplits())
                .set("error_code", failureInfo.map(queryFailureInfo -> queryFailureInfo.getErrorCode().getName()).orElse(""))
                .set("error_category", failureInfo.map(queryFailureInfo -> queryFailureInfo.getErrorCode().getType().name()).orElse(""))
                .set("failure_type", failureInfo.flatMap(queryFailureInfo -> queryFailureInfo.getFailureType()).orElse(""))
                .set("failure_message", failureInfo.flatMap(queryFailureInfo -> queryFailureInfo.getFailureMessage()).orElse(""))
                .set("failure_task", failureInfo.flatMap(queryFailureInfo -> queryFailureInfo.getFailureTask()).orElse(""))
                .set("failure_host", failureInfo.flatMap(queryFailureInfo -> queryFailureInfo.getFailureHost()).orElse(""))
                .set("output_stage_json", event.getIoMetadata().getOutput().flatMap(queryOutputMetadata -> queryOutputMetadata.getConnectorOutputMetadata()).orElse(""))
                .set("failures_json", failureInfo.map(queryFailureInfo -> queryFailureInfo.getFailuresJson()).orElse(""))
                .set("warnings_json", WARNINGS_CODEC.toJson(event.getWarnings()))
                .set("inputs_json", INPUT_METADATA_CODEC.toJson(queryIOMetadata.getInputs()))
                .set("session_properties_json", SESSION_PROPERTIES_CODEC.toJson(context.getSessionProperties()))
                .set("resource_group_name", context.getResourceGroupId().map(resourceGroupId -> LIST_STRING_CODEC.toJson(resourceGroupId.getSegments())).orElse(""))
                .set("cpu_distributions", CPU_DISTRIBUTIONS_CODEC.toJson(statistics.getCpuTimeDistribution()))
                .set("operator_summaries", statistics.getOperatorSummaries())
                .set("plan", metadata.getPlan().orElse(""))
                .set("plan_node_stats", statistics.getPlanNodeStatsAndCosts().orElse(""))
                .set("create_time_epoch_seconds", event.getCreateTime().getEpochSecond())
                .build());
    }

    private static Map<String, Long> toEstimatesMap(ResourceEstimates estimates)
    {
        ImmutableMap.Builder<String, Long> builder = ImmutableMap.builder();
        estimates.getExecutionTime().ifPresent(e -> builder.put("execution_time_ms", e.toMillis()));
        estimates.getCpuTime().ifPresent(e -> builder.put("cpu_time_ms", e.toMillis()));
        estimates.getPeakMemoryBytes().ifPresent(e -> builder.put("peak_memory_bytes", e.longValue()));
        return builder.build();
    }

    @Override
    protected Optional<GenericRecord> normalizeEvent(SplitCompletedEvent event)
    {
        requireNonNull(event, "event is null");
        // Split completion events are not logged at the moment due to space
        return Optional.empty();
    }

    @VisibleForTesting
    enum EventType
    {
        QUERY_CREATED,
        QUERY_COMPLETED,
        SPLIT_COMPLETED
    }
}
