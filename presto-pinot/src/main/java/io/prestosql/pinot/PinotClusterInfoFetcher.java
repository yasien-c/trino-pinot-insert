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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HttpHeaders;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;
import io.airlift.http.client.StringResponseHandler;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecBinder;
import io.airlift.log.Logger;
import org.apache.pinot.common.data.Schema;

import javax.inject.Inject;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.airlift.http.client.StringResponseHandler.createStringResponseHandler;
import static io.prestosql.pinot.PinotErrorCode.PINOT_INVALID_CONFIGURATION;
import static io.prestosql.pinot.PinotErrorCode.PINOT_UNABLE_TO_FIND_BROKER;
import static io.prestosql.pinot.PinotErrorCode.PINOT_UNEXPECTED_RESPONSE;
import static io.prestosql.pinot.PinotMetrics.isValidPinotHttpResponseCode;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.pinot.common.config.TableNameBuilder.extractRawTableName;

public class PinotClusterInfoFetcher
{
    private static final Logger log = Logger.get(PinotClusterInfoFetcher.class);
    private static final String APPLICATION_JSON = "application/json";
    private static final Pattern BROKER_PATTERN = Pattern.compile("Broker_(.*)_(\\d+)");

    private static final String GET_ALL_TABLES_API_TEMPLATE = "tables";
    private static final String TABLE_INSTANCES_API_TEMPLATE = "tables/%s/instances";
    private static final String TABLE_SCHEMA_API_TEMPLATE = "tables/%s/schema";
    private static final String ROUTING_TABLE_API_TEMPLATE = "debug/routingTable/%s";
    private static final String TIME_BOUNDARY_API_TEMPLATE = "debug/timeBoundary/%s";

    private final List<String> controllerUrls;
    private final PinotMetrics pinotMetrics;
    private final HttpClient httpClient;

    private final Ticker ticker = Ticker.systemTicker();

    private final LoadingCache<String, List<String>> brokersForTableCache;

    private final JsonCodec<GetTables> tablesJsonCodec;
    private final JsonCodec<BrokersForTable> brokersForTableJsonCodec;
    private final JsonCodec<RoutingTables> routingTablesJsonCodec;
    private final JsonCodec<TimeBoundary> timeBoundaryJsonCodec;

    @Inject
    public PinotClusterInfoFetcher(
            PinotConfig pinotConfig,
            PinotMetrics pinotMetrics,
            @ForPinot HttpClient httpClient,
            JsonCodec<GetTables> tablesJsonCodec,
            JsonCodec<BrokersForTable> brokersForTableJsonCodec,
            JsonCodec<RoutingTables> routingTablesJsonCodec,
            JsonCodec<TimeBoundary> timeBoundaryJsonCodec)
    {
        this.brokersForTableJsonCodec = requireNonNull(brokersForTableJsonCodec, "brokers for table json codec is null");
        this.routingTablesJsonCodec = requireNonNull(routingTablesJsonCodec, "routing tables json codec is null");
        this.timeBoundaryJsonCodec = requireNonNull(timeBoundaryJsonCodec, "time boundary json codec is null");
        this.tablesJsonCodec = requireNonNull(tablesJsonCodec, "json codec is null");

        requireNonNull(pinotConfig, "pinotConfig is null");
        if (pinotConfig.getControllerUrls() == null || pinotConfig.getControllerUrls().isEmpty()) {
            throw new PinotException(PINOT_INVALID_CONFIGURATION, Optional.empty(), "No pinot controllers specified");
        }

        this.controllerUrls = pinotConfig.getControllerUrls();
        this.pinotMetrics = requireNonNull(pinotMetrics, "pinotMetrics is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.brokersForTableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(pinotConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
                .build((CacheLoader.from(this::getAllBrokersForTable)));
    }

    public static JsonCodecBinder addJsonBinders(JsonCodecBinder jsonCodecBinder)
    {
        jsonCodecBinder.bindJsonCodec(GetTables.class);
        jsonCodecBinder.bindJsonCodec(BrokersForTable.InstancesInBroker.class);
        jsonCodecBinder.bindJsonCodec(BrokersForTable.class);
        jsonCodecBinder.bindJsonCodec(RoutingTables.class);
        jsonCodecBinder.bindJsonCodec(RoutingTables.RoutingTableSnapshot.class);
        jsonCodecBinder.bindJsonCodec(TimeBoundary.class);
        return jsonCodecBinder;
    }

    public String doHttpActionWithHeaders(Request.Builder requestBuilder, Optional<String> requestBody)
    {
        requestBuilder = requestBuilder
                .setHeader(HttpHeaders.ACCEPT, APPLICATION_JSON);
        if (requestBody.isPresent()) {
            requestBuilder.setHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON);
            requestBuilder.setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(requestBody.get(), StandardCharsets.UTF_8));
        }
        Request request = requestBuilder.build();

        long startTime = ticker.read();
        long duration;
        StringResponseHandler.StringResponse response;
        try {
            response = httpClient.execute(request, createStringResponseHandler());
        }
        finally {
            duration = ticker.read() - startTime;
        }
        pinotMetrics.monitorRequest(request, response, duration, TimeUnit.NANOSECONDS);
        String responseBody = response.getBody();
        if (!isValidPinotHttpResponseCode(response.getStatusCode())) {
            throw new PinotException(
                    PinotErrorCode.PINOT_HTTP_ERROR,
                    Optional.empty(),
                    String.format(
                            "Unexpected response status: %d for request %s to url %s, with headers %s, full response %s",
                            response.getStatusCode(),
                            requestBody.orElse(""),
                            request.getUri(),
                            request.getHeaders(),
                            responseBody));
        }
        return responseBody;
    }

    private String sendHttpGetToController(String path)
    {
        return doHttpActionWithHeaders(
                Request.builder().prepareGet().setUri(URI.create(String.format("http://%s/%s", getControllerUrl(), path))),
                Optional.empty());
    }

    private String sendHttpGetToBroker(String table, String path)
    {
        return doHttpActionWithHeaders(
                Request.builder().prepareGet().setUri(URI.create(String.format("http://%s/%s", getBrokerHost(table), path))),
                Optional.empty());
    }

    private String getControllerUrl()
    {
        return controllerUrls.get(ThreadLocalRandom.current().nextInt(controllerUrls.size()));
    }

    public static class GetTables
    {
        private final List<String> tables;

        @JsonCreator
        public GetTables(@JsonProperty("tables") List<String> tables)
        {
            this.tables = tables;
        }

        public List<String> getTables()
        {
            return tables;
        }
    }

    public List<String> getAllTables()
    {
        return tablesJsonCodec.fromJson(sendHttpGetToController(GET_ALL_TABLES_API_TEMPLATE)).getTables();
    }

    public Schema getTableSchema(String table)
            throws Exception
    {
        String responseBody = sendHttpGetToController(String.format(TABLE_SCHEMA_API_TEMPLATE, table));
        return Schema.fromString(responseBody);
    }

    public static class BrokersForTable
    {
        public static class InstancesInBroker
        {
            private final List<String> instances;

            @JsonCreator
            public InstancesInBroker(@JsonProperty("instances") List<String> instances)
            {
                this.instances = instances;
            }

            @JsonProperty("instances")
            public List<String> getInstances()
            {
                return instances;
            }
        }

        private final List<InstancesInBroker> brokers;

        @JsonCreator
        public BrokersForTable(@JsonProperty("brokers") List<InstancesInBroker> brokers)
        {
            this.brokers = brokers;
        }

        @JsonProperty("brokers")
        public List<InstancesInBroker> getBrokers()
        {
            return brokers;
        }
    }

    @VisibleForTesting
    List<String> getAllBrokersForTable(String table)
    {
        String responseBody = sendHttpGetToController(String.format(TABLE_INSTANCES_API_TEMPLATE, table));
        ArrayList<String> brokers = brokersForTableJsonCodec
                .fromJson(responseBody)
                .getBrokers().stream()
                .flatMap(broker -> broker.getInstances().stream())
                .distinct()
                .map(brokerToParse -> {
                    Matcher matcher = BROKER_PATTERN.matcher(brokerToParse);
                    if (matcher.matches() && matcher.groupCount() == 2) {
                        return matcher.group(1) + ":" + matcher.group(2);
                    }
                    else {
                        throw new PinotException(
                                PINOT_UNABLE_TO_FIND_BROKER,
                                Optional.empty(),
                                String.format("Cannot parse %s in the broker instance", brokerToParse));
                    }
                })
                .collect(Collectors.toCollection(() -> new ArrayList<>()));
        Collections.shuffle(brokers);
        return ImmutableList.copyOf(brokers);
    }

    public String getBrokerHost(String table)
    {
        try {
            List<String> brokers = brokersForTableCache.get(table);
            if (brokers.isEmpty()) {
                throw new PinotException(PINOT_UNABLE_TO_FIND_BROKER, Optional.empty(), "No valid brokers found for " + table);
            }
            return brokers.get(ThreadLocalRandom.current().nextInt(brokers.size()));
        }
        catch (ExecutionException e) {
            Throwable throwable = e.getCause();
            if (throwable instanceof PinotException) {
                throw (PinotException) throwable;
            }
            else {
                throw new PinotException(PINOT_UNABLE_TO_FIND_BROKER, Optional.empty(), "Error when getting brokers for table " + table, throwable);
            }
        }
    }

    public static class RoutingTables
    {
        public static class RoutingTableSnapshot
        {
            private final String tableName;
            private final List<Map<String, List<String>>> routingTableEntries;

            @JsonCreator
            public RoutingTableSnapshot(
                    @JsonProperty("tableName") String tableName,
                    @JsonProperty("routingTableEntries") List<Map<String, List<String>>> routingTableEntries)
            {
                this.tableName = requireNonNull(tableName, "table name is null");
                this.routingTableEntries = requireNonNull(routingTableEntries, "routing table entries is null");
            }

            @JsonProperty("tableName")
            public String getTableName()
            {
                return tableName;
            }

            @JsonProperty("routingTableEntries")
            public List<Map<String, List<String>>> getRoutingTableEntries()
            {
                return routingTableEntries;
            }
        }

        private final List<RoutingTableSnapshot> routingTableSnapshot;

        @JsonCreator
        public RoutingTables(@JsonProperty("routingTableSnapshot") List<RoutingTableSnapshot> routingTableSnapshot)
        {
            this.routingTableSnapshot = routingTableSnapshot;
        }

        public List<RoutingTableSnapshot> getRoutingTableSnapshot()
        {
            return routingTableSnapshot;
        }
    }

    public Map<String, Map<String, List<String>>> getRoutingTableForTable(String tableName)
    {
        ImmutableMap.Builder<String, Map<String, List<String>>> routingTableMap = ImmutableMap.builder();
        log.debug("Trying to get routingTable for %s from broker", tableName);
        String responseBody = sendHttpGetToBroker(tableName, String.format(ROUTING_TABLE_API_TEMPLATE, tableName));
        routingTablesJsonCodec.fromJson(responseBody).getRoutingTableSnapshot().forEach(snapshot -> {
            String tableNameWithType = snapshot.getTableName();
            // Response could contain info for tableName that matches the original table by prefix.
            // e.g. when table name is "table1", response could contain routingTable for "table1_staging"
            if (!tableName.equals(extractRawTableName(tableNameWithType))) {
                log.debug("Ignoring routingTable for %s", tableNameWithType);
            }
            else {
                List<Map<String, List<String>>> routingTableEntriesList = snapshot.getRoutingTableEntries();
                if (routingTableEntriesList.isEmpty()) {
                    throw new PinotException(
                            PINOT_UNEXPECTED_RESPONSE,
                            Optional.empty(),
                            String.format("Empty routingTableEntries for %s. RoutingTable: %s", tableName, responseBody));
                }

                // We are given multiple routing tables for a table, each with different segment to host assignments
                // We pick one randomly, so that a retry may hit a different server
                Map<String, List<String>> routingTableEntries = routingTableEntriesList.get(new Random().nextInt(routingTableEntriesList.size()));
                ImmutableMap.Builder<String, List<String>> routingTableBuilder = ImmutableMap.builder();
                routingTableEntries.forEach((host, segments) -> {
                    List<String> segmentsCopied = new ArrayList<>(segments);
                    Collections.shuffle(segmentsCopied);
                    routingTableBuilder.put(host, ImmutableList.copyOf(segmentsCopied));
                });
                routingTableMap.put(tableNameWithType, routingTableBuilder.build());
            }
        });
        return routingTableMap.build();
    }

    public static class TimeBoundary
    {
        private final Optional<String> onlineTimePredicate;
        private final Optional<String> offlineTimePredicate;

        public TimeBoundary()
        {
            this(null, null);
        }

        @JsonCreator
        public TimeBoundary(
                @JsonProperty String timeColumnName,
                @JsonProperty String timeColumnValue)
        {
            if (timeColumnName != null && timeColumnValue != null) {
                offlineTimePredicate = Optional.of(format("%s < %s", timeColumnName, timeColumnValue));
                onlineTimePredicate = Optional.of(format("%s >= %s", timeColumnName, timeColumnValue));
            }
            else {
                onlineTimePredicate = Optional.empty();
                offlineTimePredicate = Optional.empty();
            }
        }

        public Optional<String> getOnlineTimePredicate()
        {
            return onlineTimePredicate;
        }

        public Optional<String> getOfflineTimePredicate()
        {
            return offlineTimePredicate;
        }
    }

    public TimeBoundary getTimeBoundaryForTable(String table)
    {
        String responseBody = sendHttpGetToBroker(table, String.format(TIME_BOUNDARY_API_TEMPLATE, table));
        return timeBoundaryJsonCodec.fromJson(responseBody);
    }
}
