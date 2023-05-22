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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.pinot.client.PinotHostMapper;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.kafka.TestingKafka;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.plugin.pinot.BasePinotConnectorSmokeTest.schemaRegistryAwareProducer;
import static io.trino.plugin.pinot.PinotQueryRunner.createPinotQueryRunner;
import static io.trino.plugin.pinot.TestingPinotCluster.PINOT_LATEST_IMAGE_NAME;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPinotConnectorTest
        extends BaseConnectorTest
{
    // Use a recent value for updated_at to ensure Pinot doesn't clean up records older than retentionTimeValue as defined in the table specs
    private static final Instant initialUpdatedAt = Instant.now().minus(Duration.ofDays(1)).truncatedTo(SECONDS);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingKafka kafka = closeAfterClass(TestingKafka.createWithSchemaRegistry());
        kafka.start();
        TestingPinotCluster pinot = closeAfterClass(new TestingPinotCluster(kafka.getNetwork(), isSecured(), getPinotImageName()));
        pinot.start();

        DistributedQueryRunner queryRunner = createPinotQueryRunner(
                ImmutableMap.of(),
                pinotProperties(pinot),
                Optional.of(binder -> newOptionalBinder(binder, PinotHostMapper.class).setBinding()
                        .toInstance(new TestingPinotHostMapper(pinot.getBrokerHostAndPort(), pinot.getServerHostAndPort(), pinot.getServerGrpcHostAndPort()))));

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        // We need the query runner to populate nation and region data from tpch schema
        createAndPopulateNationAndRegionData(kafka, pinot, queryRunner);

        return queryRunner;
    }

    private Map<String, String> pinotProperties(TestingPinotCluster pinot)
    {
        return ImmutableMap.<String, String>builder()
                .put("pinot.controller-urls", pinot.getControllerConnectString())
                //.put("pinot.max-rows-per-split-for-segment-queries", String.valueOf(MAX_ROWS_PER_SPLIT_FOR_SEGMENT_QUERIES))
                //.put("pinot.max-rows-for-broker-queries", String.valueOf(MAX_ROWS_PER_SPLIT_FOR_BROKER_QUERIES))
                .putAll(additionalPinotProperties())
                .buildOrThrow();
    }

    protected Map<String, String> additionalPinotProperties()
    {
        if (isGrpcEnabled()) {
            return ImmutableMap.of("pinot.grpc.enabled", "true");
        }
        return ImmutableMap.of();
    }

    protected boolean isGrpcEnabled()
    {
        return true;
    }

    protected boolean isSecured()
    {
        return false;
    }

    protected String getPinotImageName()
    {
        return PINOT_LATEST_IMAGE_NAME;
    }

    private void createAndPopulateNationAndRegionData(TestingKafka kafka, TestingPinotCluster pinot, DistributedQueryRunner queryRunner)
            throws Exception
    {
        // Create and populate table and topic data
        String regionTableName = "region";
        kafka.createTopicWithConfig(2, 1, regionTableName, false);
        Schema regionSchema = SchemaBuilder.record(regionTableName).fields()
                // regionkey bigint, name varchar, comment varchar
                .name("regionkey").type().longType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("comment").type().stringType().noDefault()
                .name("updated_at_seconds").type().longType().noDefault()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> regionRowsBuilder = ImmutableList.builder();
        MaterializedResult regionRows = queryRunner.execute("SELECT * FROM tpch.tiny.region");
        for (MaterializedRow row : regionRows.getMaterializedRows()) {
            regionRowsBuilder.add(new ProducerRecord<>(regionTableName, "key" + row.getField(0), new GenericRecordBuilder(regionSchema)
                    .set("regionkey", row.getField(0))
                    .set("name", row.getField(1))
                    .set("comment", row.getField(2))
                    .set("updated_at_seconds", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(regionRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("region_schema.json"), regionTableName);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("region_realtimeSpec.json"), regionTableName);

        String nationTableName = "nation";
        kafka.createTopicWithConfig(2, 1, nationTableName, false);
        Schema nationSchema = SchemaBuilder.record(nationTableName).fields()
                // nationkey BIGINT, name VARCHAR,  VARCHAR, regionkey BIGINT
                .name("nationkey").type().longType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("comment").type().stringType().noDefault()
                .name("regionkey").type().longType().noDefault()
                .name("updated_at_seconds").type().longType().noDefault()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> nationRowsBuilder = ImmutableList.builder();
        MaterializedResult nationRows = queryRunner.execute("SELECT * FROM tpch.tiny.nation");
        for (MaterializedRow row : nationRows.getMaterializedRows()) {
            nationRowsBuilder.add(new ProducerRecord<>(nationTableName, "key" + row.getField(0), new GenericRecordBuilder(nationSchema)
                    .set("nationkey", row.getField(0))
                    .set("name", row.getField(1))
                    .set("comment", row.getField(3))
                    .set("regionkey", row.getField(2))
                    .set("updated_at_seconds", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(nationRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("nation_schema.json"), nationTableName);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("nation_realtimeSpec.json"), nationTableName);

        String ordersTableName = "orders";
        kafka.createTopicWithConfig(2, 1, ordersTableName, false);
        Schema ordersSchema = SchemaBuilder.record(ordersTableName).fields()
                // nationkey BIGINT, name VARCHAR,  VARCHAR, regionkey BIGINT
                .name("orderkey").type().longType().noDefault()
                .name("custkey").type().longType().noDefault()
                .name("orderstatus").type().stringType().noDefault()
                .name("totalprice").type().doubleType().noDefault()
                .name("orderdate").type().longType().noDefault()
                .name("orderpriority").type().stringType().noDefault()
                .name("clerk").type().stringType().noDefault()
                .name("shippriority").type().intType().noDefault()
                .name("comment").type().stringType().noDefault()
                .name("updated_at").type().longType().noDefault()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> ordersRowsBuilder = ImmutableList.builder();
        MaterializedResult ordersRows = queryRunner.execute("SELECT * FROM tpch.tiny.orders");
        for (MaterializedRow row : ordersRows.getMaterializedRows()) {
            ordersRowsBuilder.add(new ProducerRecord<>(ordersTableName, "key" + row.getField(0), new GenericRecordBuilder(ordersSchema)
                    .set("orderkey", row.getField(0))
                    .set("custkey", row.getField(1))
                    .set("orderstatus", row.getField(2))
                    .set("totalprice", row.getField(3))
                    .set("orderdate", LocalDate.parse(row.getField(4).toString()).toEpochDay())
                    .set("orderpriority", row.getField(5))
                    .set("clerk", row.getField(6))
                    .set("shippriority", row.getField(7))
                    .set("comment", row.getField(8))
                    .set("updated_at", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(ordersRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("orders_schema.json"), ordersTableName);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("orders_realtimeSpec.json"), ordersTableName);

        String customerTableName = "customer";
        kafka.createTopicWithConfig(2, 1, customerTableName, false);
        Schema customerSchema = SchemaBuilder.record(customerTableName).fields()
                // nationkey BIGINT, name VARCHAR,  VARCHAR, regionkey BIGINT
                .name("custkey").type().longType().noDefault()
                .name("name").type().stringType().noDefault()
                .name("address").type().stringType().noDefault()
                .name("nationkey").type().longType().noDefault()
                .name("phone").type().stringType().noDefault()
                .name("acctbal").type().doubleType().noDefault()
                .name("mktsegment").type().stringType().noDefault()
                .name("comment").type().stringType().noDefault()
                .name("updated_at").type().longType().noDefault()
                .endRecord();
        ImmutableList.Builder<ProducerRecord<String, GenericRecord>> customerRowsBuilder = ImmutableList.builder();
        MaterializedResult customerRows = queryRunner.execute("SELECT * FROM tpch.tiny.customer");
        for (MaterializedRow row : customerRows.getMaterializedRows()) {
            customerRowsBuilder.add(new ProducerRecord<>(customerTableName, "key" + row.getField(0), new GenericRecordBuilder(customerSchema)
                    .set("custkey", row.getField(0))
                    .set("name", row.getField(1))
                    .set("address", row.getField(2))
                    .set("nationkey", row.getField(3))
                    .set("phone", row.getField(4))
                    .set("acctbal", row.getField(5))
                    .set("mktsegment", row.getField(6))
                    .set("comment", row.getField(7))
                    .set("updated_at", initialUpdatedAt.plusMillis(1000).toEpochMilli())
                    .build()));
        }
        kafka.sendMessages(customerRowsBuilder.build().stream(), schemaRegistryAwareProducer(kafka));
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("customer_schema.json"), customerTableName);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("customer_realtimeSpec.json"), customerTableName);
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_INSERT:
            case SUPPORTS_DELETE:
            case SUPPORTS_UPDATE:
            case SUPPORTS_MERGE:
                return false;

            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_CREATE_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_RENAME_TABLE:
                return false;

            case SUPPORTS_ADD_COLUMN:
            case SUPPORTS_RENAME_COLUMN:
            case SUPPORTS_SET_COLUMN_TYPE:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
                return false;

            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected MaterializedResult getDescribeOrdersResult()
    {
        return resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("updated_at_seconds", "bigint", "", "")
                .row("clerk", "varchar", "", "") // String columns are reported only as varchar
                .row("comment", "varchar", "", "")
                .row("custkey", "bigint", "", "") // Long columns are reported as bigint
                .row("orderdate", "date", "", "")
                .row("orderkey", "bigint", "", "")
                .row("orderpriority", "varchar", "", "")
                .row("orderstatus", "varchar", "", "")
                .row("shippriority", "integer", "", "")
                .row("totalprice", "double", "", "")
                .build();
    }

    @Test
    @Override
    public void testShowColumns()
    {
        assertThat(query("SHOW COLUMNS FROM orders")).matches(getDescribeOrdersResult());
    }

    @Test
    @Override
    public void testSelectAll()
    {
        // List columns explicitly, as Druid has an additional __time column
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders");
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();
        String schemaPattern = schema.replaceAll(".$", "_");

        @Language("SQL") String ordersTableWithColumns = "VALUES " +
                "('orders', 'orderkey'), " +
                "('orders', 'custkey'), " +
                "('orders', 'orderstatus'), " +
                "('orders', 'totalprice'), " +
                "('orders', 'orderdate'), " +
                "('orders', 'updated_at_seconds'), " +
                "('orders', 'orderpriority'), " +
                "('orders', 'clerk'), " +
                "('orders', 'shippriority'), " +
                "('orders', 'comment')";

        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
        assertQuery("SELECT table_name FROM information_schema.columns WHERE table_name = 'orders' GROUP BY table_name", "VALUES 'orders'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '_rder_'", ordersTableWithColumns);
        assertQuery(
                "SELECT table_name, column_name FROM information_schema.columns " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '%orders%'",
                ordersTableWithColumns);

        assertQuerySucceeds("SELECT * FROM information_schema.columns");
        assertQuery("SELECT DISTINCT table_name, column_name FROM information_schema.columns WHERE table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "'");
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_name LIKE '%'");
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_catalog = 'something_else'", "SELECT '' WHERE false");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat(computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                .isEqualTo("""
                        CREATE TABLE pinot.default.orders (
                           clerk varchar,
                           orderkey bigint,
                           orderstatus varchar,
                           updated_at_seconds bigint,
                           custkey bigint,
                           totalprice double,
                           comment varchar,
                           orderdate date,
                           orderpriority varchar,
                           shippriority integer
                        )""");
    }

    @Test
    @Override
    public void testPredicateReflectedInExplain()
    {
        // The format of the string representation of what gets shown in the table scan is connector-specific
        // and there's no requirement that the conform to a specific shape or contain certain keywords.

        assertExplain(
                "EXPLAIN SELECT name FROM nation WHERE nationkey = 42",
                "columnName=nationkey", "dataType=bigint", "\\s\\{\\[42\\]\\}");
    }
    /*
    @Test
    public void testSleep()
        throws Exception
    {
        Thread.sleep(Integer.MAX_VALUE);
    }
     */
}
