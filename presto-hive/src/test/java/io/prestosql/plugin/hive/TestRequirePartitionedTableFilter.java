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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.plugin.hive.HiveQueryRunner.createQueryRunner;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true, groups = "test-css")
public class TestRequirePartitionedTableFilter
{
    @Test
    public void testPartitionedTableFilterRequired()
    {
        // Query assertions is not available here so manually verify we get an exception
        try (DistributedQueryRunner queryRunner = createQueryRunner(ImmutableList.of(),
                ImmutableMap.of(),
                "legacy",
                ImmutableMap.of("hive.partitioned-table-filter-required", "true"),
                Optional.empty())) {
            queryRunner.execute("CREATE TABLE foo (col_0 BIGINT, ds VARCHAR, ts VARCHAR) WITH (partitioned_by=ARRAY['ds', 'ts'])");
            queryRunner.execute("SELECT col_0 FROM foo WHERE ds = 'foo'");
        }
        catch (Exception e) {
            assertTrue(e.getMessage().startsWith("Must supply a filter for every partition key in partitioned table"));
            return;
        }
        fail("Expected exception: Must supply a filter for partitioned table");
    }

    @Test
    public void testPartitionedTableFilterRequiredWithJoin()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(ImmutableList.of(),
                ImmutableMap.of(),
                "legacy",
                ImmutableMap.of("hive.partitioned-table-filter-required", "true"),
                Optional.empty())) {
            queryRunner.execute("CREATE TABLE foo (col_0 BIGINT, ds VARCHAR, ts VARCHAR) WITH (partitioned_by=ARRAY['ds', 'ts'])");
            queryRunner.execute("CREATE TABLE bar (col_0 BIGINT, ds VARCHAR, ts VARCHAR) WITH (partitioned_by=ARRAY['ds', 'ts'])");
            queryRunner.execute("SELECT a.col_0 FROM foo a JOIN bar b ON a.ds = b.ds and a.ts = b.ts WHERE a.ds = 'ds' and a.ts = 'ts'");
        }
    }

    @Test
    public void testPartitionedTableFilterNotRequired()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner(ImmutableList.of(),
                ImmutableMap.of(),
                "legacy",
                ImmutableMap.of("hive.partitioned-table-filter-required", "false"),
                Optional.empty())) {
            queryRunner.execute("CREATE TABLE foo (col_0 BIGINT, ds VARCHAR) WITH (partitioned_by=ARRAY['ds'])");
            queryRunner.execute("INSERT INTO foo VALUES(1, '2019-01-01')");
            MaterializedResult result = queryRunner.execute("SELECT col_0 FROM foo");
            assertEquals(result.getRowCount(), 1);
        }
    }
}
