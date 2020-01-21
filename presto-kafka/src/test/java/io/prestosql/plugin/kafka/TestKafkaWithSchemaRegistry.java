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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.kafka.util.TestingKafkaWithSchemaRegistry;
import io.prestosql.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;

import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.prestosql.plugin.kafka.KafkaQueryRunner.createKafkaQueryRunner;

@Test(singleThreaded = true)
public class TestKafkaWithSchemaRegistry
        extends AbstractTestKafkaWithSchemaRegistry
{
    private TestingKafkaWithSchemaRegistry testingKafkaWithSchemaRegistry;
    private File temporaryDirectory;

    @Override
    protected String getSchemaTableName()
    {
        return "default.foo";
    }

    @Override
    protected String getSchemaRegistryTableName()
    {
        return "default.bar";
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        temporaryDirectory = createTempDir();
        testingKafkaWithSchemaRegistry = new TestingKafkaWithSchemaRegistry();
        QueryRunner queryRunner = createKafkaQueryRunner(testingKafkaWithSchemaRegistry, ImmutableMap.of("http-server.http.port", "8080"), ImmutableMap.of("kafka.hide-internal-columns", "false"), "/avro", temporaryDirectory.toPath());
        produceRecords(testingKafkaWithSchemaRegistry);
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void stopKafkaWithSchemaRegistry()
            throws Exception
    {
        try {
            testingKafkaWithSchemaRegistry.close();
            testingKafkaWithSchemaRegistry = null;
        }
        finally {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }
    }
}
