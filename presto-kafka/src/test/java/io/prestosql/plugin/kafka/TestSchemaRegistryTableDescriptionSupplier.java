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

import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import io.airlift.json.JsonCodec;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.List;

import static io.airlift.json.JsonCodec.listJsonCodec;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestSchemaRegistryTableDescriptionSupplier
{
    private static final JsonCodec<List<String>> LIST_CODEC = listJsonCodec(String.class);

    @Test
    public void testSchemaRegistryUrl()
            throws Exception
    {
        URL schemaRegistryUrl = new URL("http://10.252.27.244:8081");
        HttpUrl subjectsUrl = HttpUrl.get(schemaRegistryUrl).newBuilder().encodedPath("/subjects").build();
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder().url(subjectsUrl).get().build();
        try (Response response = client.newCall(request).execute()) {
            InputStream inputStream = response.body().byteStream();
            String topics = CharStreams.toString(new InputStreamReader(inputStream, UTF_8));

            assertEquals(LIST_CODEC.fromJson(topics),
                    ImmutableList.of("hudi-upsert-issue-value",
                            "hudi-load-test-sequential-prod-value",
                            "hudi-load-test-value",
                            "hudi-load-test-sequential-light-value",
                            "order-central-avro-updates-value",
                            "hudi-load-test-sequential-value"));
        }
        //InputStream inputStream = schemaRegistryUrl.openStream();
    }
}
