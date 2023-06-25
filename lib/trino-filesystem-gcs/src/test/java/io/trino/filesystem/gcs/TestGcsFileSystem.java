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
package io.trino.filesystem.gcs;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestGcsFileSystem
{
    @Test
    public void test()
    {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Bucket bucket = storage.get("elon-test");
        bucket.list().iterateAll().forEach(System.out::println);
    }

    @Test
    public void testByteBuffer()
    {
        ByteBuffer buffer = ByteBuffer.allocate(1);
        byte value = 3;
        buffer.put(value);
        buffer.position(0);
        buffer.get(0);
        assertEquals(value, buffer.get(0));
        value = 5;
        buffer.put((byte) 5);
        assertEquals(value, buffer.get(0));
    }
}
