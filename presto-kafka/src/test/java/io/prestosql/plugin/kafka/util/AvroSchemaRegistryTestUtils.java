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
package io.prestosql.plugin.kafka.util;

import com.google.common.io.ByteStreams;
import io.airlift.json.JsonCodec;
import io.prestosql.metadata.Metadata;
import io.prestosql.plugin.kafka.KafkaTopicDescription;
import io.prestosql.plugin.kafka.KafkaTopicFieldGroup;
import io.prestosql.spi.connector.SchemaTableName;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.io.MoreFiles.createParentDirectories;
import static java.util.function.Function.identity;

public class AvroSchemaRegistryTestUtils
{
    public static final String DEFAULT_SCHEMA = "default";

    private AvroSchemaRegistryTestUtils() {}

    public static Map<SchemaTableName, KafkaTopicDescription> createTopicDescriptions(Metadata metadata, String resourcePath, Path destinationPath)
            throws Exception
    {
        JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec = new CodecSupplier<>(KafkaTopicDescription.class, metadata).get();
        return Files.list(Paths.get(AvroSchemaRegistryTestUtils.class.getResource(resourcePath).getFile()))
                .filter(path -> path.getFileName().toString().endsWith(".json"))
                .map(path -> getTopicDescriptionFromPath(topicDescriptionJsonCodec, path, destinationPath))
                .collect(toImmutableMap(topicDescription -> new SchemaTableName(topicDescription.getSchemaName().orElse(DEFAULT_SCHEMA), topicDescription.getTableName()), identity()));
    }

    private static KafkaTopicDescription getTopicDescriptionFromPath(JsonCodec<KafkaTopicDescription> topicDescriptionJsonCodec, Path sourcePath, Path destinationPath)
    {
        try {
            KafkaTopicDescription topicDescription = topicDescriptionJsonCodec.fromJson(ByteStreams.toByteArray(Files.newInputStream(sourcePath)));
            Optional<KafkaTopicFieldGroup> key = copyFieldGroup(topicDescription, true, destinationPath);
            Optional<KafkaTopicFieldGroup> message = copyFieldGroup(topicDescription, false, destinationPath);

            topicDescription = new KafkaTopicDescription(topicDescription.getTableName(),
                    topicDescription.getSchemaName(),
                    topicDescription.getTopicName(),
                    key,
                    message);
            return topicDescription;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Optional<KafkaTopicFieldGroup> copyFieldGroup(KafkaTopicDescription topicDescription, boolean isKey, Path destinationDirectory)
    {
        Optional<KafkaTopicFieldGroup> fieldGroup = getFieldGroup(topicDescription, isKey);
        Optional<String> dataSchema = fieldGroup.flatMap(KafkaTopicFieldGroup::getDataSchema);
        if (dataSchema.isPresent()) {
            dataSchema = Optional.of(copyResourceToDirectory(dataSchema.get(), destinationDirectory));
            return Optional.of(new KafkaTopicFieldGroup(fieldGroup.get().getDataFormat(), dataSchema, fieldGroup.get().getFields()));
        }
        return fieldGroup;
    }

    private static Optional<KafkaTopicFieldGroup> getFieldGroup(KafkaTopicDescription topicDescription, boolean isKey)
    {
        if (isKey) {
            return topicDescription.getKey();
        }
        return topicDescription.getMessage();
    }

    private static String copyResourceToDirectory(String resourcePath, Path destinationDirectory)
    {
        try {
            Path destinationPath = Paths.get(destinationDirectory.toString(), resourcePath);
            createParentDirectories(destinationPath);
            Files.copy(AvroSchemaRegistryTestUtils.class.getResourceAsStream(resourcePath), destinationPath);
            return destinationPath.toString();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
