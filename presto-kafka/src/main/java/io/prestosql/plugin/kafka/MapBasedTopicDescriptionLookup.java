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
import io.prestosql.spi.connector.SchemaTableName;

import java.util.Collection;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class MapBasedTopicDescriptionLookup
        implements TopicDescriptionLookup
{
    private final Map<SchemaTableName, KafkaTopicDescription> topicDescriptions;

    public MapBasedTopicDescriptionLookup(Map<SchemaTableName, KafkaTopicDescription> topicDescriptions)
    {
        requireNonNull(topicDescriptions, "topicDescriptions is null");
        this.topicDescriptions = ImmutableMap.copyOf(topicDescriptions);
    }

    @Override
    public KafkaTopicDescription getTopicDescription(SchemaTableName schemaTableName)
    {
        return topicDescriptions.get(schemaTableName);
    }

    @Override
    public Collection<SchemaTableName> getAllTables()
    {
        return topicDescriptions.keySet();
    }
}
