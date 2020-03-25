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
import io.prestosql.plugin.kafka.decoder.DispatchingSchemaReader;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.kafka.KafkaErrorCode.KAFKA_SPLIT_ERROR;
import static io.prestosql.plugin.kafka.KafkaInternalFieldDescription.PARTITION_ID_FIELD;
import static io.prestosql.plugin.kafka.KafkaInternalFieldDescription.PARTITION_OFFSET_FIELD;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class KafkaSplitManager
        implements ConnectorSplitManager
{
    private final KafkaConsumerFactory consumerFactory;
    private final int messagesPerSplit;
    private final DispatchingSchemaReader dispatchingSchemaReader;

    @Inject
    public KafkaSplitManager(KafkaConsumerFactory consumerFactory, KafkaConfig kafkaConfig, DispatchingSchemaReader dispatchingSchemaReader)
    {
        this.consumerFactory = requireNonNull(consumerFactory, "consumerManager is null");
        messagesPerSplit = requireNonNull(kafkaConfig, "kafkaConfig is null").getMessagesPerSplit();
        this.dispatchingSchemaReader = requireNonNull(dispatchingSchemaReader, "dispatchingSchemaReader is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorTableHandle table, SplitSchedulingStrategy splitSchedulingStrategy)
    {
        KafkaTableHandle kafkaTableHandle = (KafkaTableHandle) table;
        try (KafkaConsumer<byte[], byte[]> kafkaConsumer = consumerFactory.create()) {
            List<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor(kafkaTableHandle.getTopicName());

            List<TopicPartition> topicPartitions = partitionInfos.stream()
                    .map(KafkaSplitManager::toTopicPartition)
                    .collect(toImmutableList());

            Map<TopicPartition, Long> partitionBeginOffsets = kafkaConsumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> partitionEndOffsets = kafkaConsumer.endOffsets(topicPartitions);

            ImmutableList.Builder<KafkaSplit> splits = ImmutableList.builder();
            Optional<String> keyDataSchemaContents = readSchema(kafkaTableHandle, true);
            Optional<String> messageDataSchemaContents = readSchema(kafkaTableHandle, false);
            partitionInfos = applyPartitionFilter(kafkaTableHandle.getConstraint(), partitionInfos);
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition topicPartition = toTopicPartition(partitionInfo);
                HostAddress leader = HostAddress.fromParts(partitionInfo.leader().host(), partitionInfo.leader().port());

                List<Range> ranges = applyOffsetsFilter(kafkaTableHandle.getConstraint(), new Range(partitionBeginOffsets.get(topicPartition), partitionEndOffsets.get(topicPartition)));
                for (Range offsetRange : ranges) {
                    for (Range range : offsetRange.partition(messagesPerSplit)) {
                        splits.add(new KafkaSplit(
                                kafkaTableHandle.getTopicName(),
                                kafkaTableHandle.getKeyDataFormat(),
                                kafkaTableHandle.getMessageDataFormat(),
                                keyDataSchemaContents,
                                messageDataSchemaContents,
                                partitionInfo.partition(),
                                range,
                                leader));
                    }
                }
            }
            return new FixedSplitSource(splits.build());
        }
        catch (Exception e) { // Catch all exceptions because Kafka library is written in scala and checked exceptions are not declared in method signature.
            if (e instanceof PrestoException) {
                throw e;
            }
            throw new PrestoException(KAFKA_SPLIT_ERROR, format("Cannot list splits for table '%s' reading topic '%s'", kafkaTableHandle.getTableName(), kafkaTableHandle.getTopicName()), e);
        }
    }

    private List<PartitionInfo> applyPartitionFilter(TupleDomain<ColumnHandle> tupleDomain, List<PartitionInfo> partitionInfos)
    {
        Domain partitionDomain = Domain.all(BIGINT);
        if (!tupleDomain.equals(TupleDomain.all()) && tupleDomain.getDomains().isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
                KafkaColumnHandle columnHandle = (KafkaColumnHandle) entry.getKey();
                if (columnHandle.isInternal() && columnHandle.getName().equals(PARTITION_ID_FIELD.getColumnName())) {
                    Domain domain = entry.getValue();
                    if (domain != null) {
                        partitionDomain = partitionDomain.intersect(domain);
                    }
                }
            }
            Domain finalPartitionDomain = partitionDomain;
            partitionInfos = partitionInfos.stream()
                    .filter(partitionInfo -> finalPartitionDomain.contains(Domain.create(ValueSet.of(BIGINT, (long) partitionInfo.partition()), false))).collect(toImmutableList());
        }
        return partitionInfos;
    }

    private List<Range> applyOffsetsFilter(TupleDomain<ColumnHandle> tupleDomain, Range offsets)
    {
        Domain offsetsDomain = Domain.all(BIGINT);
        if (!tupleDomain.equals(TupleDomain.all()) && tupleDomain.getDomains().isPresent()) {
            for (Map.Entry<ColumnHandle, Domain> entry : tupleDomain.getDomains().get().entrySet()) {
                KafkaColumnHandle columnHandle = (KafkaColumnHandle) entry.getKey();
                if (columnHandle.isInternal() && columnHandle.getName().equals(PARTITION_OFFSET_FIELD.getColumnName())) {
                    Domain domain = entry.getValue();
                    if (domain != null) {
                        offsetsDomain = offsetsDomain.intersect(domain);
                    }
                }
            }
            offsetsDomain = normalizeDomain(offsetsDomain);
            return offsetsDomain.intersect(toDomain(offsets))
                    .getValues()
                    .getRanges()
                    .getOrderedRanges().stream().map(this::toRange)
                    .collect(toImmutableList());
        }
        else {
            return ImmutableList.of(offsets);
        }
    }

    // For an empty partition return Domain.none
    private static Domain toDomain(Range range)
    {
        if (range.getBegin() >= range.getEnd()) {
            return Domain.none(BIGINT);
        }
        return Domain.create(
                ValueSet.ofRanges(
                        io.prestosql.spi.predicate.Range.range(BIGINT, range.getBegin(), true, range.getEnd(), false)), false);
    }

    // Transform discrete value domains into range sets
    // This will allow adjacent values to be combined into one range
    private Domain normalizeDomain(Domain domain)
    {
        requireNonNull(domain, "domain is null");
        if (domain.getValues().isDiscreteSet()) {
            long firstValue = (long) domain.getValues().getRanges().getOrderedRanges().get(0).getSingleValue();
            io.prestosql.spi.predicate.Range first = io.prestosql.spi.predicate.Range.range(BIGINT, firstValue, true, firstValue + 1, false);
            io.prestosql.spi.predicate.Range[] rest = domain.getValues().getRanges().getOrderedRanges().stream()
                    .skip(1)
                    .map(range -> io.prestosql.spi.predicate.Range.range(BIGINT, range.getSingleValue(), true, (long) range.getSingleValue() + 1L, false))
                    .toArray(io.prestosql.spi.predicate.Range[]::new);
            return Domain.create(ValueSet.ofRanges(first, rest), false);
        }
        return domain;
    }

    private Range toRange(io.prestosql.spi.predicate.Range range)
    {
        long low = 0L;
        if (!range.getLow().isLowerUnbounded()) {
            switch (range.getLow().getBound()) {
                case EXACTLY:
                    low = (long) range.getLow().getValue();
                    break;
                case ABOVE:
                    low = (long) range.getLow().getValue() + 1;
                    break;
                default:
                    throw new IllegalStateException("Unexpected range for lower bound");
            }
        }
        long high = Long.MAX_VALUE;
        if (!range.getHigh().isUpperUnbounded()) {
            switch (range.getHigh().getBound()) {
                case EXACTLY:
                    high = (long) range.getHigh().getValue() + 1;
                    break;
                case BELOW:
                    high = (long) range.getHigh().getValue();
                    break;
                default:
                    throw new IllegalStateException("Unexpected range for upper bound");
            }
        }
        return new Range(low, high);
    }

    private String getDataFormat(KafkaTableHandle kafkaTableHandle, boolean isKey)
    {
        if (isKey) {
            return kafkaTableHandle.getKeyDataFormat();
        }
        return kafkaTableHandle.getMessageDataFormat();
    }

    private Optional<String> getSchemaLocation(KafkaTableHandle kafkaTableHandle, boolean isKey)
    {
        if (isKey) {
            return kafkaTableHandle.getKeyDataSchemaLocation();
        }
        return kafkaTableHandle.getMessageDataSchemaLocation();
    }

    private Optional<String> readSchema(KafkaTableHandle kafkaTableHandle, boolean isKey)
    {
        return dispatchingSchemaReader.getSchemaReader(getDataFormat(kafkaTableHandle, isKey))
                .readSchema(kafkaTableHandle.getTopicName(), getSchemaLocation(kafkaTableHandle, isKey), isKey);
    }

    private static TopicPartition toTopicPartition(PartitionInfo partitionInfo)
    {
        return new TopicPartition(partitionInfo.topic(), partitionInfo.partition());
    }
}
