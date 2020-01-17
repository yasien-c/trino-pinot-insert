package io.prestosql.plugin.kafka.util;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.testcontainers.containers.GenericContainer;

import java.util.Properties;

import static io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static java.lang.String.format;

public class TestingKafkaWithSchemaRegistry
        extends TestingKafka
{
    public static final int SCHEMA_REGISTRY_PORT = 8081;
    private final GenericContainer<?> container;

    public TestingKafkaWithSchemaRegistry()
    {
        container = new GenericContainer<>("confluentinc/cp-schema-registry:5.2.1")
                .withNetwork(getNetwork())
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + getInternalConnectString())
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "0.0.0.0")
                .withEnv("SCHEMA_REGISTRY_LISTENERS", format("http://0.0.0.0:%s", SCHEMA_REGISTRY_PORT))
                .withExposedPorts(SCHEMA_REGISTRY_PORT);
    }

    @Override
    public void start()
    {
        super.start();
        try {
            container.start();
        }
        catch (Exception e) {
            super.close();
            throw e;
        }
    }

    @Override
    public void close()
    {
        try {
            super.close();
        }
        finally {
            container.stop();
        }
    }

    public KafkaProducer<String, GenericRecord> createKafkaAvroProducer() {
        Properties properties = new Properties();
        properties.put(SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryConnectString());
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getConnectString());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
        return new KafkaProducer<>(properties);
    }

    public String getSchemaRegistryConnectString() {
        return "http://" + container.getContainerIpAddress() + ":" + container.getMappedPort(SCHEMA_REGISTRY_PORT);
    }
}
