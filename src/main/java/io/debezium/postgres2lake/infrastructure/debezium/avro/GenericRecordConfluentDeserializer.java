package io.debezium.postgres2lake.infrastructure.debezium.avro;

import io.confluent.connect.avro.AvroConverterConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;

import java.util.Collections;
import java.util.Map;

public class GenericRecordConfluentDeserializer implements GenericRecordDeserializer {
    private final KafkaAvroDeserializer valueDeserializer;
    private final KafkaAvroDeserializer keyDeserializer;

    // configs for key & value are always the same
    public GenericRecordConfluentDeserializer(Map<String, String> configs) {
        var avroConverterConfig = new AvroConverterConfig(configs);
        var client = new CachedSchemaRegistryClient(
                avroConverterConfig.getSchemaRegistryUrls(),
                avroConverterConfig.getMaxSchemasPerSubject(),
                Collections.singletonList(new AvroSchemaProvider()),
                configs,
                avroConverterConfig.requestHeaders()
        );

        this.valueDeserializer = new KafkaAvroDeserializer(client, configs);
        this.keyDeserializer = new KafkaAvroDeserializer(client, configs);
    }

    @Override
    public GenericRecord deserializeValue(String topic, byte[] data) {
        return (GenericRecord) valueDeserializer.deserialize(topic, data);
    }

    @Override
    public GenericRecord deserializeKey(String topic, byte[] data) {
        return (GenericRecord) keyDeserializer.deserialize(topic, data);
    }
}
