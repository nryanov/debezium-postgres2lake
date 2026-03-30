package io.debezium.postgres2lake.infrastructure.debezium.avro;

import org.apache.avro.generic.GenericRecord;

public interface GenericRecordDeserializer {
    GenericRecord deserializeValue(String topic, byte[] data);

    GenericRecord deserializeKey(String topic, byte[] data);
}
