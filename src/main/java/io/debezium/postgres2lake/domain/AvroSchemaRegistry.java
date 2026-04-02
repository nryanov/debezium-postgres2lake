package io.debezium.postgres2lake.domain;

import org.apache.avro.Schema;

public interface AvroSchemaRegistry<T> {
    T convertFromAvro(Schema schema);
}
