package io.debezium.postgres2lake.engine;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public record EventRecord(GenericRecord key, GenericRecord value, String destination) {
    public Schema flattenSchema() {
        return value.getSchema().getField("after").schema().getTypes().get(1);
    }
}
