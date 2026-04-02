package io.debezium.postgres2lake.domain.model;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public record EventRecord(
        Operation operation,
        GenericRecord key,
        GenericRecord value,
        String rawDestination,
        EventDestination destination
) {
    public static final String UNWRAPPED_OPERATION_FIELD_NAME = "__operation";
    public static final String UNWRAPPED_IDEMPOTENCY_KEY_FIELD_NAME = "__idempotency_key";
    public static final String UNWRAPPED_EVENT_TIME_FIELD_NAME = "__event_time";

    public EventRecord(Operation operation, GenericRecord key, GenericRecord value, String rawDestination) {
        this(operation, key, value, rawDestination, resolveDestination(rawDestination));
    }

    public long eventTime() {
        return (Long) value.get(UNWRAPPED_EVENT_TIME_FIELD_NAME);
    }

    public Schema keySchema() {
        return key.getSchema();
    }

    public Schema valueSchema() {
        return value.getSchema();
    }

    private static EventDestination resolveDestination(String rawDestination) {
        // always 3 parts: database (topic_prefix), paimonSchema, table
        var parts = rawDestination.split("[.]");

        if (parts.length != 3) {
            throw new IllegalArgumentException("Incorrect rawDestination");
        }

        var database = parts[0];
        var schema = parts[1];
        var table = parts[2];

        return new EventDestination(database, schema, table);
    }
}
