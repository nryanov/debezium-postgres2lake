package io.debezium.postgres2lake.infrastructure.schema.exceptions;

import org.apache.avro.Schema;

public class IncompatibleChangeToRequiredException extends RuntimeException {
    public IncompatibleChangeToRequiredException(Schema.Field field) {
        super(String.format("Attempt to make optional field %s required", field.name()));
    }
}
