package io.debezium.postgres2lake.infrastructure.schema.exceptions;

import org.apache.avro.Schema;

public class IncompatibleTypePromotion extends RuntimeException {
    public IncompatibleTypePromotion(Schema existingField, Schema newField) {
        super(String.format("Incompatible type promotion from %s to %s", existingField.getType(), newField.getType()));
    }
}
