package io.debezium.postgres2lake.core.infrastructure.schema.exceptions;

import org.apache.avro.Schema;

public class NonPrimitiveFieldPromotionException extends RuntimeException {
    public NonPrimitiveFieldPromotionException(Schema existingField, Schema newField) {
        super(String.format("Attempt to promote field from %s to %s", existingField.getType(), newField.getType()));
    }
}