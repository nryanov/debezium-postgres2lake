package io.debezium.postgres2lake.infrastructure.debezium.exceptions;

public class UnknownOperationException extends RuntimeException {
    public UnknownOperationException(String op) {
        super(String.format("Unknown operation: %s", op));
    }
}
