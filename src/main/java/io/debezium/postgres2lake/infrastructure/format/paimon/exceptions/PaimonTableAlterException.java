package io.debezium.postgres2lake.infrastructure.format.paimon.exceptions;

public class PaimonTableAlterException extends RuntimeException {
    public PaimonTableAlterException(Throwable cause) {
        super(cause);
    }
}
