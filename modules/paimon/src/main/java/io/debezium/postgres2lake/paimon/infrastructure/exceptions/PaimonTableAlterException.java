package io.debezium.postgres2lake.paimon.infrastructure.exceptions;

public class PaimonTableAlterException extends RuntimeException {
    public PaimonTableAlterException(Throwable cause) {
        super(cause);
    }
}
