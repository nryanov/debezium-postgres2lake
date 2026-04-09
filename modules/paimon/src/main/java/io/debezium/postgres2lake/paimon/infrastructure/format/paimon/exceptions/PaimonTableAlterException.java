package io.debezium.postgres2lake.paimon.infrastructure.format.paimon.exceptions;

public class PaimonTableAlterException extends RuntimeException {
    public PaimonTableAlterException(Throwable cause) {
        super(cause);
    }
}
