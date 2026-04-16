package io.debezium.postgres2lake.paimon.infrastructure.exceptions;

public class PaimonTableAccessException extends RuntimeException {

    public PaimonTableAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
