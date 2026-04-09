package io.debezium.postgres2lake.core.service.exceptions;

public abstract class EventSaverException extends RuntimeException {

    protected EventSaverException(String message, Throwable cause) {
        super(message, cause);
    }
}
