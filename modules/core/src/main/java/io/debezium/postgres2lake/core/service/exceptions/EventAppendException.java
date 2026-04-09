package io.debezium.postgres2lake.core.service.exceptions;

public class EventAppendException extends EventSaverException {

    public EventAppendException(String message, Throwable cause) {
        super(message, cause);
    }
}
