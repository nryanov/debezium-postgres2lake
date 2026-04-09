package io.debezium.postgres2lake.core.service.exceptions;

public class EventFlushException extends EventSaverException {

    public EventFlushException(String message, Throwable cause) {
        super(message, cause);
    }
}
