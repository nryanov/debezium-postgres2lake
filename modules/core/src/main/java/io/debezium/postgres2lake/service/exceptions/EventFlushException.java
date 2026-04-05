package io.debezium.postgres2lake.service.exceptions;

public class EventFlushException extends EventSaverException {

    public EventFlushException(String message, Throwable cause) {
        super(message, cause);
    }
}
