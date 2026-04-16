package io.debezium.postgres2lake.core.infrastructure.exceptions;

public class WriterOpenException extends StorageException {

    public WriterOpenException(String message, Throwable cause) {
        super(message, cause);
    }
}
