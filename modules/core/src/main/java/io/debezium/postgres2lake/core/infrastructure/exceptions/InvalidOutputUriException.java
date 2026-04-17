package io.debezium.postgres2lake.core.infrastructure.exceptions;

public class InvalidOutputUriException extends StorageException {

    public InvalidOutputUriException(String message, Throwable cause) {
        super(message, cause);
    }
}
