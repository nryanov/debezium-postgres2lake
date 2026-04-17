package io.debezium.postgres2lake.core.infrastructure.exceptions;

import io.debezium.postgres2lake.core.service.exceptions.EventSaverException;

public abstract class StorageException extends EventSaverException {

    protected StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
