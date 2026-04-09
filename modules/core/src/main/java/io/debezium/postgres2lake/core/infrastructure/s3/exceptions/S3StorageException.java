package io.debezium.postgres2lake.core.infrastructure.s3.exceptions;

import io.debezium.postgres2lake.core.service.exceptions.EventSaverException;

public abstract class S3StorageException extends EventSaverException {

    protected S3StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
