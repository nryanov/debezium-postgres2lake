package io.debezium.postgres2lake.infrastructure.s3.exceptions;

import io.debezium.postgres2lake.service.exceptions.EventSaverException;

public abstract class S3StorageException extends EventSaverException {

    protected S3StorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
