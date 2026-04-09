package io.debezium.postgres2lake.core.infrastructure.s3.exceptions;

public class S3PaimonTableAccessException extends S3StorageException {

    public S3PaimonTableAccessException(String message, Throwable cause) {
        super(message, cause);
    }
}
