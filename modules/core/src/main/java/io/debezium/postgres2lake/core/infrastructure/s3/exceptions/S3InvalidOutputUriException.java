package io.debezium.postgres2lake.core.infrastructure.s3.exceptions;

public class S3InvalidOutputUriException extends S3StorageException {

    public S3InvalidOutputUriException(String message, Throwable cause) {
        super(message, cause);
    }
}
