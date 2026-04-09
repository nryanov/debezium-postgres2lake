package io.debezium.postgres2lake.core.infrastructure.s3.exceptions;

public class S3WriterOpenException extends S3StorageException {

    public S3WriterOpenException(String message, Throwable cause) {
        super(message, cause);
    }
}
