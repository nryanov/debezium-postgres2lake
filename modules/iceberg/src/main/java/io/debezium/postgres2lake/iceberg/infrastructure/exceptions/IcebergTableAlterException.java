package io.debezium.postgres2lake.iceberg.infrastructure.exceptions;

public class IcebergTableAlterException extends RuntimeException {
    public IcebergTableAlterException(Throwable cause) {
        super(cause);
    }
}
