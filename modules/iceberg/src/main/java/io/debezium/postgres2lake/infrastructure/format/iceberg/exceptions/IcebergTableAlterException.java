package io.debezium.postgres2lake.infrastructure.format.iceberg.exceptions;

public class IcebergTableAlterException extends RuntimeException {
    public IcebergTableAlterException(Throwable cause) {
        super(cause);
    }
}
