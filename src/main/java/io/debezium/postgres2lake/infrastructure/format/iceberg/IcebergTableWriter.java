package io.debezium.postgres2lake.infrastructure.format.iceberg;

import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;

public record IcebergTableWriter(Table table, TaskWriter<Record> writer) {
}
