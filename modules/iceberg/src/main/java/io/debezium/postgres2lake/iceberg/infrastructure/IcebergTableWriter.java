package io.debezium.postgres2lake.iceberg.infrastructure;

import io.debezium.postgres2lake.domain.model.EventDestination;
import org.apache.avro.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;

public record IcebergTableWriter(
        Table table,
        EventDestination destination,
        TaskWriter<Record> writer,
        org.apache.iceberg.Schema icebergSchema,
        Schema schema
) {
}
