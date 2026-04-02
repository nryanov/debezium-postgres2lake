package io.debezium.postgres2lake.infrastructure.format.iceberg;

import io.debezium.postgres2lake.domain.model.TableWriter;
import org.apache.avro.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;

public record IcebergTableWriter(
        Table table,
        TaskWriter<Record> writer,
        org.apache.iceberg.Schema icebergSchema,
        Schema schema
) implements TableWriter {
    @Override
    public String partition() {
        // iceberg resolve partition in tableIo
        return "";
    }
}
