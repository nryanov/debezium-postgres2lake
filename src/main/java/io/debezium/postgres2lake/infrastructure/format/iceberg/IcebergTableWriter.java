package io.debezium.postgres2lake.infrastructure.format.iceberg;

import io.debezium.postgres2lake.domain.model.PartitionAware;
import io.debezium.postgres2lake.domain.model.SchemaAware;
import org.apache.avro.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.TaskWriter;

public record IcebergTableWriter(
        Table table,
        TaskWriter<Record> writer,
        org.apache.iceberg.Schema icebergSchema,
        Schema schema
) implements SchemaAware, PartitionAware {
    @Override
    public String partition() {
        // iceberg resolve partition in tableIo
        return "";
    }
}
