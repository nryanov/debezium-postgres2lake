package io.debezium.postgres2lake.infrastructure.format.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;

public record ParquetTableWriter(
        ParquetWriter<GenericRecord> writer,
        Schema schema,
        String partition
) {
}
