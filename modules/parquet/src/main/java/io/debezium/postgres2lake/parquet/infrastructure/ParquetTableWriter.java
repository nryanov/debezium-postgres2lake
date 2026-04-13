package io.debezium.postgres2lake.parquet.infrastructure;

import io.debezium.postgres2lake.domain.model.EventDestination;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;

public record ParquetTableWriter(
        ParquetWriter<GenericRecord> writer,
        Schema schema,
        String partition,
        String file,
        EventDestination destination
) {
}
