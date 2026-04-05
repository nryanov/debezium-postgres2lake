package io.debezium.postgres2lake.infrastructure.format.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;

public record AvroTableWriter(
        DataFileWriter<GenericRecord> writer,
        Schema schema,
        String partition
) {}
