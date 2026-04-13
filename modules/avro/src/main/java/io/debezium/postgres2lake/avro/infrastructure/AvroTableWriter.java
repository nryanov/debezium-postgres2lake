package io.debezium.postgres2lake.avro.infrastructure;

import io.debezium.postgres2lake.domain.model.EventDestination;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;

public record AvroTableWriter(
        DataFileWriter<GenericRecord> writer,
        Schema schema,
        String partition,
        String file,
        EventDestination destination
) {}
