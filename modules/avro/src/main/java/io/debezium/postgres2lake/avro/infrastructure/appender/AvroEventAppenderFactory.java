package io.debezium.postgres2lake.avro.infrastructure.appender;

import io.debezium.postgres2lake.avro.infrastructure.AvroTableWriter;

public interface AvroEventAppenderFactory {
    AvroEventAppender create(AvroTableWriter writer);
}
