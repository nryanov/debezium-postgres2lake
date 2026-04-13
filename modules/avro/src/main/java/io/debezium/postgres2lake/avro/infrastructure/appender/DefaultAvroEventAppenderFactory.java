package io.debezium.postgres2lake.avro.infrastructure.appender;

import io.debezium.postgres2lake.avro.infrastructure.AvroTableWriter;

public class DefaultAvroEventAppenderFactory implements AvroEventAppenderFactory {
    @Override
    public AvroEventAppender create(AvroTableWriter writer) {
        return new AvroEventAppender(writer);
    }
}
