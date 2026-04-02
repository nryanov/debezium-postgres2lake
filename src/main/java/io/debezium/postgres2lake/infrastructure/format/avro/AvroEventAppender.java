package io.debezium.postgres2lake.infrastructure.format.avro;

import io.debezium.postgres2lake.domain.EventAppender;
import io.debezium.postgres2lake.domain.model.EventRecord;

import java.io.IOException;

public class AvroEventAppender implements EventAppender<AvroTableWriter> {
    @Override
    public void appendEvent(EventRecord event, AvroTableWriter writer) throws IOException {
        writer.writer().append(event.value());
    }
}
