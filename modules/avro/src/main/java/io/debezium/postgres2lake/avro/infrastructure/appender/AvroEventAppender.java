package io.debezium.postgres2lake.avro.infrastructure.appender;

import io.debezium.postgres2lake.avro.infrastructure.AvroTableWriter;
import io.debezium.postgres2lake.domain.EventAppender;
import io.debezium.postgres2lake.domain.model.EventDestination;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.avro.Schema;

import java.io.IOException;

public class AvroEventAppender implements EventAppender {
    private final AvroTableWriter writer;

    public AvroEventAppender(AvroTableWriter writer) {
        this.writer = writer;
    }

    @Override
    public void appendEvent(EventRecord event) throws IOException {
        writer.writer().append(event.value());
    }

    @Override
    public void commitPendingEvents() throws Exception {
        writer.writer().close();
    }

    @Override
    public String currentPartition() {
        return writer.partition();
    }

    @Override
    public Schema currentSchema() {
        return writer.schema();
    }

    @Override
    public EventDestination destination() {
        return writer.destination();
    }
}
