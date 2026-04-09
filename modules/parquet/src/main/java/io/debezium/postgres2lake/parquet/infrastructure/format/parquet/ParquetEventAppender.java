package io.debezium.postgres2lake.parquet.infrastructure.format.parquet;

import io.debezium.postgres2lake.domain.EventAppender;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.avro.Schema;

import java.io.IOException;

public class ParquetEventAppender implements EventAppender {
    private final ParquetTableWriter writer;

    public ParquetEventAppender(ParquetTableWriter writer) {
        this.writer = writer;
    }

    @Override
    public void appendEvent(EventRecord event) throws IOException {
        writer.writer().write(event.value());
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
}
