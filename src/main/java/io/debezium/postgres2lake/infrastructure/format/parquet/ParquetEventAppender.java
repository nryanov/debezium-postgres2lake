package io.debezium.postgres2lake.infrastructure.format.parquet;

import io.debezium.postgres2lake.domain.EventAppender;
import io.debezium.postgres2lake.domain.model.EventRecord;

import java.io.IOException;

public class ParquetEventAppender implements EventAppender<ParquetTableWriter> {
    @Override
    public void appendEvent(EventRecord event, ParquetTableWriter writer) throws IOException {
        writer.writer().write(event.value());
    }

    @Override
    public void commitPendingEvents(ParquetTableWriter writer) throws Exception {
        writer.writer().close();
    }
}
