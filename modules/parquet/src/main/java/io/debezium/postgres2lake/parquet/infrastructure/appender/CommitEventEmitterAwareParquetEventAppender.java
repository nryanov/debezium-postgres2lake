package io.debezium.postgres2lake.parquet.infrastructure.appender;

import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;
import io.debezium.postgres2lake.parquet.infrastructure.ParquetTableWriter;

public class CommitEventEmitterAwareParquetEventAppender extends ParquetEventAppender {
    private final CommitEventEmitterHandler handler;

    public CommitEventEmitterAwareParquetEventAppender(ParquetTableWriter writer, CommitEventEmitterHandler handler) {
        super(writer);
        this.handler = handler;
    }

    @Override
    public void commitPendingEvents() throws Exception {
        super.commitPendingEvents();
    }
}
