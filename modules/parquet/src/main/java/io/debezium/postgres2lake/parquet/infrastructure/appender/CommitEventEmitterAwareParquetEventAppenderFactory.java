package io.debezium.postgres2lake.parquet.infrastructure.appender;

import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;
import io.debezium.postgres2lake.parquet.infrastructure.ParquetTableWriter;

public class CommitEventEmitterAwareParquetEventAppenderFactory implements ParquetEventAppenderFactory {
    private final CommitEventEmitterHandler commitEventEmitterHandler;

    public CommitEventEmitterAwareParquetEventAppenderFactory(CommitEventEmitterHandler commitEventEmitterHandler) {
        this.commitEventEmitterHandler = commitEventEmitterHandler;
    }

    @Override
    public ParquetEventAppender create(ParquetTableWriter writer) {
        return new CommitEventEmitterAwareParquetEventAppender(writer, commitEventEmitterHandler);
    }
}
