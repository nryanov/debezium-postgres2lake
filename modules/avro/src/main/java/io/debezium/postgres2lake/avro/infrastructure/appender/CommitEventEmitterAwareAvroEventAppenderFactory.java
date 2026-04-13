package io.debezium.postgres2lake.avro.infrastructure.appender;

import io.debezium.postgres2lake.avro.infrastructure.AvroTableWriter;
import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;

public class CommitEventEmitterAwareAvroEventAppenderFactory implements AvroEventAppenderFactory {
    private final CommitEventEmitterHandler commitEventEmitterHandler;

    public CommitEventEmitterAwareAvroEventAppenderFactory(CommitEventEmitterHandler commitEventEmitterHandler) {
        this.commitEventEmitterHandler = commitEventEmitterHandler;
    }

    @Override
    public AvroEventAppender create(AvroTableWriter writer) {
        return new CommitEventEmitterAwareAvroEventAppender(writer, commitEventEmitterHandler);
    }
}
