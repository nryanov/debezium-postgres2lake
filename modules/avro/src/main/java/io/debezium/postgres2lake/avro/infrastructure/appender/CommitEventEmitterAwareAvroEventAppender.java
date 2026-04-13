package io.debezium.postgres2lake.avro.infrastructure.appender;

import io.debezium.postgres2lake.avro.infrastructure.AvroTableWriter;
import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;

public class CommitEventEmitterAwareAvroEventAppender extends AvroEventAppender {
    private final CommitEventEmitterHandler handler;

    public CommitEventEmitterAwareAvroEventAppender(AvroTableWriter writer, CommitEventEmitterHandler handler) {
        super(writer);
        this.handler = handler;
    }

    @Override
    public void commitPendingEvents() throws Exception {
        super.commitPendingEvents();
    }
}
