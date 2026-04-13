package io.debezium.postgres2lake.avro.infrastructure.appender;

import io.debezium.postgres2lake.avro.infrastructure.AvroTableWriter;
import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;
import io.debezium.postgres2lake.extensions.common.model.TableDestination;

public class CommitEventEmitterAwareAvroEventAppender extends AvroEventAppender {
    private final CommitEventEmitterHandler handler;
    private final AvroTableWriter writer;

    public CommitEventEmitterAwareAvroEventAppender(AvroTableWriter writer, CommitEventEmitterHandler handler) {
        super(writer);

        this.writer = writer;
        this.handler = handler;
    }

    @Override
    public void commitPendingEvents() throws Exception {
        super.commitPendingEvents();

        var destination = new TableDestination(
                writer.destination().database(),
                writer.destination().schema(),
                writer.destination().table()
        );

        handler.emit(destination, writer.file());
    }
}
