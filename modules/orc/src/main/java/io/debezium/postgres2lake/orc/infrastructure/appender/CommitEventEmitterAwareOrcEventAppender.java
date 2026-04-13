package io.debezium.postgres2lake.orc.infrastructure.appender;

import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;
import io.debezium.postgres2lake.extensions.common.model.TableDestination;
import io.debezium.postgres2lake.orc.infrastructure.OrcTableWriter;

public class CommitEventEmitterAwareOrcEventAppender extends OrcEventAppender {
    private final CommitEventEmitterHandler handler;
    private final OrcTableWriter writer;

    public CommitEventEmitterAwareOrcEventAppender(OrcTableWriter writer, CommitEventEmitterHandler handler) {
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
