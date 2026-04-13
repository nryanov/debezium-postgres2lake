package io.debezium.postgres2lake.orc.infrastructure.appender;

import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;
import io.debezium.postgres2lake.orc.infrastructure.OrcTableWriter;

public class CommitEventEmitterAwareOrcEventAppender extends OrcEventAppender {
    private final CommitEventEmitterHandler handler;

    public CommitEventEmitterAwareOrcEventAppender(OrcTableWriter writer, CommitEventEmitterHandler handler) {
        super(writer);
        this.handler = handler;
    }

    @Override
    public void commitPendingEvents() throws Exception {
        super.commitPendingEvents();
    }
}
