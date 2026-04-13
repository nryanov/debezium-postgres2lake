package io.debezium.postgres2lake.orc.infrastructure.appender;

import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;
import io.debezium.postgres2lake.orc.infrastructure.OrcTableWriter;

public class CommitEventEmmitterAwareOrcEventAppenderFactory implements OrcEventAppenderFactory {
    private final CommitEventEmitterHandler commitEventEmitterHandler;

    public CommitEventEmmitterAwareOrcEventAppenderFactory(CommitEventEmitterHandler commitEventEmitterHandler) {
        this.commitEventEmitterHandler = commitEventEmitterHandler;
    }

    @Override
    public OrcEventAppender create(OrcTableWriter writer) {
        return new CommitEventEmitterAwareOrcEventAppender(writer, commitEventEmitterHandler);
    }
}
