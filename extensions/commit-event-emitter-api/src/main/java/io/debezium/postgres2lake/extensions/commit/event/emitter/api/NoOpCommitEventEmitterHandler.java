package io.debezium.postgres2lake.extensions.commit.event.emitter.api;

import io.debezium.postgres2lake.extensions.common.model.TableDestination;

import java.util.Map;

public class NoOpCommitEventEmitterHandler implements CommitEventEmitterHandler {
    public static final NoOpCommitEventEmitterHandler INSTANCE = new NoOpCommitEventEmitterHandler();

    @Override
    public void initialize(Map<String, String> properties) {
        // no-op
    }

    @Override
    public void emit(TableDestination destination, String file) {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }
}
