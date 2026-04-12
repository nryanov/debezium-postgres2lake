package io.debezium.postgres2lake.extensions.commit.event.emitter.api;

import io.debezium.postgres2lake.extensions.common.SpiProvider;

public interface CommitEventEmitterProvider extends SpiProvider<CommitEventEmitterHandler> {
}
