package io.debezium.postgres2lake.extensions.commit.event.emitter.kafka;

import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;
import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterProvider;

public class KafkaCommitEventEmitterProvider implements CommitEventEmitterProvider {
    @Override
    public CommitEventEmitterHandler create() {
        return new KafkaCommitEventEmitterHandler();
    }
}
