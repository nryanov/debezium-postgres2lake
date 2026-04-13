package io.debezium.postgres2lake.extensions.commit.event.emitter.api;

import io.debezium.postgres2lake.extensions.common.SpiHandler;
import io.debezium.postgres2lake.extensions.common.model.TableDestination;

public interface CommitEventEmitterHandler extends SpiHandler {
    void emit(TableDestination destination, String file);
}
