package io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api;

import io.debezium.postgres2lake.extensions.common.model.TableDestination;

import java.util.List;
import java.util.Map;

public class NoOpReadinessMarkerEventEmitterHandler implements ReadinessMarkerEventEmitterHandler {
    public static final NoOpReadinessMarkerEventEmitterHandler INSTANCE = new NoOpReadinessMarkerEventEmitterHandler();

    @Override
    public void initialize(Map<String, String> properties) {
        // no-op
    }

    @Override
    public void emit(List<TableDestination> destinations) {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }
}
