package io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api;

import io.debezium.postgres2lake.extensions.common.SpiHandler;
import io.debezium.postgres2lake.extensions.common.model.TableDestination;

import java.util.List;

public interface ReadinessMarkerEventEmitterHandler extends SpiHandler {
    void emit(List<TableDestination> destinations);
}
