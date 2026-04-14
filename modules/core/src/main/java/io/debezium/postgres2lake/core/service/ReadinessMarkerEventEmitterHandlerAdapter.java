package io.debezium.postgres2lake.core.service;

import io.debezium.postgres2lake.domain.model.EventDestination;
import io.debezium.postgres2lake.extensions.common.model.TableDestination;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.NoOpReadinessMarkerEventEmitterHandler;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;

import java.util.List;

public class ReadinessMarkerEventEmitterHandlerAdapter {
    private final ReadinessMarkerEventEmitterHandler handler;
    private final boolean shouldEmit;

    public ReadinessMarkerEventEmitterHandlerAdapter(ReadinessMarkerEventEmitterHandler handler) {
        this.handler = handler;
        // small optimization to avoid unnecessary conversion from eventDestination to tableDestination
        this.shouldEmit = !(handler instanceof NoOpReadinessMarkerEventEmitterHandler);
    }

    public void emit(List<EventDestination> destinations) {
        if (shouldEmit) {
            var tableDestinations = destinations.stream()
                    .map(it -> new TableDestination(it.database(), it.schema(), it.table()))
                    .toList();

            handler.emit(tableDestinations);
        }
    }
}
