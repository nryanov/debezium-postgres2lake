package io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api;

import io.debezium.postgres2lake.extensions.common.SpiProvider;

public interface ReadinessMarkerEventEmitterProvider extends SpiProvider<ReadinessMarkerEventEmitterHandler> {
}
