package io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.s3;

import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterProvider;

public class S3ReadinessMarkerEventEmitterProvider implements ReadinessMarkerEventEmitterProvider {
    @Override
    public ReadinessMarkerEventEmitterHandler create() {
        return new S3ReadinessMarkerEventEmitterHandler();
    }
}
