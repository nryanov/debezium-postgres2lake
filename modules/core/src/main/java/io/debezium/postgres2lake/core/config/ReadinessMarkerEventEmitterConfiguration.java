package io.debezium.postgres2lake.core.config;

import io.smallrye.config.ConfigMapping;

import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "debezium.extensions.readiness-marker-event-emitter")
public interface ReadinessMarkerEventEmitterConfiguration {
    Optional<String> name();

    Map<String, String> properties();
}
