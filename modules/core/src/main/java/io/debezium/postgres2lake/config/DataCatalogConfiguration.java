package io.debezium.postgres2lake.config;

import io.smallrye.config.ConfigMapping;

import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "debezium.extensions.data-catalog")
public interface DataCatalogConfiguration {
    Optional<String> name();

    Map<String, String> properties();
}
