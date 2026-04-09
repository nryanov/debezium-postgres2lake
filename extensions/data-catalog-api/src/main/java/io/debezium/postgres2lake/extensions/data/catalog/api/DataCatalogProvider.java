package io.debezium.postgres2lake.extensions.data.catalog.api;

/**
 * SPI service: produces a {@link DataCatalogHandler}. Register implementations under
 * {@code META-INF/services/io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogProvider}.
 * <p>
 * Implementations must expose a public no-arg constructor (required by {@link java.util.ServiceLoader}).
 */
public interface DataCatalogProvider {

    DataCatalogHandler create();
}
