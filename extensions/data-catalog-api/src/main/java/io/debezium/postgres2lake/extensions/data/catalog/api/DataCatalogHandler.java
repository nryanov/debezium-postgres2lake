package io.debezium.postgres2lake.extensions.data.catalog.api;

import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableDestination;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableSchema;

import java.util.Map;

/**
 * Integration with an external data catalog. The host constructs an instance per provider, calls
 * {@link #initialize(Map)} once, then {@link #createOrUpdateTable} whenever a table is created or its schema changes.
 */
public interface DataCatalogHandler {

    void initialize(Map<String, String> properties);

    /**
     * Create or update metadata for the given table so it matches {@code schema}. Implementations should be
     * idempotent (safe to call repeatedly with the same or evolved schema).
     */
    void createOrUpdateTable(TableDestination destination, TableSchema schema);

    /**
     * Close opened resources if any
     */
    void close();
}
