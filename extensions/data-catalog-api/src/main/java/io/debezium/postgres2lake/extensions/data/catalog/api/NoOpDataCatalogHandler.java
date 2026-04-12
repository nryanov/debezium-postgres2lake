package io.debezium.postgres2lake.extensions.data.catalog.api;

import io.debezium.postgres2lake.extensions.common.model.TableDestination;
import io.debezium.postgres2lake.extensions.data.catalog.api.model.TableSchema;

import java.util.Map;

/** Default handler that performs no catalog operations. */
public final class NoOpDataCatalogHandler implements DataCatalogHandler {

    public static final NoOpDataCatalogHandler INSTANCE = new NoOpDataCatalogHandler();

    private NoOpDataCatalogHandler() {
    }

    @Override
    public void initialize(Map<String, String> properties) {
        // no-op
    }

    @Override
    public void createOrUpdateTable(TableDestination destination, TableSchema schema) {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }
}
