package io.debezium.postgres2lake.extensions.data.catalog.datahub;

import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogProvider;

/**
 * SPI provider for the DataHub-backed {@link DataCatalogHandler}.
 */
public class DataHubDataCatalogProvider implements DataCatalogProvider {

    public DataHubDataCatalogProvider() {
    }

    @Override
    public DataCatalogHandler create() {
        return new DataHubDataCatalogHandler();
    }
}
