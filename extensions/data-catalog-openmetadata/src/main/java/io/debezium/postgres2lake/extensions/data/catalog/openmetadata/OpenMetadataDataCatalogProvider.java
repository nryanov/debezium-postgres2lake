package io.debezium.postgres2lake.extensions.data.catalog.openmetadata;

import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogProvider;

/**
 * SPI provider for the OpenMetadata-backed {@link DataCatalogHandler}.
 */
public class OpenMetadataDataCatalogProvider implements DataCatalogProvider {

    public OpenMetadataDataCatalogProvider() {
    }

    @Override
    public DataCatalogHandler create() {
        return new OpenMetadataDataCatalogHandler();
    }
}
