package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.config.IcebergConfiguration;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.NoOpDataCatalogHandler;
import io.debezium.postgres2lake.infrastructure.format.iceberg.IcebergSchemaConverter;
import io.debezium.postgres2lake.infrastructure.s3.S3IcebergEventSaver;
import io.debezium.postgres2lake.infrastructure.schema.CachedSchemaConverter;
import io.debezium.postgres2lake.infrastructure.schema.DataCatalogAwareSchemaConverter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.iceberg.Schema;

@ApplicationScoped
public class IcebergBeans {
    @Inject
    IcebergConfiguration configuration;

    @Inject
    DataCatalogHandler dataCatalogHandler;

    @Singleton
    @Produces
    public EventSaver eventSaver() {
        SchemaConverter<Schema> schemaConverter;
        if (dataCatalogHandler instanceof NoOpDataCatalogHandler) {
            schemaConverter = new CachedSchemaConverter<>(new IcebergSchemaConverter());
        } else {
            schemaConverter = new DataCatalogAwareSchemaConverter<>(
                    new CachedSchemaConverter<>(new IcebergSchemaConverter()),
                    dataCatalogHandler
            );
        }

        return new S3IcebergEventSaver(configuration, schemaConverter);
    }
}
