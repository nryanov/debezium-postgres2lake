package io.debezium.postgres2lake.iceberg.bootstrap;

import io.debezium.postgres2lake.iceberg.config.IcebergConfiguration;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.NoOpDataCatalogHandler;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;
import io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg.IcebergSchemaConverter;
import io.debezium.postgres2lake.iceberg.infrastructure.s3.S3IcebergEventSaver;
import io.debezium.postgres2lake.core.infrastructure.schema.CachedSchemaConverter;
import io.debezium.postgres2lake.core.infrastructure.schema.DataCatalogAwareSchemaConverter;
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

    @Inject
    ReadinessMarkerEventEmitterHandler readinessMarkerEventEmitterHandler;

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

        return new S3IcebergEventSaver(configuration, schemaConverter, readinessMarkerEventEmitterHandler);
    }
}
