package io.debezium.postgres2lake.paimon.bootstrap;

import io.debezium.postgres2lake.paimon.config.PaimonConfiguration;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.NoOpDataCatalogHandler;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;
import io.debezium.postgres2lake.paimon.infrastructure.format.paimon.PaimonSchemaConverter;
import io.debezium.postgres2lake.paimon.infrastructure.s3.S3PaimonEventSaver;
import io.debezium.postgres2lake.core.infrastructure.schema.CachedSchemaConverter;
import io.debezium.postgres2lake.core.infrastructure.schema.DataCatalogAwareSchemaConverter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.paimon.schema.Schema;

@ApplicationScoped
public class PaimonBeans {
    @Inject
    PaimonConfiguration configuration;

    @Inject
    DataCatalogHandler dataCatalogHandler;

    @Inject
    ReadinessMarkerEventEmitterHandler readinessMarkerEventEmitterHandler;

    @Singleton
    @Produces
    public EventSaver eventSaver() {
        SchemaConverter<Schema> schemaConverter;
        if (dataCatalogHandler instanceof NoOpDataCatalogHandler) {
            schemaConverter = new CachedSchemaConverter<>(new PaimonSchemaConverter());
        } else {
            schemaConverter = new DataCatalogAwareSchemaConverter<>(
                    new CachedSchemaConverter<>(new PaimonSchemaConverter()),
                    dataCatalogHandler
            );
        }

        return new S3PaimonEventSaver(configuration, schemaConverter, readinessMarkerEventEmitterHandler);
    }
}
