package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.config.PaimonConfiguration;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.NoOpDataCatalogHandler;
import io.debezium.postgres2lake.infrastructure.format.paimon.PaimonSchemaConverter;
import io.debezium.postgres2lake.infrastructure.s3.S3PaimonEventSaver;
import io.debezium.postgres2lake.infrastructure.schema.CachedSchemaConverter;
import io.debezium.postgres2lake.infrastructure.schema.DataCatalogAwareSchemaConverter;
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

        return new S3PaimonEventSaver(configuration, schemaConverter);
    }
}
