package io.debezium.postgres2lake.orc.bootstrap;

import io.debezium.postgres2lake.orc.config.OrcConfiguration;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.NoOpDataCatalogHandler;
import io.debezium.postgres2lake.orc.infrastructure.format.orc.OrcCompressionCodec;
import io.debezium.postgres2lake.orc.infrastructure.format.orc.OrcSchemaConverter;
import io.debezium.postgres2lake.orc.infrastructure.s3.S3OrcEventSaver;
import io.debezium.postgres2lake.core.bootstrap.OutputLocationGeneratorFactory;
import io.debezium.postgres2lake.core.infrastructure.schema.CachedSchemaConverter;
import io.debezium.postgres2lake.core.infrastructure.schema.DataCatalogAwareSchemaConverter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.orc.TypeDescription;

@ApplicationScoped
public class OrcBeans {
    @Inject
    OrcConfiguration configuration;

    @Inject
    DataCatalogHandler dataCatalogHandler;

    @Singleton
    @Produces
    public EventSaver eventSaver() {
        var locationGenerator = OutputLocationGeneratorFactory.resolve(configuration.namingStrategy(), OutputFileFormat.orc);
        SchemaConverter<TypeDescription> schemaConverter;
        if (dataCatalogHandler instanceof NoOpDataCatalogHandler) {
            schemaConverter = new CachedSchemaConverter<>(new OrcSchemaConverter());
        } else {
            schemaConverter = new DataCatalogAwareSchemaConverter<>(
                    new CachedSchemaConverter<>(new OrcSchemaConverter()),
                    dataCatalogHandler
            );
        }

        return new S3OrcEventSaver(
                configuration.threshold(),
                locationGenerator,
                configuration.fileIO(),
                configuration.codec().orElse(OrcCompressionCodec.NONE),
                schemaConverter
        );
    }
}
