package io.debezium.postgres2lake.avro.bootstrap;

import io.debezium.postgres2lake.avro.config.AvroConfiguration;
import io.debezium.postgres2lake.avro.infrastructure.appender.DefaultAvroEventAppenderFactory;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.NoOpDataCatalogHandler;
import io.debezium.postgres2lake.avro.infrastructure.AvroCompressionCodec;
import io.debezium.postgres2lake.avro.infrastructure.AvroSchemaConverter;
import io.debezium.postgres2lake.avro.infrastructure.S3AvroEventSaver;
import io.debezium.postgres2lake.core.bootstrap.OutputLocationGeneratorFactory;
import io.debezium.postgres2lake.core.infrastructure.schema.CachedSchemaConverter;
import io.debezium.postgres2lake.core.infrastructure.schema.DataCatalogAwareSchemaConverter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.avro.Schema;

@ApplicationScoped
public class AvroBeans {
    @Inject
    AvroConfiguration configuration;

    @Inject
    DataCatalogHandler dataCatalogHandler;

    @Singleton
    @Produces
    public EventSaver eventSaver() {
        var locationGenerator = OutputLocationGeneratorFactory.resolve(configuration.namingStrategy(), OutputFileFormat.avro);

        SchemaConverter<Schema> schemaConverter;
        if (dataCatalogHandler instanceof NoOpDataCatalogHandler) {
            schemaConverter = new CachedSchemaConverter<>(new AvroSchemaConverter());
        } else {
            schemaConverter = new DataCatalogAwareSchemaConverter<>(
                    new CachedSchemaConverter<>(new AvroSchemaConverter()),
                    dataCatalogHandler
            );
        }

        return new S3AvroEventSaver(
                configuration.threshold(),
                locationGenerator,
                configuration.fileIO(),
                configuration.codec().orElse(AvroCompressionCodec.NONE),
                schemaConverter,
                new DefaultAvroEventAppenderFactory()
        );
    }
}
