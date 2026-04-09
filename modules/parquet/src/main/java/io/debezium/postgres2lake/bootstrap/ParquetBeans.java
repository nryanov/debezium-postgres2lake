package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.config.ParquetConfiguration;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.NoOpDataCatalogHandler;
import io.debezium.postgres2lake.infrastructure.format.parquet.ParquetCompressionCodec;
import io.debezium.postgres2lake.infrastructure.format.parquet.ParquetSchemaConverter;
import io.debezium.postgres2lake.infrastructure.s3.S3ParquetEventSaver;
import io.debezium.postgres2lake.infrastructure.schema.CachedSchemaConverter;
import io.debezium.postgres2lake.infrastructure.schema.DataCatalogAwareSchemaConverter;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.avro.Schema;

import static io.debezium.postgres2lake.domain.model.OutputFileFormat.parquet;

@ApplicationScoped
public class ParquetBeans {
    @Inject
    ParquetConfiguration configuration;

    @Inject
    DataCatalogHandler dataCatalogHandler;

    @Singleton
    @Produces
    public EventSaver eventSaver() {
        var locationGenerator = OutputLocationGeneratorFactory.resolve(configuration.namingStrategy(), parquet);
        SchemaConverter<Schema> schemaConverter;
        if (dataCatalogHandler instanceof NoOpDataCatalogHandler) {
            schemaConverter = new CachedSchemaConverter<>(new ParquetSchemaConverter());
        } else {
            schemaConverter = new DataCatalogAwareSchemaConverter<>(
                    new CachedSchemaConverter<>(new ParquetSchemaConverter()),
                    dataCatalogHandler
            );
        }

        return new S3ParquetEventSaver(
                configuration.threshold(),
                locationGenerator,
                configuration.fileIO(),
                configuration.codec().orElse(ParquetCompressionCodec.NONE),
                schemaConverter
        );
    }
}
