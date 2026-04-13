package io.debezium.postgres2lake.parquet.bootstrap;

import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;
import io.debezium.postgres2lake.extensions.commit.event.emitter.api.NoOpCommitEventEmitterHandler;
import io.debezium.postgres2lake.parquet.config.ParquetConfiguration;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.NoOpDataCatalogHandler;
import io.debezium.postgres2lake.parquet.infrastructure.ParquetCompressionCodec;
import io.debezium.postgres2lake.parquet.infrastructure.ParquetSchemaConverter;
import io.debezium.postgres2lake.parquet.infrastructure.S3ParquetEventSaver;
import io.debezium.postgres2lake.core.bootstrap.OutputLocationGeneratorFactory;
import io.debezium.postgres2lake.core.infrastructure.schema.CachedSchemaConverter;
import io.debezium.postgres2lake.core.infrastructure.schema.DataCatalogAwareSchemaConverter;
import io.debezium.postgres2lake.parquet.infrastructure.appender.CommitEventEmitterAwareParquetEventAppenderFactory;
import io.debezium.postgres2lake.parquet.infrastructure.appender.DefaultParquetEventAppenderFactory;
import io.debezium.postgres2lake.parquet.infrastructure.appender.ParquetEventAppenderFactory;
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

    @Inject
    CommitEventEmitterHandler commitEventEmitterHandler;

    @Singleton
    @Produces
    public EventSaver eventSaver() {
        var locationGenerator = OutputLocationGeneratorFactory.resolve(configuration.namingStrategy(), parquet);

        var schemaConverter = resolveSchemaConverter();
        var appenderFactory = resolveAppenderFactory();

        return new S3ParquetEventSaver(
                configuration.threshold(),
                locationGenerator,
                configuration.fileIO(),
                configuration.codec().orElse(ParquetCompressionCodec.NONE),
                schemaConverter,
                appenderFactory
        );
    }

    private SchemaConverter<Schema> resolveSchemaConverter() {
        if (dataCatalogHandler instanceof NoOpDataCatalogHandler) {
            return new CachedSchemaConverter<>(new ParquetSchemaConverter());
        } else {
            return new DataCatalogAwareSchemaConverter<>(
                    new CachedSchemaConverter<>(new ParquetSchemaConverter()),
                    dataCatalogHandler
            );
        }
    }

    private ParquetEventAppenderFactory resolveAppenderFactory() {
        if (commitEventEmitterHandler instanceof NoOpCommitEventEmitterHandler) {
            return new DefaultParquetEventAppenderFactory();
        } else {
            return new CommitEventEmitterAwareParquetEventAppenderFactory(commitEventEmitterHandler);
        }
    }
}
