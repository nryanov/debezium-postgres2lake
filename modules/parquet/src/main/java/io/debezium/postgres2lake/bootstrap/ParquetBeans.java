package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.config.ParquetConfiguration;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.infrastructure.format.parquet.ParquetCompressionCodec;
import io.debezium.postgres2lake.infrastructure.s3.S3ParquetEventSaver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import static io.debezium.postgres2lake.domain.model.OutputFileFormat.parquet;

@ApplicationScoped
public class ParquetBeans {
    @Inject
    ParquetConfiguration configuration;

    @Singleton
    @Produces
    public EventSaver eventSaver() {
        var locationGenerator = OutputLocationGeneratorFactory.resolve(configuration.namingStrategy(), parquet);
        return new S3ParquetEventSaver(
                configuration.threshold(),
                locationGenerator,
                configuration.fileIO(),
                configuration.codec().orElse(ParquetCompressionCodec.NONE)
        );
    }
}
