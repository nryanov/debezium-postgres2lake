package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.domain.model.OutputFormat;
import io.debezium.postgres2lake.infrastructure.format.parquet.ParquetCompressionCodec;
import io.debezium.postgres2lake.infrastructure.s3.S3ParquetEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@ApplicationScoped
public class ParquetBeans {
    @Inject
    OutputConfiguration outputConfiguration;

    @Singleton
    @Produces
    EventSaver eventSaver() {
        if (outputConfiguration.format() != OutputFormat.PARQUET) {
            throw new IllegalStateException(
                    "This application is built for output.format=PARQUET, but configuration has: " + outputConfiguration.format());
        }
        var parquet = outputConfiguration.parquet()
                .orElseThrow(() -> new IllegalArgumentException("Empty parquet format output configuration"));
        var locationGenerator = OutputLocationGeneratorFactory.resolve(parquet.namingStrategy(), OutputFileFormat.parquet);
        return new S3ParquetEventSaver(
                outputConfiguration.threshold(),
                locationGenerator,
                parquet.fileIO(),
                ParquetCompressionCodec.fromConfig(parquet.codec())
        );
    }
}
