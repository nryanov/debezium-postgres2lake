package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.domain.model.OutputFormat;
import io.debezium.postgres2lake.infrastructure.format.avro.AvroCompressionCodec;
import io.debezium.postgres2lake.infrastructure.s3.S3AvroEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@ApplicationScoped
public class AvroBeans {
    @Inject
    OutputConfiguration outputConfiguration;

    @Singleton
    @Produces
    EventSaver eventSaver() {
        if (outputConfiguration.format() != OutputFormat.AVRO) {
            throw new IllegalStateException(
                    "This application is built for output.format=AVRO, but configuration has: " + outputConfiguration.format());
        }
        var avro = outputConfiguration.avro()
                .orElseThrow(() -> new IllegalArgumentException("Empty avro format output configuration"));
        var locationGenerator = OutputLocationGeneratorFactory.resolve(avro.namingStrategy(), OutputFileFormat.avro);
        return new S3AvroEventSaver(
                outputConfiguration.threshold(),
                locationGenerator,
                avro.fileIO(),
                AvroCompressionCodec.fromConfig(avro.codec())
        );
    }
}
