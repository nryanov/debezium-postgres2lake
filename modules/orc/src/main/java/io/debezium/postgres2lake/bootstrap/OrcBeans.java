package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.domain.model.OutputFormat;
import io.debezium.postgres2lake.infrastructure.format.orc.OrcCompressionCodec;
import io.debezium.postgres2lake.infrastructure.s3.S3OrcEventSaver;
import io.debezium.postgres2lake.config.CommonConfiguration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@ApplicationScoped
public class OrcBeans {
    @Inject
    CommonConfiguration outputConfiguration;

    @Singleton
    @Produces
    EventSaver eventSaver() {
        if (outputConfiguration.format() != OutputFormat.ORC) {
            throw new IllegalStateException(
                    "This application is built for output.format=ORC, but configuration has: " + outputConfiguration.format());
        }
        var orc = outputConfiguration.orc()
                .orElseThrow(() -> new IllegalArgumentException("Empty orc format output configuration"));
        var locationGenerator = OutputLocationGeneratorFactory.resolve(orc.namingStrategy(), OutputFileFormat.orc);
        return new S3OrcEventSaver(
                outputConfiguration.threshold(),
                locationGenerator,
                orc.fileIO(),
                OrcCompressionCodec.fromConfig(orc.codec())
        );
    }
}
