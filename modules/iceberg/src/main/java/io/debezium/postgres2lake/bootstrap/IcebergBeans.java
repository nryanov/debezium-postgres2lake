package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.model.OutputFormat;
import io.debezium.postgres2lake.infrastructure.s3.S3IcebergEventSaver;
import io.debezium.postgres2lake.config.CommonConfiguration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@ApplicationScoped
public class IcebergBeans {
    @Inject
    CommonConfiguration outputConfiguration;

    @Singleton
    @Produces
    EventSaver eventSaver() {
        if (outputConfiguration.format() != OutputFormat.ICEBERG) {
            throw new IllegalStateException(
                    "This application is built for output.format=ICEBERG, but configuration has: " + outputConfiguration.format());
        }
        var iceberg = outputConfiguration.iceberg()
                .orElseThrow(() -> new IllegalArgumentException("Empty iceberg format output configuration"));
        return new S3IcebergEventSaver(
                outputConfiguration.threshold(),
                iceberg
        );
    }
}
