package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.model.OutputFormat;
import io.debezium.postgres2lake.infrastructure.s3.S3PaimonEventSaver;
import io.debezium.postgres2lake.config.CommonConfiguration;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@ApplicationScoped
public class PaimonBeans {
    @Inject
    CommonConfiguration outputConfiguration;

    @Singleton
    @Produces
    EventSaver eventSaver() {
        if (outputConfiguration.format() != OutputFormat.PAIMON) {
            throw new IllegalStateException(
                    "This application is built for output.format=PAIMON, but configuration has: " + outputConfiguration.format());
        }
        var paimon = outputConfiguration.paimon()
                .orElseThrow(() -> new IllegalArgumentException("Empty paimon format output configuration"));
        return new S3PaimonEventSaver(
                outputConfiguration.threshold(),
                paimon
        );
    }
}
