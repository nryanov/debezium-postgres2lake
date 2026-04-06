package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.config.PaimonConfiguration;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.infrastructure.s3.S3PaimonEventSaver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@ApplicationScoped
public class PaimonBeans {
    @Inject
    PaimonConfiguration configuration;

    @Singleton
    @Produces
    public EventSaver eventSaver() {
        return new S3PaimonEventSaver(configuration);
    }
}
