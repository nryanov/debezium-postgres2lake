package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.config.IcebergConfiguration;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.infrastructure.s3.S3IcebergEventSaver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@ApplicationScoped
public class IcebergBeans {
    @Inject
    IcebergConfiguration configuration;

    @Singleton
    @Produces
    public EventSaver eventSaver() {
        return new S3IcebergEventSaver(configuration);
    }
}
