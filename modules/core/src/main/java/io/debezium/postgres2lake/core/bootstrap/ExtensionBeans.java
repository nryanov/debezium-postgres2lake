package io.debezium.postgres2lake.core.bootstrap;

import io.debezium.postgres2lake.core.config.CommitEventEmitterConfiguration;
import io.debezium.postgres2lake.core.config.DataCatalogConfiguration;
import io.debezium.postgres2lake.core.config.ReadinessMarkerEventEmitterConfiguration;
import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterHandler;
import io.debezium.postgres2lake.extensions.commit.event.emitter.api.CommitEventEmitterProvider;
import io.debezium.postgres2lake.extensions.commit.event.emitter.api.NoOpCommitEventEmitterHandler;
import io.debezium.postgres2lake.extensions.common.SpiProviderSupport;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogProvider;
import io.debezium.postgres2lake.extensions.data.catalog.api.NoOpDataCatalogHandler;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.NoOpReadinessMarkerEventEmitterHandler;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@ApplicationScoped
public class ExtensionBeans {
    @Inject
    DataCatalogConfiguration dataCatalogConfiguration;

    @Inject
    CommitEventEmitterConfiguration commitEventEmitterConfiguration;

    @Inject
    ReadinessMarkerEventEmitterConfiguration readinessMarkerEventEmitterConfiguration;

    @Singleton
    @Produces
    DataCatalogHandler dataCatalogHandler() {
        var dataCatalogProviderSupport = new SpiProviderSupport<DataCatalogHandler, DataCatalogProvider>();
        var classLoader = Thread.currentThread().getContextClassLoader();
        var handler = dataCatalogProviderSupport
                .loadAndInitialize(
                        dataCatalogConfiguration.name(),
                        classLoader,
                        dataCatalogConfiguration.properties(),
                        DataCatalogProvider.class,
                        NoOpDataCatalogHandler.INSTANCE
                );

        return handler;
    }

    @Singleton
    @Produces
    CommitEventEmitterHandler commitEventEmitterHandler() {
        var spiProvider = new SpiProviderSupport<CommitEventEmitterHandler, CommitEventEmitterProvider>();
        var classLoader = Thread.currentThread().getContextClassLoader();
        var handler = spiProvider
                .loadAndInitialize(
                        commitEventEmitterConfiguration.name(),
                        classLoader,
                        commitEventEmitterConfiguration.properties(),
                        CommitEventEmitterProvider.class,
                        NoOpCommitEventEmitterHandler.INSTANCE
                );

        return handler;
    }

    @Singleton
    @Produces
    ReadinessMarkerEventEmitterHandler readinessMarkerEventEmitterHandler() {
        var spiProvider = new SpiProviderSupport<ReadinessMarkerEventEmitterHandler, ReadinessMarkerEventEmitterProvider>();
        var classLoader = Thread.currentThread().getContextClassLoader();
        var handler = spiProvider
                .loadAndInitialize(
                        readinessMarkerEventEmitterConfiguration.name(),
                        classLoader,
                        readinessMarkerEventEmitterConfiguration.properties(),
                        ReadinessMarkerEventEmitterProvider.class,
                        NoOpReadinessMarkerEventEmitterHandler.INSTANCE
                );

        return handler;
    }
}
