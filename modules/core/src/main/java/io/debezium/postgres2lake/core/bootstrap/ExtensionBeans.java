package io.debezium.postgres2lake.core.bootstrap;

import io.debezium.postgres2lake.core.config.DataCatalogConfiguration;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogHandler;
import io.debezium.postgres2lake.extensions.data.catalog.api.DataCatalogProviderSupport;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@ApplicationScoped
public class ExtensionBeans {
    @Inject
    DataCatalogConfiguration dataCatalogConfiguration;

    @Singleton
    @Produces
    DataCatalogHandler dataCatalogHandler() {
        var dataCatalogProviderSupport = new DataCatalogProviderSupport();
        var classLoader = Thread.currentThread().getContextClassLoader();
        var handler = dataCatalogProviderSupport.loadAndInitialize(dataCatalogConfiguration.name(), classLoader, dataCatalogConfiguration.properties());

        return handler;
    }
}
