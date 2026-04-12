package io.debezium.postgres2lake.extensions.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;

public final class SpiProviderSupport<H extends SpiHandler, P extends SpiProvider<H>> {

    public SpiProviderSupport() {
    }

    public H loadAndInitialize(
            Optional<String> providerClassName,
            ClassLoader classLoader,
            Map<String, String> properties,
            Class<P> clazz,
            H defaultInstance
    ) {
        var handler = loadHandler(providerClassName, classLoader, clazz, defaultInstance);
        handler.initialize(properties);

        return handler;
    }

    private H loadHandler(
            Optional<String> providerClassName,
            ClassLoader classLoader,
            Class<P> clazz,
            H defaultInstance
    ) {
        Objects.requireNonNull(providerClassName, "providerClassName");
        Objects.requireNonNull(classLoader, "classLoader");

        var providers = new ArrayList<P>();
        ServiceLoader.load(clazz, classLoader).forEach(providers::add);

        if (providers.isEmpty()) {
            if (providerClassName.isEmpty()) {
                return defaultInstance;
            } else {
                throw new IllegalStateException("DataCatalog provider class name was specified but provider wasn't found");
            }
        } else {
            if (providerClassName.isEmpty()) {
                // todo: add warning: additional providers was found
                return defaultInstance;
            }

            var desiredProviderName = providerClassName.get();

            return providers.stream()
                    .filter(p -> p.getClass().getName().equals(desiredProviderName))
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException(
                            "No DataCatalogProvider with class " + desiredProviderName + "; available: " + describe(providers)))
                    .create();
        }
    }

    private String describe(List<P> providers) {
        return String.join(", ", providers.stream().map(p -> p.getClass().getName()).toList());
    }
}
