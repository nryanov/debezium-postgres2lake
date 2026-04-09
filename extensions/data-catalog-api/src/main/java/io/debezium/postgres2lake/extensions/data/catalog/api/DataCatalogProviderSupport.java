package io.debezium.postgres2lake.extensions.data.catalog.api;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.ServiceLoader;
/**
 * Resolves a {@link DataCatalogHandler} from {@link ServiceLoader}-discovered {@link DataCatalogProvider}s,
 * optionally filtered by fully-qualified provider class name.
 */
public final class DataCatalogProviderSupport {

    private DataCatalogProviderSupport() {
    }

    /**
     * Loads a provider-selected handler and initializes it.
     */
    public DataCatalogHandler loadAndInitialize(Optional<String> providerClassName, ClassLoader classLoader, Map<String, String> properties) {
        var handler = loadHandler(providerClassName, classLoader);
        handler.initialize(Map.copyOf(properties));

        return handler;
    }

    /**
     * @param providerClassName fully qualified class name of the {@link DataCatalogProvider} implementation, or
     *                          empty to use the sole provider on the classpath
     * @param classLoader       class loader for discovery; often {@link Thread#getContextClassLoader()}
     */
    private DataCatalogHandler loadHandler(Optional<String> providerClassName, ClassLoader classLoader) {
        Objects.requireNonNull(providerClassName, "providerClassName");
        Objects.requireNonNull(classLoader, "classLoader");

        var providers = new ArrayList<DataCatalogProvider>();
        ServiceLoader.load(DataCatalogProvider.class, classLoader).forEach(providers::add);

        if (providers.isEmpty()) {
            if (providerClassName.isEmpty()) {
                return NoOpDataCatalogHandler.INSTANCE;
            } else {
                throw new IllegalStateException("DataCatalog provider class name was specified but provider wasn't found");
            }
        } else {
            if (providerClassName.isEmpty()) {
                // todo: add warning: additional providers was found
                return NoOpDataCatalogHandler.INSTANCE;
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

    private String describe(List<DataCatalogProvider> providers) {
        return String.join(", ", providers.stream().map(p -> p.getClass().getName()).toList());
    }
}
