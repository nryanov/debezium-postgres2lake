package io.debezium.postgres2lake.extensions.data.catalog.api;

import java.util.Map;

public class DataCatalogPropertyReader {
    private DataCatalogPropertyReader() {
    }

    public static String required(Map<String, String> properties, String key) {
        var v = properties.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }
        return v.trim();
    }

    public static String optional(Map<String, String> properties, String key, String defaultValue) {
        var v = properties.get(key);
        if (v == null || v.isBlank()) {
            return defaultValue;
        }
        return v.trim();
    }
}
