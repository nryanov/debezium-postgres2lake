package io.debezium.postgres2lake.extensions.common;

import java.util.Map;

public interface SpiHandler {
    void initialize(Map<String, String> properties);

    /**
     * Close opened resources if any
     */
    void close();
}
