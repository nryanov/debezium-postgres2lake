package io.debezium.postgres2lake.extensions.data.catalog.api.model;

import java.util.Optional;

public record TableField(String name, Optional<String> description, TableColumnType type) {
    public TableField(String name, String description, TableColumnType type) {
        this(name, Optional.ofNullable(description), type);
    }
}
