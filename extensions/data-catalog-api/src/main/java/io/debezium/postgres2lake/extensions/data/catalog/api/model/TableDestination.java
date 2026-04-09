package io.debezium.postgres2lake.extensions.data.catalog.api.model;

import java.util.Objects;

/**
 * Logical identity of a table in the source or lake (database / schema / table names as strings).
 */
public record TableDestination(String database, String schema, String table) {

    public TableDestination {
        Objects.requireNonNull(database, "database");
        Objects.requireNonNull(schema, "schema");
        Objects.requireNonNull(table, "table");
        if (database.isBlank() || schema.isBlank() || table.isBlank()) {
            throw new IllegalArgumentException("database, schema and table must be non-blank");
        }
    }
}
