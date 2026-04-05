package io.debezium.postgres2lake.domain.model;

public record EventDestination(String database, String schema, String table) {
}
