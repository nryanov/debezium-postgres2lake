package io.debezium.postgres2lake.test.helper;

import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;

public class PostgresHelper {
    private final PostgreSQLContainer<?> container;

    private final String username;
    private final String database;

    public PostgresHelper(PostgreSQLContainer<?> container) {
        this.container = container;

        this.username = container.getUsername();
        this.database = container.getDatabaseName();
    }

    public void executeSql(String query) {
        try {
            container.execInContainer("psql", "-U", username, "-d", database, "-c", query);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public String jdbcUrl() {
        return container.getJdbcUrl();
    }
}
