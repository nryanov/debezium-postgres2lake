package io.debezium.postgres2lake.test.container;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public final class PostgresTestContainer {

    private static final DockerImageName IMAGE = DockerImageName.parse("postgres:17");

    private static PostgreSQLContainer<?> sharedDebezium;

    private PostgresTestContainer() {
    }

    public static PostgreSQLContainer<?> newDedicatedForJdbcCatalog() {
        return new PostgreSQLContainer<>(IMAGE)
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres");
    }

    public static PostgreSQLContainer<?> newDebeziumContainer() {
        return new PostgreSQLContainer<>(IMAGE)
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres")
                .withCommand(
                        "postgres",
                        "-c", "wal_level=logical",
                        "-c", "max_wal_senders=5",
                        "-c", "max_replication_slots=5"
                );
    }

    public static synchronized PostgreSQLContainer<?> ensureSharedDebeziumStarted() {
        if (sharedDebezium == null) {
            sharedDebezium = newDebeziumContainer();
            sharedDebezium.start();
        }
        return sharedDebezium;
    }

    public static void stopSharedDebezium() {
        if (sharedDebezium != null) {
            sharedDebezium.stop();
            sharedDebezium = null;
        }
    }
}
