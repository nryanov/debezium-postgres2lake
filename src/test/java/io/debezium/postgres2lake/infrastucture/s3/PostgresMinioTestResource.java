package io.debezium.postgres2lake.infrastucture.s3;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Starts Postgres (logical replication) and MinIO once per JVM; exposes config for Quarkus tests.
 */
public class PostgresMinioTestResource implements QuarkusTestResourceLifecycleManager {

    private static final Object LOCK = new Object();
    private static PostgreSQLContainer<?> postgres;
    private static GenericContainer<?> minio;
    private static volatile String jdbcUrl;
    private static volatile String minioEndpoint;
    private static volatile boolean schemaAndBucketReady;

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            synchronized (LOCK) {
                if (minio != null) {
                    minio.stop();
                }
                if (postgres != null) {
                    postgres.stop();
                }
            }
        }));
    }

    public static String getJdbcUrl() {
        return jdbcUrl;
    }

    public static String getMinioEndpoint() {
        return minioEndpoint;
    }

    @Override
    public Map<String, String> start() {
        synchronized (LOCK) {
            if (postgres == null) {
                postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:17"))
                        .withDatabaseName("postgres")
                        .withUsername("postgres")
                        .withPassword("postgres")
                        .withCommand(
                                "postgres",
                                "-c", "wal_level=logical",
                                "-c", "max_wal_senders=5",
                                "-c", "max_replication_slots=5"
                        );
                postgres.start();
            }
            if (minio == null) {
                minio = new GenericContainer<>(DockerImageName.parse("minio/minio:RELEASE.2025-02-28T09-55-16Z"))
                        .withEnv("MINIO_ROOT_USER", "admin")
                        .withEnv("MINIO_ROOT_PASSWORD", "password")
                        .withCommand("server", "/data")
                        .withExposedPorts(9000);
                minio.start();
            }

            jdbcUrl = postgres.getJdbcUrl();
            minioEndpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);

            if (!schemaAndBucketReady) {
                createWarehouseBucketIfNeeded();
                applySchemaOnce();
                schemaAndBucketReady = true;
            }
        }

        return buildConfigOverrides();
    }

    private void createWarehouseBucketIfNeeded() {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(URI.create(minioEndpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("admin", "password")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build()) {
            try {
                s3.createBucket(CreateBucketRequest.builder().bucket("warehouse").build());
            } catch (software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException ignored) {
                // ok
            } catch (software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException ignored) {
                // ok
            }
        }
    }

    private void applySchemaOnce() {
        try (Connection conn = DriverManager.getConnection(jdbcUrl, postgres.getUsername(), postgres.getPassword());
             Statement st = conn.createStatement()) {
            var sql = readClasspathResource("/integration-test-schema.sql");
            for (var part : sql.split(";")) {
                var trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    st.execute(trimmed);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to apply integration test schema", e);
        }
    }

    private static String readClasspathResource(String path) {
        try (var in = PostgresMinioTestResource.class.getResourceAsStream(path)) {
            if (in == null) {
                throw new IllegalStateException("Missing resource: " + path);
            }
            return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
                    .lines()
                    .collect(Collectors.joining("\n"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, String> buildConfigOverrides() {
        var host = postgres.getHost();
        var port = String.valueOf(postgres.getMappedPort(5432));
        var jdbc = "jdbc:postgresql://" + host + ":" + port + "/postgres";
        var ep = minioEndpoint;

        var m = new HashMap<String, String>();
        m.put("debezium.engine.database.hostname", host);
        m.put("debezium.engine.database.port", port);

        m.put("output.avro.file-io.properties.fs.s3a.endpoint", ep);
        m.put("output.orc.file-io.properties.fs.s3a.endpoint", ep);
        m.put("output.parquet.file-io.properties.fs.s3a.endpoint", ep);

        m.put("output.iceberg.properties.uri", jdbc);
        m.put("output.iceberg.properties.s3.endpoint", ep);

        m.put("output.paimon.properties.jdbc-url", jdbc);
        m.put("output.paimon.file-io.properties.fs.s3a.endpoint", ep);

        return m;
    }

    @Override
    public void stop() {
        // Containers are shared across test classes and stopped on JVM shutdown.
    }
}
