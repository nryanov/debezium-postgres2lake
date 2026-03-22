package io.debezium.postgres2lake.infrastucture.s3;

import io.debezium.postgres2lake.infrastucture.profile.IcebergOutputFormatProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static java.util.concurrent.TimeUnit.SECONDS;

@QuarkusTest
@TestProfile(IcebergOutputFormatProfile.class)
@QuarkusTestResource(PostgresMinioTestResource.class)
class S3IcebergEventSaverTest {

    private static final long TEST_PK = 100_004L;

    @Test
    void debeziumWritesIcebergReadableBySpark() {
        try (var conn = DriverManager.getConnection(
                PostgresMinioTestResource.getJdbcUrl(),
                "postgres",
                "postgres")) {
            IntegrationTestData.insertSampleRow(conn, TEST_PK);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        await().atMost(120, SECONDS).pollInterval(1, SECONDS).until(() ->
                SparkIntegrationSupport.countIcebergRowsWithPk(TEST_PK) >= 1L
        );

        assertEquals(1L, SparkIntegrationSupport.countIcebergRowsWithPk(TEST_PK));
    }
}
