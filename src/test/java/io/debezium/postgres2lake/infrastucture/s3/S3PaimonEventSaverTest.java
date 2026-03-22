package io.debezium.postgres2lake.infrastucture.s3;

import io.debezium.postgres2lake.infrastucture.profile.PaimonOutputFormatProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static java.util.concurrent.TimeUnit.SECONDS;

@QuarkusTest
@TestProfile(PaimonOutputFormatProfile.class)
@QuarkusTestResource(PostgresMinioTestResource.class)
class S3PaimonEventSaverTest {

    private static final long TEST_PK = 100_005L;

    @Test
    void debeziumWritesPaimonReadableBySpark() {
        try (var conn = DriverManager.getConnection(
                PostgresMinioTestResource.getJdbcUrl(),
                "postgres",
                "postgres")) {
            IntegrationTestData.insertSampleRow(conn, TEST_PK);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        await().atMost(120, SECONDS).pollInterval(1, SECONDS).until(() ->
                SparkIntegrationSupport.countPaimonRowsWithPk(TEST_PK) >= 1L
        );

        assertEquals(1L, SparkIntegrationSupport.countPaimonRowsWithPk(TEST_PK));
    }
}
