package io.debezium.postgres2lake.infrastucture.s3;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.infrastucture.profile.OrcOutputFormatProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static java.util.concurrent.TimeUnit.SECONDS;

@QuarkusTest
@TestProfile(OrcOutputFormatProfile.class)
@QuarkusTestResource(PostgresMinioTestResource.class)
class S3OrcEventSaverTest {
    @Inject
    private EventSaver eventSaver;

    private static final long TEST_PK = 100_002L;

    @Test
    void debeziumWritesOrcReadableBySpark() {
        try (var conn = DriverManager.getConnection(
                PostgresMinioTestResource.getJdbcUrl(),
                "postgres",
                "postgres")) {
            IntegrationTestData.insertSampleRow(conn, TEST_PK);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        // force flush
        eventSaver.flush();

        await().atMost(120, SECONDS).pollInterval(1, SECONDS).until(() -> {
                    try {
                        return SparkIntegrationSupport.countFileRowsWithPk("orc", SparkIntegrationSupport.fileDatasetBasePath(), TEST_PK) >= 1L;
                    } catch (Exception e) {
                        return false;
                    }
                }
        );

        assertEquals(1L, SparkIntegrationSupport.countFileRowsWithPk("orc", SparkIntegrationSupport.fileDatasetBasePath(), TEST_PK));
    }
}
