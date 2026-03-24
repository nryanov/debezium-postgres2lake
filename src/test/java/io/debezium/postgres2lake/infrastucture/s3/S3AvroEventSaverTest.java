package io.debezium.postgres2lake.infrastucture.s3;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.infrastucture.profile.AvroOutputFormatProfile;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.test.MinioHelper;
import io.debezium.postgres2lake.test.MinioResource;
import io.debezium.postgres2lake.test.PostgresHelper;
import io.debezium.postgres2lake.test.PostgresQueries;
import io.debezium.postgres2lake.test.PostgresResource;
import io.debezium.postgres2lake.test.SparkHelper;
import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.debezium.postgres2lake.test.annotation.InjectPostgresHelper;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.awaitility.Awaitility.await;

@QuarkusTest
@TestProfile(AvroOutputFormatProfile.class)
@QuarkusTestResource(value = PostgresResource.class, initArgs = {
        @ResourceArg(name = PostgresResource.PREFIX_NAME_ARG, value = "default"),
        @ResourceArg(name = PostgresResource.PUBLICATION_NAME_ARG, value = "debezium_publication"),
        @ResourceArg(name = PostgresResource.SLOT_NAME_ARG, value = "debezium_slot")
})
@QuarkusTestResource(value = MinioResource.class, initArgs = {
        @ResourceArg(name = MinioResource.BUCKET_NAME_ARG, value = "warehouse"),
        @ResourceArg(name = MinioResource.FORMAT_TYPE_ARG, value = "avro")
})
public class S3AvroEventSaverTest {
    @Inject
    private EventSaver eventSaver;

    @InjectMinioHelper
    MinioHelper minioHelper;

    @InjectPostgresHelper
    PostgresHelper postgresHelper;

    private static final String BUCKET = "warehouse";
    private static final long TEST_PK = 100_001L;

    @BeforeEach
    public void cleanup() {
        minioHelper.clearBucket(BUCKET);
    }

    @Test
    void debeziumWritesAvroReadableBySpark() {
        var s3Path = "s3a://warehouse/default/public/data";

        postgresHelper.executeSql(PostgresQueries.createTableWithAllPrimitiveTypes("public.data"));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication("debezium_publication", "public.data"));
        postgresHelper.executeSql(PostgresQueries.insertIntoTableWithAllPrimitiveTypes("public.data", TEST_PK));

        var testEventSaver = (AbstractEventSaver<?>) eventSaver;
        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(1)).until(() -> testEventSaver.getCurrentRecords() > 0);
        // force flush
        eventSaver.flush();

        var sparkHelper = new SparkHelper(postgresHelper, minioHelper);
        sparkHelper.show("avro", s3Path);
    }
}
