package io.debezium.postgres2lake.infrastucture.s3;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.infrastucture.profile.IcebergOutputFormatProfile;
import io.debezium.postgres2lake.service.AbstractEventSaver;
import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.debezium.postgres2lake.test.annotation.InjectPostgresHelper;
import io.debezium.postgres2lake.test.helper.IcebergHelper;
import io.debezium.postgres2lake.test.helper.MinioHelper;
import io.debezium.postgres2lake.test.helper.PostgresHelper;
import io.debezium.postgres2lake.test.helper.PostgresQueries;
import io.debezium.postgres2lake.test.resource.MinioResource;
import io.debezium.postgres2lake.test.resource.PostgresResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.awaitility.Awaitility.await;

@QuarkusTest
@TestProfile(IcebergOutputFormatProfile.class)
@QuarkusTestResource(value = PostgresResource.class, initArgs = {
        @ResourceArg(name = PostgresResource.PREFIX_NAME_ARG, value = "default"),
        @ResourceArg(name = PostgresResource.PUBLICATION_NAME_ARG, value = "debezium_publication"),
        @ResourceArg(name = PostgresResource.SLOT_NAME_ARG, value = "debezium_slot"),
        @ResourceArg(name = PostgresResource.CATALOG_TYPE_ARG, value = "iceberg")
})
@QuarkusTestResource(value = MinioResource.class, initArgs = {
        @ResourceArg(name = MinioResource.BUCKET_NAME_ARG, value = "warehouse"),
        @ResourceArg(name = MinioResource.FORMAT_TYPE_ARG, value = "iceberg")
})
public class S3IcebergEventSaverTest {
    private final static Logger logger = Logger.getLogger(S3IcebergEventSaverTest.class);

    @Inject
    private EventSaver eventSaver;

    @InjectMinioHelper
    MinioHelper minioHelper;

    @InjectPostgresHelper
    PostgresHelper postgresHelper;

    private static final long TEST_PK = 100_004L;

    @Test
    void debeziumWritesIcebergReadableBySpark() {
        postgresHelper.executeSql(PostgresQueries.createTableWithAllPrimitiveTypes("public.data"));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication("debezium_publication", "public.data"));
        postgresHelper.executeSql(PostgresQueries.insertIntoTableWithAllPrimitiveTypes("public.data", TEST_PK));

        var testEventSaver = (AbstractEventSaver<?>) eventSaver;
        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(1)).until(() -> testEventSaver.getCurrentRecords() > 0);
        // force flush
        eventSaver.flush();

        var icebergHelper = new IcebergHelper("s3a://warehouse", postgresHelper, minioHelper);
        icebergHelper.readTable("development", "data");
    }
}
